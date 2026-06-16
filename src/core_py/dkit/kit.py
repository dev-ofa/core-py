"""Default dkit helper built on an Atomic backend."""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Protocol

from sonyflake import Sonyflake

from core_py._async import maybe_await
from core_py.dkit.errors import DefaultKitNotConfiguredError, InvalidOptionError
from core_py.dkit.options import lock_ttl, lock_with_max_wait
from core_py.dkit.protocols import Atomic, DistributedMutex
from core_py.model import SnowflakeID

Action = Callable[[], None | Awaitable[None]]

SNOWFLAKE_START_TIME = datetime(2007, 7, 1, tzinfo=UTC)
"""Fixed epoch shared by dev-ofa Sonyflake generators.

The date is intentionally earlier than Sonyflake's original 2014-09-01 epoch so
newly generated decimal IDs are already 19 digits in 2026-era systems. This
avoids a future 18-to-19-digit boundary where plain decimal string ordering
would temporarily diverge from numeric ordering. Treat this value as a
cross-language compatibility constant; changing it after launch can break ID
ordering assumptions and may risk collisions with generators using another epoch.
"""


class SnowflakeIDGenerator(Protocol):
    def get_id(self) -> int: ...
    def get_snowflake_id(self) -> SnowflakeID: ...
    def get_id_string(self) -> str: ...


class Kit(SnowflakeIDGenerator, Protocol):
    def new_mutex(self, key: str) -> DistributedMutex: ...
    async def mutex_try_do(self, mutex_key: str, action: Action) -> tuple[bool, None]: ...
    async def mutex_do(self, mutex_key: str, action: Action) -> None: ...


_default_kit: Kit | None = None
_default_kit_lock = threading.RLock()


class DefaultKit:
    def __init__(self, atomic: Atomic, machine_id: int) -> None:
        self.atomic = atomic
        self._machine_id = machine_id & 0xFFFF
        self._id_generator = Sonyflake(
            start_time=SNOWFLAKE_START_TIME,
            machine_id=self._machine_id,
        )
        self._lock = threading.Lock()

    def get_id(self) -> int:
        with self._lock:
            return self._id_generator.next_id()

    def get_snowflake_id(self) -> SnowflakeID:
        return SnowflakeID(self.get_id())

    def get_id_string(self) -> str:
        return str(self.get_id())

    def new_mutex(self, key: str) -> DistributedMutex:
        return self.atomic.new_mutex(key)

    async def mutex_try_do(self, mutex_key: str, action: Action) -> tuple[bool, None]:
        if action is None:
            raise InvalidOptionError("dkit: invalid option: action is nil")
        mutex = self.new_mutex(mutex_key)
        if mutex is None:
            raise InvalidOptionError("dkit: invalid option: mutex is nil")
        if not await mutex.try_lock():
            return False, None
        action_err: BaseException | None = None
        try:
            await maybe_await(action())
        except BaseException as exc:
            action_err = exc
            raise
        finally:
            try:
                await mutex.unlock()
            except Exception as unlock_err:
                if action_err is not None:
                    raise RuntimeError(
                        f"unlock failed: {unlock_err}, action error: {action_err}"
                    ) from action_err
                raise RuntimeError("unlock failed") from unlock_err
        return True, None

    async def mutex_do(self, mutex_key: str, action: Action) -> None:
        if action is None:
            raise InvalidOptionError("dkit: invalid option: action is nil")
        mutex = self.new_mutex(mutex_key)
        if mutex is None:
            raise InvalidOptionError("dkit: invalid option: mutex is nil")
        default_ttl = self.atomic.get_mutex_default_ttl()
        await mutex.lock(lock_ttl(default_ttl), lock_with_max_wait(default_ttl))
        await self._run_locked_action(mutex, action)

    async def _run_locked_action(self, mutex: object, action: Action) -> None:
        action_err: BaseException | None = None
        try:
            await maybe_await(action())
        except BaseException as exc:
            action_err = exc
            raise
        finally:
            try:
                await mutex.unlock()  # type: ignore[attr-defined]
            except Exception as unlock_err:
                if action_err is not None:
                    raise RuntimeError(
                        f"unlock failed: {unlock_err}, action error: {action_err}"
                    ) from action_err
                raise RuntimeError("unlock failed") from unlock_err


def set_default_kit(kit: Kit) -> None:
    global _default_kit
    if kit is None:
        raise InvalidOptionError("dkit: invalid option: kit is nil")
    with _default_kit_lock:
        _default_kit = kit


def default_kit() -> Kit:
    with _default_kit_lock:
        kit = _default_kit
    if kit is None:
        raise DefaultKitNotConfiguredError("dkit: default kit not configured")
    return kit


def reset_default_kit() -> None:
    global _default_kit
    with _default_kit_lock:
        _default_kit = None


@contextmanager
def use_default_kit(kit: Kit) -> Iterator[None]:
    global _default_kit
    with _default_kit_lock:
        previous = _default_kit
        set_default_kit(kit)
    try:
        yield
    finally:
        with _default_kit_lock:
            _default_kit = previous


async def new_default_kit(atomic: Atomic) -> DefaultKit:
    if atomic is None:
        raise InvalidOptionError("dkit: invalid option: atomic is nil")
    num = await atomic.get_unique_random_number(65535)
    if num < 0 or num > 65535:
        raise InvalidOptionError(f"dkit: invalid option: machine id {num} out of uint16 range")
    return DefaultKit(atomic, num)
