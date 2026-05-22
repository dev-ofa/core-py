"""Default dkit helper built on an Atomic backend."""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from sonyflake import Sonyflake

from core_py._async import maybe_await
from core_py.dkit.errors import InvalidOptionError
from core_py.dkit.options import lock_ttl, lock_with_max_wait
from core_py.dkit.protocols import Atomic

Action = Callable[[], None | Awaitable[None]]


class DefaultKit:
    def __init__(self, atomic: Atomic, machine_id: int) -> None:
        self.atomic = atomic
        self._machine_id = machine_id & 0xFFFF
        self._id_generator = Sonyflake(
            start_time=datetime(2014, 9, 1, tzinfo=UTC),
            machine_id=self._machine_id,
        )
        self._lock = threading.Lock()

    def get_id(self) -> int:
        with self._lock:
            return self._id_generator.next_id()

    def get_id_string(self) -> str:
        return str(self.get_id())

    async def mutex_try_do(self, mutex_key: str, action: Action) -> tuple[bool, None]:
        if action is None:
            raise InvalidOptionError("dkit: invalid option: action is nil")
        mutex = self.atomic.new_mutex(mutex_key)
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
        mutex = self.atomic.new_mutex(mutex_key)
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

async def new_default_kit(atomic: Atomic) -> DefaultKit:
    if atomic is None:
        raise InvalidOptionError("dkit: invalid option: atomic is nil")
    num = await atomic.get_unique_random_number(65535)
    if num < 0 or num > 65535:
        raise InvalidOptionError(f"dkit: invalid option: machine id {num} out of uint16 range")
    return DefaultKit(atomic, num)
