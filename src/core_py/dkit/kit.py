"""Default dkit helper built on an Atomic backend."""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable

from core_py import context as ctx_mod
from core_py.dkit.errors import InvalidOptionError
from core_py.dkit.protocols import Atomic

Action = Callable[[ctx_mod.Context | None], None]


class DefaultKit:
    def __init__(self, atomic: Atomic, machine_id: int) -> None:
        self.atomic = atomic
        self._machine_id = machine_id & 0xFFFF
        self._seq = random.randint(0, 0xFFF)
        self._lock = threading.Lock()

    def next_id(self, ctx: ctx_mod.Context | None = None) -> int:
        with self._lock:
            self._seq = (self._seq + 1) & 0xFFF
            now_ms = int(time.time() * 1000) & ((1 << 41) - 1)
            return (now_ms << 22) | (self._machine_id << 12) | self._seq

    def next_id_string(self, ctx: ctx_mod.Context | None = None) -> str:
        return str(self.next_id(ctx))

    def get_id(self) -> int:
        return self.next_id(None)

    def get_id_string(self) -> str:
        return self.next_id_string(None)

    def mutex_ctx_try_do(
        self, ctx: ctx_mod.Context | None, mutex_key: str, action: Action
    ) -> tuple[bool, None]:
        if action is None:
            raise InvalidOptionError("dkit: invalid option: action is nil")
        mutex = self.atomic.new_mutex(mutex_key)
        if mutex is None:
            raise InvalidOptionError("dkit: invalid option: mutex is nil")
        if not mutex.try_lock(ctx):
            return False, None
        action_err: BaseException | None = None
        try:
            action(ctx)
        except BaseException as exc:
            action_err = exc
            raise
        finally:
            try:
                mutex.unlock(ctx)
            except Exception as unlock_err:
                if action_err is not None:
                    raise RuntimeError(
                        f"unlock failed: {unlock_err}, action error: {action_err}"
                    ) from action_err
                raise RuntimeError("unlock failed") from unlock_err
        return True, None

    def mutex_try_do(self, mutex_key: str, action: Action) -> tuple[bool, None]:
        return self.mutex_ctx_try_do(None, mutex_key, action)

    def mutex_ctx_do(self, ctx: ctx_mod.Context | None, mutex_key: str, action: Action) -> None:
        if action is None:
            raise InvalidOptionError("dkit: invalid option: action is nil")
        mutex = self.atomic.new_mutex(mutex_key)
        if mutex is None:
            raise InvalidOptionError("dkit: invalid option: mutex is nil")
        mutex.lock(ctx)
        action_err: BaseException | None = None
        try:
            action(ctx)
        except BaseException as exc:
            action_err = exc
            raise
        finally:
            try:
                mutex.unlock(ctx)
            except Exception as unlock_err:
                if action_err is not None:
                    raise RuntimeError(
                        f"unlock failed: {unlock_err}, action error: {action_err}"
                    ) from action_err
                raise RuntimeError("unlock failed") from unlock_err

    def mutex_do(self, mutex_key: str, action: Action) -> None:
        self.mutex_ctx_do(None, mutex_key, action)


def new_default_kit_with_context(ctx: ctx_mod.Context | None, atomic: Atomic) -> DefaultKit:
    if atomic is None:
        raise InvalidOptionError("dkit: invalid option: atomic is nil")
    num = atomic.get_unique_random_number(ctx, 65535)
    if num < 0 or num > 65535:
        raise InvalidOptionError(f"dkit: invalid option: machine id {num} out of uint16 range")
    return DefaultKit(atomic, num)


def new_default_kit(atomic: Atomic) -> DefaultKit:
    return new_default_kit_with_context(None, atomic)
