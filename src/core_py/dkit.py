"""Distributed primitive protocols and default helper implementation."""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from core_py import context as ctx_mod

DEFAULT_LOCK_TTL = 30.0
DEFAULT_LOCK_SPIN_INTERVAL = 0.5


class DKitError(Exception):
    pass


class InvalidOptionError(DKitError):
    pass


class LockNotAcquiredError(DKitError):
    pass


class AlreadyUnlockedError(DKitError):
    pass


class ElectionNotEnabledError(DKitError):
    pass


class BackendUnavailableError(DKitError):
    pass


class NoAvailableNumberError(DKitError):
    pass


ERR_INVALID_OPTION = InvalidOptionError("dkit: invalid option")
ERR_LOCK_NOT_ACQUIRED = LockNotAcquiredError("dkit: lock not acquired")
ERR_ALREADY_UNLOCKED = AlreadyUnlockedError("dkit: already unlocked")
ERR_ELECTION_NOT_ENABLED = ElectionNotEnabledError("dkit: election not enabled")
ERR_BACKEND_UNAVAILABLE = BackendUnavailableError("dkit: backend unavailable")
ERR_NO_AVAILABLE_NUMBER = NoAvailableNumberError("dkit: no available number")


@dataclass(slots=True)
class LockOption:
    ttl: float = DEFAULT_LOCK_TTL
    reentrant_identity: str = ""
    spin_interval: float = DEFAULT_LOCK_SPIN_INTERVAL
    max_wait_time: float = 0.0


LockOptionOp = Callable[[LockOption], None]


def new_lock_option(
    default_ttl: float = DEFAULT_LOCK_TTL, ops: list[LockOptionOp] | None = None
) -> LockOption:
    ttl = default_ttl if default_ttl > 0 else DEFAULT_LOCK_TTL
    opt = LockOption(ttl=ttl)
    for op in ops or []:
        op(opt)
    if opt.ttl <= 0:
        opt.ttl = ttl
    if opt.spin_interval <= 0:
        opt.spin_interval = DEFAULT_LOCK_SPIN_INTERVAL
    return opt


def lock_ttl(seconds: float) -> LockOptionOp:
    def op(option: LockOption) -> None:
        if seconds > 0:
            option.ttl = seconds

    return op


def lock_with_max_wait(seconds: float) -> LockOptionOp:
    def op(option: LockOption) -> None:
        if seconds > 0:
            option.max_wait_time = seconds

    return op


def lock_with_spin_interval(seconds: float) -> LockOptionOp:
    def op(option: LockOption) -> None:
        if seconds > 0:
            option.spin_interval = seconds

    return op


def reentrant(identity: str) -> LockOptionOp:
    def op(option: LockOption) -> None:
        option.reentrant_identity = identity

    return op


class DistributedMutex(Protocol):
    def lock(self, ctx: ctx_mod.Context | None = None, *ops: LockOptionOp) -> None: ...
    def try_lock(self, ctx: ctx_mod.Context | None = None, *ops: LockOptionOp) -> bool: ...
    def unlock(self, ctx: ctx_mod.Context | None = None) -> None: ...
    def exist_lock(self, ctx: ctx_mod.Context | None = None) -> bool: ...


class Atomic(Protocol):
    def get_unique_random_number(self, ctx: ctx_mod.Context | None, max_value: int) -> int: ...
    def new_mutex(self, key: str) -> DistributedMutex: ...
    def get_mutex_default_ttl(self) -> float: ...
    def close(self) -> None: ...


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
        try:
            action(ctx)
        finally:
            mutex.unlock(ctx)
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
        try:
            action(ctx)
        finally:
            mutex.unlock(ctx)

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


class InMemoryMutex:
    def __init__(
        self,
        locks: dict[str, tuple[str, float]],
        guard: threading.RLock,
        key: str,
        default_ttl: float,
    ) -> None:
        self._locks = locks
        self._guard = guard
        self._key = key
        self._default_ttl = default_ttl
        self._owner = f"{id(self)}-{random.random()}"

    def _expired(self, expires_at: float) -> bool:
        return expires_at <= time.time()

    def try_lock(self, ctx: ctx_mod.Context | None = None, *ops: LockOptionOp) -> bool:
        opt = new_lock_option(self._default_ttl, list(ops))
        with self._guard:
            existing = self._locks.get(self._key)
            if (
                existing
                and not self._expired(existing[1])
                and existing[0] != opt.reentrant_identity
            ):
                return False
            owner = opt.reentrant_identity or self._owner
            self._locks[self._key] = (owner, time.time() + opt.ttl)
            return True

    def lock(self, ctx: ctx_mod.Context | None = None, *ops: LockOptionOp) -> None:
        opt = new_lock_option(self._default_ttl, list(ops))
        start = time.time()
        while True:
            if self.try_lock(ctx, *ops):
                return
            if opt.max_wait_time > 0 and time.time() - start >= opt.max_wait_time:
                raise LockNotAcquiredError("dkit: lock not acquired")
            time.sleep(opt.spin_interval)

    def unlock(self, ctx: ctx_mod.Context | None = None) -> None:
        with self._guard:
            if self._key not in self._locks:
                raise AlreadyUnlockedError("dkit: already unlocked")
            del self._locks[self._key]

    def exist_lock(self, ctx: ctx_mod.Context | None = None) -> bool:
        with self._guard:
            existing = self._locks.get(self._key)
            if not existing:
                return False
            if self._expired(existing[1]):
                del self._locks[self._key]
                return False
            return True


class InMemoryAtomic:
    def __init__(self, default_ttl: float = DEFAULT_LOCK_TTL) -> None:
        self._allocated: set[int] = set()
        self._locks: dict[str, tuple[str, float]] = {}
        self._guard = threading.RLock()
        self._default_ttl = default_ttl

    def get_unique_random_number(self, ctx: ctx_mod.Context | None, max_value: int) -> int:
        if max_value <= 0:
            raise NoAvailableNumberError("dkit: no available number")
        with self._guard:
            if len(self._allocated) >= max_value:
                raise NoAvailableNumberError("dkit: no available number")
            while True:
                num = random.randrange(max_value)
                if num not in self._allocated:
                    self._allocated.add(num)
                    return num

    def new_mutex(self, key: str) -> InMemoryMutex:
        return InMemoryMutex(self._locks, self._guard, key, self._default_ttl)

    def get_mutex_default_ttl(self) -> float:
        return self._default_ttl

    def close(self) -> None:
        return None


# Go-style aliases.
DefaultLockTTL = DEFAULT_LOCK_TTL
DefaultLockSpinInterval = DEFAULT_LOCK_SPIN_INTERVAL
ErrInvalidOption = ERR_INVALID_OPTION
ErrLockNotAcquired = ERR_LOCK_NOT_ACQUIRED
ErrAlreadyUnlocked = ERR_ALREADY_UNLOCKED
ErrElectionNotEnabled = ERR_ELECTION_NOT_ENABLED
ErrBackendUnavailable = ERR_BACKEND_UNAVAILABLE
ErrNoAvailableNumber = ERR_NO_AVAILABLE_NUMBER
NewLockOption = new_lock_option
LockTTL = lock_ttl
LockWithMaxWait = lock_with_max_wait
LockWithSpinInterval = lock_with_spin_interval
Reentrant = reentrant
NewDefaultKit = new_default_kit
NewDefaultKitWithContext = new_default_kit_with_context
