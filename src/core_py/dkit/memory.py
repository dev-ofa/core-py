"""In-memory dkit backend for local tests and single-process use."""

from __future__ import annotations

import asyncio
import random
import threading
import time

from core_py.dkit.errors import (
    AlreadyUnlockedError,
    ElectionNotEnabledError,
    LockNotAcquiredError,
    NoAvailableNumberError,
)
from core_py.dkit.options import LockOptionOp, new_lock_option


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
        self._locked_owner = ""

    def _expired(self, expires_at: float) -> bool:
        return expires_at <= time.time()

    async def try_lock(self, *ops: LockOptionOp) -> bool:
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
            self._locked_owner = owner
            return True

    async def lock(self, *ops: LockOptionOp) -> None:
        opt = new_lock_option(self._default_ttl, list(ops))
        start = time.time()
        while True:
            if await self.try_lock(*ops):
                return
            if opt.max_wait_time > 0 and time.time() - start >= opt.max_wait_time:
                raise LockNotAcquiredError("dkit: lock not acquired")
            sleep_for = opt.spin_interval
            if opt.max_wait_time > 0:
                sleep_for = min(sleep_for, max(0.0, opt.max_wait_time - (time.time() - start)))
            await asyncio.sleep(sleep_for)

    async def unlock(self) -> None:
        with self._guard:
            existing = self._locks.get(self._key)
            owner = self._locked_owner or self._owner
            if not existing:
                raise AlreadyUnlockedError("dkit: already unlocked")
            if self._expired(existing[1]):
                del self._locks[self._key]
                raise AlreadyUnlockedError("dkit: already unlocked")
            if existing[0] != owner:
                raise AlreadyUnlockedError("dkit: already unlocked")
            del self._locks[self._key]
            self._locked_owner = ""

    async def exist_lock(self) -> bool:
        with self._guard:
            existing = self._locks.get(self._key)
            if not existing:
                return False
            if self._expired(existing[1]):
                del self._locks[self._key]
                return False
            return True


class InMemoryAtomic:
    def __init__(self, default_ttl: float = 30.0) -> None:
        self._allocated: set[int] = set()
        self._locks: dict[str, tuple[str, float]] = {}
        self._guard = threading.RLock()
        self._default_ttl = default_ttl

    async def get_unique_random_number(self, max_value: int) -> int:
        if max_value <= 0:
            raise NoAvailableNumberError("dkit: no available number")
        with self._guard:
            if len(self._allocated) >= max_value:
                raise NoAvailableNumberError("dkit: no available number")
            start = random.randrange(max_value)
            for offset in range(max_value):
                num = (start + offset) % max_value
                if num not in self._allocated:
                    self._allocated.add(num)
                    return num
            raise NoAvailableNumberError("dkit: no available number")

    def new_mutex(self, key: str) -> InMemoryMutex:
        return InMemoryMutex(self._locks, self._guard, key, self._default_ttl)

    def get_mutex_default_ttl(self) -> float:
        return self._default_ttl

    async def enable_election(self, opt: object | None = None) -> None:
        raise ElectionNotEnabledError("dkit: election not enabled")

    def node_key(self) -> str:
        raise ElectionNotEnabledError("dkit: election not enabled")

    async def is_leader(self) -> bool:
        raise ElectionNotEnabledError("dkit: election not enabled")

    async def alive_nodes(self) -> list[str]:
        raise ElectionNotEnabledError("dkit: election not enabled")

    async def is_alive(self, node_key: str) -> bool:
        raise ElectionNotEnabledError("dkit: election not enabled")

    async def close(self) -> None:
        return None
