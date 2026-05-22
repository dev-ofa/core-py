"""Redis-backed dkit primitives."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import math
import random
import time
import uuid
from typing import Any

from core_py._async import maybe_await
from core_py.dkit.errors import (
    AlreadyUnlockedError,
    ElectionNotEnabledError,
    InvalidOptionError,
    LockNotAcquiredError,
    NoAvailableNumberError,
)
from core_py.dkit.options import ElectionOption, LockOptionOp, new_lock_option

_DELETE_IF_VALUE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

_PEXPIRE_IF_VALUE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
"""


class RedisMutex:
    def __init__(self, client: Any, key: str, default_ttl: float) -> None:
        self._client = client
        self._key = key
        self._default_ttl = default_ttl
        self._owner = uuid.uuid4().hex
        self._locked_owner = ""

    async def try_lock(self, *ops: LockOptionOp) -> bool:
        opt = new_lock_option(self._default_ttl, list(ops))
        owner = opt.reentrant_identity or self._owner
        existing = await maybe_await(self._client.get(self._key))
        if existing is not None and _to_text(existing) == owner:
            await maybe_await(self._client.set(self._key, owner, px=_ttl_ms(opt.ttl)))
            self._locked_owner = owner
            return True
        ok = await maybe_await(self._client.set(self._key, owner, nx=True, px=_ttl_ms(opt.ttl)))
        if bool(ok):
            self._locked_owner = owner
            return True
        return False

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
        owner = self._locked_owner or self._owner
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        end
        return 0
        """
        deleted = await maybe_await(self._client.eval(script, 1, self._key, owner))
        if int(deleted or 0) == 0:
            raise AlreadyUnlockedError("dkit: already unlocked")
        self._locked_owner = ""

    async def exist_lock(self) -> bool:
        return bool(await maybe_await(self._client.exists(self._key)))


class RedisAtomic:
    def __init__(
        self,
        client: Any,
        key_prefix: str = "dkit",
        default_ttl: float = 30.0,
        random_lease_seconds: float = 60,
    ) -> None:
        if client is None:
            raise InvalidOptionError("dkit: invalid option: redis client is nil")
        self._client = client
        self._key_prefix = key_prefix.strip(":") or "dkit"
        self._default_ttl = default_ttl
        self._random_lease_seconds = random_lease_seconds
        self._election: ElectionOption | None = None
        self._election_task: asyncio.Task[None] | None = None
        self._last_is_leader = False

    async def get_unique_random_number(self, max_value: int) -> int:
        if max_value <= 0:
            raise NoAvailableNumberError("dkit: no available number")
        start = random.randrange(max_value)
        for offset in range(max_value):
            num = (start + offset) % max_value
            key = self._random_key(num)
            if await maybe_await(
                self._client.set(key, "1", nx=True, px=_ttl_ms(self._random_lease_seconds))
            ):
                return num
        raise NoAvailableNumberError("dkit: no available number")

    def new_mutex(self, key: str) -> RedisMutex:
        return RedisMutex(self._client, self._mutex_key(key), self._default_ttl)

    def get_mutex_default_ttl(self) -> float:
        return self._default_ttl

    async def enable_election(self, opt: ElectionOption | None = None) -> None:
        option = opt or ElectionOption()
        if not option.node_key:
            raise InvalidOptionError("dkit: invalid option: node key is empty")
        if self._election is not None:
            raise InvalidOptionError("dkit: invalid option: election already enabled")
        self._election = option
        self._last_is_leader = False
        await self._tick_election()
        self._start_election_loop()

    def node_key(self) -> str:
        if self._election is None:
            return ""
        return self._election.node_key

    async def is_leader(self) -> bool:
        option = self._election
        if option is None:
            return False
        return _to_text(await maybe_await(self._client.get(self._leader_key()))) == option.node_key

    async def alive_nodes(self) -> list[str]:
        option = self._require_election()
        if not option.keep_heartbeat:
            raise InvalidOptionError("dkit: invalid option: heartbeat is disabled")
        prefix = self._heartbeat_prefix(option.isolation_key)
        pattern = f"{prefix}:*"
        prefix = f"{prefix}:"
        nodes = []
        for raw_key in await maybe_await(self._client.scan_iter(match=pattern)):
            key = _to_text(raw_key)
            if key.startswith(prefix):
                nodes.append(_decode_component(key[len(prefix) :]))
        return sorted(nodes)

    async def is_alive(self, node_key: str) -> bool:
        option = self._require_election()
        if not option.keep_heartbeat:
            raise InvalidOptionError("dkit: invalid option: heartbeat is disabled")
        return bool(await maybe_await(self._client.exists(self._heartbeat_key(node_key))))

    async def close(self) -> None:
        option = self._election
        await self._stop_election_loop()
        if option is not None:
            await self._cleanup_election(option)
        self._election = None
        self._last_is_leader = False
        close = getattr(self._client, "close", None)
        if callable(close):
            await maybe_await(close())

    def _start_election_loop(self) -> None:
        option = self._require_election()
        interval = _election_interval(option)
        self._election_task = asyncio.create_task(
            self._run_election_loop(interval), name=f"dkit-redis-election-{option.node_key}"
        )

    async def _stop_election_loop(self) -> None:
        if self._election_task is not None:
            self._election_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._election_task
        self._election_task = None

    async def _run_election_loop(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await self._tick_election()
            except Exception:
                continue

    async def _tick_election(self) -> None:
        option = self._require_election()
        lease_ms = _ttl_ms(option.unhealthy_time)
        if option.keep_heartbeat:
            await maybe_await(
                self._client.set(self._heartbeat_key(option.node_key), option.node_key, px=lease_ms)
            )

        leader_key = self._leader_key()
        leader = _to_text(await maybe_await(self._client.get(leader_key)))
        if leader == option.node_key:
            await maybe_await(
                self._client.eval(
                    _PEXPIRE_IF_VALUE_SCRIPT,
                    1,
                    leader_key,
                    option.node_key,
                    str(lease_ms),
                )
            )
        elif not leader and (option.can_elect is None or option.can_elect()):
            await maybe_await(self._client.set(leader_key, option.node_key, nx=True, px=lease_ms))

        await self._notify_leader_change()

    async def _cleanup_election(self, option: ElectionOption) -> None:
        leader_key = self._leader_key_for(option.isolation_key)
        await maybe_await(self._client.eval(_DELETE_IF_VALUE_SCRIPT, 1, leader_key, option.node_key))
        if option.keep_heartbeat:
            await self._delete_key(self._heartbeat_key_for(option.isolation_key, option.node_key))

    async def _delete_key(self, key: str) -> None:
        delete = getattr(self._client, "delete", None)
        if callable(delete):
            await maybe_await(delete(key))
            return
        await maybe_await(self._client.eval(_DELETE_IF_VALUE_SCRIPT, 1, key, "1"))

    def _require_election(self) -> ElectionOption:
        if self._election is None:
            raise ElectionNotEnabledError("dkit: election not enabled")
        return self._election

    def _mutex_key(self, key: str) -> str:
        return f"{self._key_prefix}:mutex:{key}"

    def _random_key(self, num: int) -> str:
        return f"{self._key_prefix}:random:{num}"

    def _leader_key(self) -> str:
        option = self._require_election()
        return self._leader_key_for(option.isolation_key)

    def _leader_key_for(self, isolation_key: str) -> str:
        return f"{self._key_prefix}:leader:{_encode_component(isolation_key)}"

    def _heartbeat_prefix(self, isolation_key: str) -> str:
        return f"{self._key_prefix}:heartbeat:{_encode_component(isolation_key)}"

    def _heartbeat_key(self, node_key: str) -> str:
        option = self._require_election()
        return self._heartbeat_key_for(option.isolation_key, node_key)

    def _heartbeat_key_for(self, isolation_key: str, node_key: str) -> str:
        return f"{self._heartbeat_prefix(isolation_key)}:{_encode_component(node_key)}"

    async def _notify_leader_change(self) -> None:
        option = self._require_election()
        leader = _to_text(await maybe_await(self._client.get(self._leader_key())))
        is_leader = leader == option.node_key
        if option.on_leader_changed is None or is_leader == self._last_is_leader:
            self._last_is_leader = is_leader
            return
        from core_py.dkit.options import LeaderChangedEvent

        self._last_is_leader = is_leader
        await maybe_await(
            option.on_leader_changed(
                LeaderChangedEvent(
                    node_key=option.node_key,
                    is_leader=is_leader,
                    isolation_key=option.isolation_key,
                    leader_key=leader,
                )
            )
        )


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _encode_component(value: str) -> str:
    if not value:
        return ""
    raw = value.encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_component(value: str) -> str:
    if not value:
        return ""
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii")).decode("utf-8")


def _election_interval(option: ElectionOption) -> float:
    candidates = [option.timeout, option.unhealthy_time / 3]
    positives = [value for value in candidates if value > 0]
    return max(0.05, min(positives or [1.0]))


def _ttl_ms(value: float) -> int:
    return max(1, int(math.ceil(value * 1000)))
