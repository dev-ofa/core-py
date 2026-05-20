"""Redis-backed dkit primitives.

The module keeps the Redis dependency optional: pass a redis-py compatible
client object to `RedisAtomic`, or install `core-py[dkit-redis]`.
"""

from __future__ import annotations

import base64
import random
import threading
import time
import uuid
from typing import Any

from core_py import context as ctx_mod
from core_py.dkit.errors import (
    AlreadyUnlockedError,
    ElectionNotEnabledError,
    InvalidOptionError,
    LockNotAcquiredError,
    NoAvailableNumberError,
)
from core_py.dkit.options import ElectionOption, LockOptionOp, new_lock_option


class RedisMutex:
    def __init__(self, client: Any, key: str, default_ttl: float) -> None:
        self._client = client
        self._key = key
        self._default_ttl = default_ttl
        self._owner = uuid.uuid4().hex
        self._locked_owner = ""

    def try_lock(self, ctx: ctx_mod.Context | None = None, *ops: LockOptionOp) -> bool:
        opt = new_lock_option(self._default_ttl, list(ops))
        owner = opt.reentrant_identity or self._owner
        existing = self._client.get(self._key)
        if existing is not None and _to_text(existing) == owner:
            self._client.set(self._key, owner, ex=max(1, int(opt.ttl)))
            self._locked_owner = owner
            return True
        ok = self._client.set(self._key, owner, nx=True, ex=max(1, int(opt.ttl)))
        if bool(ok):
            self._locked_owner = owner
            return True
        return False

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
        owner = self._locked_owner or self._owner
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        end
        return 0
        """
        deleted = self._client.eval(script, 1, self._key, owner)
        if int(deleted or 0) == 0:
            raise AlreadyUnlockedError("dkit: already unlocked")
        self._locked_owner = ""

    def exist_lock(self, ctx: ctx_mod.Context | None = None) -> bool:
        return bool(self._client.exists(self._key))


class RedisAtomic:
    def __init__(
        self,
        client: Any,
        key_prefix: str = "dkit",
        default_ttl: float = 30.0,
        random_lease_seconds: int = 60,
    ) -> None:
        if client is None:
            raise InvalidOptionError("dkit: invalid option: redis client is nil")
        self._client = client
        self._key_prefix = key_prefix.strip(":") or "dkit"
        self._default_ttl = default_ttl
        self._random_lease_seconds = random_lease_seconds
        self._election: ElectionOption | None = None
        self._election_stop = threading.Event()
        self._election_thread: threading.Thread | None = None
        self._last_is_leader = False

    def get_unique_random_number(self, ctx: ctx_mod.Context | None, max_value: int) -> int:
        if max_value <= 0:
            raise NoAvailableNumberError("dkit: no available number")
        start = random.randrange(max_value)
        for offset in range(max_value):
            num = (start + offset) % max_value
            key = self._key("random", str(max_value), str(num))
            if self._client.set(key, "1", nx=True, ex=max(1, self._random_lease_seconds)):
                return num
        raise NoAvailableNumberError("dkit: no available number")

    def new_mutex(self, key: str) -> RedisMutex:
        return RedisMutex(self._client, self._key("mutex", key), self._default_ttl)

    def get_mutex_default_ttl(self) -> float:
        return self._default_ttl

    def enable_election(self, opt: ElectionOption | None = None) -> None:
        option = opt or ElectionOption()
        if not option.node_key:
            raise InvalidOptionError("dkit: invalid option: node key is empty")
        self._stop_election_loop()
        self._election = option
        self._last_is_leader = False
        self._tick_election()
        self._start_election_loop()

    def node_key(self) -> str:
        return self._require_election().node_key

    def is_leader(self) -> bool:
        option = self._require_election()
        return _to_text(self._client.get(self._leader_key())) == option.node_key

    def alive_nodes(self) -> list[str]:
        option = self._require_election()
        domain = self._key("election", option.isolation_key or "default", "heartbeat")
        pattern = f"{domain}:*"
        prefix = f"{domain}:"
        nodes = []
        for raw_key in self._client.scan_iter(match=pattern):
            key = _to_text(raw_key)
            if key.startswith(prefix):
                nodes.append(_unsafe_key(key[len(prefix) :]))
        if option.keep_heartbeat and option.node_key not in nodes:
            self._client.set(
                self._heartbeat_key(option.node_key),
                "1",
                ex=max(1, int(option.unhealthy_time)),
            )
            nodes.append(option.node_key)
        return sorted(nodes)

    def is_alive(self, node_key: str) -> bool:
        self._require_election()
        return bool(self._client.exists(self._heartbeat_key(node_key)))

    def close(self) -> None:
        self._stop_election_loop()
        close = getattr(self._client, "close", None)
        if callable(close):
            close()

    def _start_election_loop(self) -> None:
        option = self._require_election()
        interval = _election_interval(option)
        self._election_stop.clear()
        self._election_thread = threading.Thread(
            target=self._run_election_loop,
            args=(interval,),
            name=f"dkit-redis-election-{option.node_key}",
            daemon=True,
        )
        self._election_thread.start()

    def _stop_election_loop(self) -> None:
        self._election_stop.set()
        if self._election_thread is not None and self._election_thread.is_alive():
            self._election_thread.join(timeout=1.0)
        self._election_thread = None
        self._election_stop.clear()

    def _run_election_loop(self, interval: float) -> None:
        while not self._election_stop.wait(interval):
            try:
                self._tick_election()
            except Exception:
                continue

    def _tick_election(self) -> None:
        option = self._require_election()
        lease = max(1, int(option.unhealthy_time))
        if option.keep_heartbeat:
            self._client.set(self._heartbeat_key(option.node_key), "1", ex=lease)

        leader_key = self._leader_key()
        leader = _to_text(self._client.get(leader_key))
        if leader == option.node_key:
            self._client.set(leader_key, option.node_key, ex=lease)
        elif not leader and (option.can_elect is None or option.can_elect()):
            self._client.set(leader_key, option.node_key, nx=True, ex=lease)

        self._notify_leader_change()

    def _require_election(self) -> ElectionOption:
        if self._election is None:
            raise ElectionNotEnabledError("dkit: election not enabled")
        return self._election

    def _key(self, *parts: str) -> str:
        return ":".join([self._key_prefix, *[_safe_key(part) for part in parts]])

    def _leader_key(self) -> str:
        option = self._require_election()
        return self._key("election", option.isolation_key or "default", "leader")

    def _heartbeat_key(self, node_key: str) -> str:
        option = self._require_election()
        return self._key("election", option.isolation_key or "default", "heartbeat", node_key)

    def _notify_leader_change(self) -> None:
        option = self._require_election()
        leader = _to_text(self._client.get(self._leader_key()))
        is_leader = leader == option.node_key
        if option.on_leader_changed is None or is_leader == self._last_is_leader:
            self._last_is_leader = is_leader
            return
        from core_py.dkit.options import LeaderChangedEvent

        self._last_is_leader = is_leader
        option.on_leader_changed(
            LeaderChangedEvent(
                node_key=option.node_key,
                is_leader=is_leader,
                leader_key=leader,
            )
        )


def new_redis_atomic(
    client: Any,
    key_prefix: str = "dkit",
    default_ttl: float = 30.0,
) -> RedisAtomic:
    return RedisAtomic(client=client, key_prefix=key_prefix, default_ttl=default_ttl)


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _safe_key(value: str) -> str:
    if not value:
        return "default"
    raw = value.encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unsafe_key(value: str) -> str:
    if value == "default":
        return ""
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii")).decode("utf-8")


def _election_interval(option: ElectionOption) -> float:
    candidates = [option.timeout, option.unhealthy_time / 3]
    positives = [value for value in candidates if value > 0]
    return max(0.05, min(positives or [1.0]))
