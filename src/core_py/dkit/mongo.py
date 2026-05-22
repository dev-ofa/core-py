"""MongoDB-backed dkit primitives.

Pass a PyMongo database object to `MongoAtomic`. The module uses duck typing so
the base package does not need PyMongo at import time.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from core_py._async import collect_async_iterable, maybe_await
from core_py.dkit.errors import (
    AlreadyUnlockedError,
    ElectionNotEnabledError,
    InvalidOptionError,
    LockNotAcquiredError,
    NoAvailableNumberError,
)
from core_py.dkit.options import (
    ElectionOption,
    LeaderChangedEvent,
    LockOptionOp,
    new_lock_option,
)


class MongoMutex:
    def __init__(self, collection: Any, key: str, default_ttl: float) -> None:
        self._collection = collection
        self._key = key
        self._default_ttl = default_ttl
        self._lock_detail: dict[str, Any] | None = None

    async def try_lock(self, *ops: LockOptionOp) -> bool:
        opt = new_lock_option(self._default_ttl, list(ops))
        identity = opt.reentrant_identity
        now = _now()
        expires = now + timedelta(seconds=opt.ttl)
        if self._lock_detail is not None:
            held = await maybe_await(self._collection.find_one({"_id": self._key}))
            if (
                held
                and held.get("expires", now) > now
                and held.get("identity") == self._lock_detail.get("identity")
                and held.get("expires") == self._lock_detail.get("expires")
            ):
                return True
            self._lock_detail = None
        doc = await maybe_await(self._collection.find_one({"_id": self._key}))
        if not doc:
            try:
                detail = {"_id": self._key, "identity": identity, "expires": expires}
                await maybe_await(self._collection.insert_one(detail))
                self._lock_detail = dict(detail)
                return True
            except Exception:
                return False

        current_expires = doc.get("expires", now)
        if current_expires < now:
            result = await maybe_await(
                self._collection.update_one(
                {
                    "_id": self._key,
                    "expires": current_expires,
                },
                {"$set": {"identity": identity, "expires": expires}},
                upsert=False,
                )
            )
            if int(getattr(result, "matched_count", 0)) == 0:
                return False
            doc["identity"] = identity
            doc["expires"] = expires
            self._lock_detail = doc
            return True

        if identity and doc.get("identity") == identity:
            self._lock_detail = doc
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
        if self._lock_detail is None:
            raise AlreadyUnlockedError("dkit: already unlocked")
        result = await maybe_await(
            self._collection.delete_one({"_id": self._key, "expires": self._lock_detail["expires"]})
        )
        if int(getattr(result, "deleted_count", 0)) == 0:
            raise AlreadyUnlockedError("dkit: already unlocked")
        self._lock_detail = None

    async def exist_lock(self) -> bool:
        now = _now()
        doc = await maybe_await(self._collection.find_one({"_id": self._key}))
        if not doc:
            return False
        if doc.get("expires", now) <= now:
            await maybe_await(self._collection.delete_one({"_id": self._key}))
            return False
        return True


class MongoAtomic:
    def __init__(
        self,
        database: Any,
        collection_prefix: str = "dkit",
        default_ttl: float = 30.0,
        random_lease_seconds: float = 60,
    ) -> None:
        if database is None:
            raise InvalidOptionError("dkit: invalid option: mongo database is nil")
        self._database = database
        self._collection_prefix = collection_prefix or "dkit"
        self._default_ttl = default_ttl
        self._random_lease_seconds = random_lease_seconds
        self._random = database[f"{self._collection_prefix}_random"]
        self._mutex = database[f"{self._collection_prefix}_mutex"]
        self._election_coll = database[f"{self._collection_prefix}_elect"]
        self._heartbeat_coll = database[f"{self._collection_prefix}_heartbeat"]
        _ensure_ttl_index(self._random)
        _ensure_ttl_index(self._mutex)
        self._election: ElectionOption | None = None
        self._election_task: asyncio.Task[None] | None = None
        self._last_is_leader = False

    async def get_unique_random_number(self, max_value: int) -> int:
        if max_value <= 0:
            raise NoAvailableNumberError("dkit: no available number")
        now = _now()
        await maybe_await(self._random.delete_many({"expires": {"$lte": now}}))
        start = random.randrange(max_value)
        for offset in range(max_value):
            num = (start + offset) % max_value
            try:
                await maybe_await(
                    self._random.insert_one(
                    {
                        "_id": num,
                        "expires": now + timedelta(seconds=self._random_lease_seconds),
                    }
                )
                )
                return num
            except Exception:
                continue
        raise NoAvailableNumberError("dkit: no available number")

    def new_mutex(self, key: str) -> MongoMutex:
        return MongoMutex(self._mutex, key, self._default_ttl)

    def get_mutex_default_ttl(self) -> float:
        return self._default_ttl

    async def enable_election(self, opt: ElectionOption | None = None) -> None:
        option = opt or ElectionOption()
        if not option.node_key:
            raise InvalidOptionError("dkit: invalid option: node key is empty")
        if self._election is not None:
            raise InvalidOptionError("dkit: invalid option: election already enabled")
        _ensure_ttl_index(self._election_coll)
        if option.keep_heartbeat:
            _ensure_ttl_index(self._heartbeat_coll)
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
        leader = await maybe_await(self._election_coll.find_one({"_id": self._leader_id()}))
        return bool(
            leader and leader.get("node_key") == option.node_key and leader["expires"] > _now()
        )

    async def alive_nodes(self) -> list[str]:
        option = self._require_election()
        if not option.keep_heartbeat:
            raise InvalidOptionError("dkit: invalid option: heartbeat is disabled")
        now = _now()
        docs = await maybe_await(
            self._heartbeat_coll.find(
            {
                "isolation_key": option.isolation_key,
                "expires": {"$gt": now},
            }
        )
        )
        rows = await collect_async_iterable(docs)
        return sorted(str(doc.get("_id", "")) for doc in rows if doc.get("_id"))

    async def is_alive(self, node_key: str) -> bool:
        option = self._require_election()
        if not option.keep_heartbeat:
            raise InvalidOptionError("dkit: invalid option: heartbeat is disabled")
        doc = await maybe_await(self._heartbeat_coll.find_one({"_id": node_key}))
        return bool(
            doc
            and doc.get("isolation_key", option.isolation_key) == option.isolation_key
            and doc.get("expires", _now()) > _now()
        )

    async def close(self) -> None:
        option = self._election
        await self._stop_election_loop()
        if option is not None:
            await self._cleanup_election(option)
        self._election = None
        self._last_is_leader = False
        client = getattr(self._database, "client", None)
        close = getattr(client, "close", None)
        if callable(close):
            await maybe_await(close())

    def _start_election_loop(self) -> None:
        option = self._require_election()
        interval = _election_interval(option)
        self._election_task = asyncio.create_task(
            self._run_election_loop(interval), name=f"dkit-mongo-election-{option.node_key}"
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
        now = _now()
        if option.keep_heartbeat:
            await maybe_await(
                self._heartbeat_coll.update_one(
                {"_id": option.node_key},
                {"$set": self._heartbeat_doc(option.node_key, option, now)},
                upsert=True,
                )
            )

        leader_id = self._leader_id()
        leader = await maybe_await(self._election_coll.find_one({"_id": leader_id}))
        leader_alive = bool(leader and leader.get("expires", now) > now)
        if leader_alive and leader.get("node_key") == option.node_key:
            await maybe_await(
                self._election_coll.update_one(
                {
                    "_id": leader_id,
                    "node_key": option.node_key,
                    "expires": {"$gt": now},
                },
                {"$set": self._leader_doc(option, now)},
                upsert=False,
                )
            )
        elif not leader_alive and (option.can_elect is None or option.can_elect()):
            await self._try_become_leader(leader_id, leader, option, now)

        await self._notify_leader_change()

    async def _cleanup_election(self, option: ElectionOption) -> None:
        await maybe_await(
            self._election_coll.delete_one({"_id": self._leader_id_for(option), "node_key": option.node_key})
        )
        if option.keep_heartbeat:
            await maybe_await(
                self._heartbeat_coll.delete_one({"_id": option.node_key, "isolation_key": option.isolation_key})
            )

    def _require_election(self) -> ElectionOption:
        if self._election is None:
            raise ElectionNotEnabledError("dkit: election not enabled")
        return self._election

    def _leader_id(self) -> str:
        option = self._require_election()
        return self._leader_id_for(option)

    def _leader_id_for(self, option: ElectionOption) -> str:
        return f"leader-{option.isolation_key}" if option.isolation_key else "leader"

    async def _notify_leader_change(self) -> None:
        option = self._require_election()
        leader = await maybe_await(self._election_coll.find_one({"_id": self._leader_id()}))
        leader_key = (
            str(leader.get("node_key", ""))
            if leader and leader.get("expires", _now()) > _now()
            else ""
        )
        is_leader = leader_key == option.node_key
        if option.on_leader_changed is None or is_leader == self._last_is_leader:
            self._last_is_leader = is_leader
            return
        self._last_is_leader = is_leader
        await maybe_await(
            option.on_leader_changed(
                LeaderChangedEvent(
                    node_key=option.node_key,
                    is_leader=is_leader,
                    isolation_key=option.isolation_key,
                    leader_key=leader_key,
                )
            )
        )

    def _leader_doc(self, option: ElectionOption, now: datetime) -> dict[str, Any]:
        return {
            "node_key": option.node_key,
            "isolation_key": option.isolation_key,
            "expires": now + timedelta(seconds=option.unhealthy_time),
        }

    async def _try_become_leader(
        self,
        leader_id: str,
        leader: dict[str, Any] | None,
        option: ElectionOption,
        now: datetime,
    ) -> None:
        next_leader = self._leader_doc(option, now)
        if leader is None:
            try:
                await maybe_await(self._election_coll.insert_one({"_id": leader_id, **next_leader}))
            except Exception:
                return
            return

        result = await maybe_await(
            self._election_coll.update_one(
                {
                    "_id": leader_id,
                    "node_key": leader.get("node_key"),
                    "expires": leader.get("expires", now),
                },
                {"$set": next_leader},
                upsert=False,
            )
        )
        if int(getattr(result, "matched_count", 0)) == 0:
            return

    def _heartbeat_doc(
        self, node_key: str, option: ElectionOption, now: datetime
    ) -> dict[str, Any]:
        return {
            "_id": node_key,
            "isolation_key": option.isolation_key,
            "expires": now + timedelta(seconds=option.unhealthy_time),
        }
def _ensure_ttl_index(collection: Any) -> None:
    if collection is None:
        raise InvalidOptionError("dkit: invalid option: mongo collection is nil")
    create_index = getattr(collection, "create_index", None)
    if not callable(create_index):
        return
    try:
        create_index([("expires", 1)], expireAfterSeconds=1)
    except Exception as exc:
        if not _is_index_option_conflict(exc):
            raise RuntimeError(f"create ttl index failed: {exc}") from exc
        _drop_indexes(collection, exc)
        try:
            create_index([("expires", 1)], expireAfterSeconds=1)
        except Exception as create_exc:
            raise RuntimeError(f"recreate ttl index failed: {create_exc}") from create_exc


def _drop_indexes(collection: Any, err: Exception) -> None:
    drop_indexes = getattr(collection, "drop_indexes", None)
    if callable(drop_indexes):
        try:
            drop_indexes()
            return
        except Exception as drop_err:
            raise RuntimeError(f"drop indexes failed: {drop_err}") from drop_err
    raise RuntimeError(f"drop indexes failed: {err}") from err


def _is_index_option_conflict(err: Exception) -> bool:
    return int(getattr(err, "code", 0) or 0) == 85
def _now() -> datetime:
    return datetime.now(UTC)


def _election_interval(option: ElectionOption) -> float:
    candidates = [option.timeout, option.unhealthy_time / 3]
    positives = [value for value in candidates if value > 0]
    return max(0.05, min(positives or [1.0]))
