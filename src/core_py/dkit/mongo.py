"""MongoDB-backed dkit primitives.

Pass a PyMongo database object to `MongoAtomic`. The module uses duck typing so
the base package does not need PyMongo at import time.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from core_py import context as ctx_mod
from core_py.dkit.errors import (
    AlreadyUnlockedError,
    ElectionNotEnabledError,
    InvalidOptionError,
    LockNotAcquiredError,
    NoAvailableNumberError,
)
from core_py.dkit.options import ElectionOption, LeaderChangedEvent, LockOptionOp, new_lock_option


class MongoMutex:
    def __init__(self, collection: Any, key: str, default_ttl: float) -> None:
        self._collection = collection
        self._key = key
        self._default_ttl = default_ttl
        self._owner = uuid.uuid4().hex
        self._locked_owner = ""

    def try_lock(self, ctx: ctx_mod.Context | None = None, *ops: LockOptionOp) -> bool:
        opt = new_lock_option(self._default_ttl, list(ops))
        owner = opt.reentrant_identity or self._owner
        now = _now()
        expires_at = now + timedelta(seconds=opt.ttl)
        doc = self._collection.find_one({"_id": self._key})
        if doc and doc.get("expires_at", now) > now and doc.get("owner") != owner:
            return False
        if doc:
            result = self._collection.update_one(
                {
                    "_id": self._key,
                    "$or": [{"expires_at": {"$lte": now}}, {"owner": owner}],
                },
                {"$set": {"owner": owner, "expires_at": expires_at}},
                upsert=False,
            )
            if int(getattr(result, "matched_count", 0)) == 0:
                return False
        else:
            try:
                self._collection.insert_one(
                    {"_id": self._key, "owner": owner, "expires_at": expires_at}
                )
            except Exception:
                return False
        self._locked_owner = owner
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
        owner = self._locked_owner or self._owner
        result = self._collection.delete_one({"_id": self._key, "owner": owner})
        if int(getattr(result, "deleted_count", 0)) == 0:
            raise AlreadyUnlockedError("dkit: already unlocked")
        self._locked_owner = ""

    def exist_lock(self, ctx: ctx_mod.Context | None = None) -> bool:
        now = _now()
        doc = self._collection.find_one({"_id": self._key})
        if not doc:
            return False
        if doc.get("expires_at", now) <= now:
            self._collection.delete_one({"_id": self._key})
            return False
        return True


class MongoAtomic:
    def __init__(
        self,
        database: Any,
        collection_prefix: str = "dkit",
        default_ttl: float = 30.0,
        random_lease_seconds: int = 60,
    ) -> None:
        if database is None:
            raise InvalidOptionError("dkit: invalid option: mongo database is nil")
        self._database = database
        self._collection_prefix = collection_prefix or "dkit"
        self._default_ttl = default_ttl
        self._random_lease_seconds = random_lease_seconds
        self._random = database[f"{self._collection_prefix}_random"]
        self._mutex = database[f"{self._collection_prefix}_mutex"]
        self._election_coll = database[f"{self._collection_prefix}_election"]
        self._election: ElectionOption | None = None
        self._election_stop = threading.Event()
        self._election_thread: threading.Thread | None = None
        self._last_is_leader = False

    def get_unique_random_number(self, ctx: ctx_mod.Context | None, max_value: int) -> int:
        if max_value <= 0:
            raise NoAvailableNumberError("dkit: no available number")
        now = _now()
        self._random.delete_many({"expires_at": {"$lte": now}})
        start = random.randrange(max_value)
        for offset in range(max_value):
            num = (start + offset) % max_value
            key = f"{max_value}:{num}"
            try:
                self._random.insert_one(
                    {
                        "_id": key,
                        "expires_at": now + timedelta(seconds=self._random_lease_seconds),
                    }
                )
                return num
            except Exception:
                continue
        raise NoAvailableNumberError("dkit: no available number")

    def new_mutex(self, key: str) -> MongoMutex:
        return MongoMutex(self._mutex, key, self._default_ttl)

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
        leader = self._election_coll.find_one({"_id": self._leader_id()})
        return bool(
            leader and leader.get("node_key") == option.node_key and leader["expires_at"] > _now()
        )

    def alive_nodes(self) -> list[str]:
        option = self._require_election()
        now = _now()
        if option.keep_heartbeat:
            self._election_coll.update_one(
                {"_id": self._heartbeat_id(option.node_key)},
                {
                    "$set": {
                        "kind": "heartbeat",
                        "node_key": option.node_key,
                        "isolation_key": option.isolation_key,
                        "expires_at": now + timedelta(seconds=option.unhealthy_time),
                    }
                },
                upsert=True,
            )
        docs = self._election_coll.find(
            {
                "kind": "heartbeat",
                "isolation_key": option.isolation_key,
                "expires_at": {"$gt": now},
            }
        )
        return sorted(str(doc.get("node_key", "")) for doc in docs if doc.get("node_key"))

    def is_alive(self, node_key: str) -> bool:
        option = self._require_election()
        doc = self._election_coll.find_one({"_id": self._heartbeat_id(node_key)})
        return bool(
            doc
            and doc.get("isolation_key", option.isolation_key) == option.isolation_key
            and doc.get("expires_at", _now()) > _now()
        )

    def close(self) -> None:
        self._stop_election_loop()
        client = getattr(self._database, "client", None)
        close = getattr(client, "close", None)
        if callable(close):
            close()

    def _start_election_loop(self) -> None:
        option = self._require_election()
        interval = _election_interval(option)
        self._election_stop.clear()
        self._election_thread = threading.Thread(
            target=self._run_election_loop,
            args=(interval,),
            name=f"dkit-mongo-election-{option.node_key}",
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
        now = _now()
        if option.keep_heartbeat:
            self._election_coll.update_one(
                {"_id": self._heartbeat_id(option.node_key)},
                {"$set": self._heartbeat_doc(option, now)},
                upsert=True,
            )

        leader_id = self._leader_id()
        leader = self._election_coll.find_one({"_id": leader_id})
        leader_alive = bool(leader and leader.get("expires_at", now) > now)
        if leader_alive and leader.get("node_key") == option.node_key:
            self._election_coll.update_one(
                {"_id": leader_id, "node_key": option.node_key},
                {"$set": self._leader_doc(option, now)},
                upsert=True,
            )
        elif not leader_alive and (option.can_elect is None or option.can_elect()):
            self._election_coll.update_one(
                {"_id": leader_id},
                {"$set": self._leader_doc(option, now)},
                upsert=True,
            )

        self._notify_leader_change()

    def _require_election(self) -> ElectionOption:
        if self._election is None:
            raise ElectionNotEnabledError("dkit: election not enabled")
        return self._election

    def _leader_id(self) -> str:
        option = self._require_election()
        return f"leader:{option.isolation_key or 'default'}"

    def _heartbeat_id(self, node_key: str) -> str:
        option = self._require_election()
        return f"heartbeat:{option.isolation_key or 'default'}:{node_key}"

    def _notify_leader_change(self) -> None:
        option = self._require_election()
        leader = self._election_coll.find_one({"_id": self._leader_id()})
        leader_key = (
            str(leader.get("node_key", ""))
            if leader and leader.get("expires_at", _now()) > _now()
            else ""
        )
        is_leader = leader_key == option.node_key
        if option.on_leader_changed is None or is_leader == self._last_is_leader:
            self._last_is_leader = is_leader
            return
        self._last_is_leader = is_leader
        option.on_leader_changed(
            LeaderChangedEvent(
                node_key=option.node_key,
                is_leader=is_leader,
                leader_key=leader_key,
            )
        )

    def _leader_doc(self, option: ElectionOption, now: datetime) -> dict[str, Any]:
        return {
            "kind": "leader",
            "node_key": option.node_key,
            "isolation_key": option.isolation_key,
            "expires_at": now + timedelta(seconds=option.unhealthy_time),
        }

    def _heartbeat_doc(self, option: ElectionOption, now: datetime) -> dict[str, Any]:
        return {
            "kind": "heartbeat",
            "node_key": option.node_key,
            "isolation_key": option.isolation_key,
            "expires_at": now + timedelta(seconds=option.unhealthy_time),
        }


def new_mongo_atomic(
    database: Any,
    collection_prefix: str = "dkit",
    default_ttl: float = 30.0,
) -> MongoAtomic:
    return MongoAtomic(
        database=database, collection_prefix=collection_prefix, default_ttl=default_ttl
    )


def _now() -> datetime:
    return datetime.now(UTC)


def _election_interval(option: ElectionOption) -> float:
    candidates = [option.timeout, option.unhealthy_time / 3]
    positives = [value for value in candidates if value > 0]
    return max(0.05, min(positives or [1.0]))
