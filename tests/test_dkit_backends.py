import fnmatch
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from core_py import dkit


class FakeRedis:
    def __init__(self) -> None:
        self.data: dict[str, tuple[str, float | None]] = {}
        self.closed = False

    def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        ex: int | None = None,
    ) -> bool:
        self._purge(key)
        if nx and key in self.data:
            return False
        expires_at = time.time() + ex if ex is not None else None
        self.data[key] = (value, expires_at)
        return True

    def get(self, key: str) -> str | None:
        self._purge(key)
        item = self.data.get(key)
        return item[0] if item else None

    def exists(self, key: str) -> int:
        self._purge(key)
        return int(key in self.data)

    def eval(self, script: str, keys_count: int, key: str, owner: str) -> int:
        del script, keys_count
        self._purge(key)
        item = self.data.get(key)
        if item and item[0] == owner:
            del self.data[key]
            return 1
        return 0

    def scan_iter(self, match: str) -> list[str]:
        for key in list(self.data):
            self._purge(key)
        return [key for key in self.data if fnmatch.fnmatch(key, match)]

    def close(self) -> None:
        self.closed = True

    def _purge(self, key: str) -> None:
        item = self.data.get(key)
        if item and item[1] is not None and item[1] <= time.time():
            del self.data[key]


@dataclass
class FakeDeleteResult:
    deleted_count: int


@dataclass
class FakeUpdateResult:
    matched_count: int


class FakeCollection:
    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {}

    def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        for doc in self.docs.values():
            if _matches(doc, query):
                return dict(doc)
        return None

    def update_one(
        self, query: dict[str, Any], update: dict[str, Any], upsert: bool = False
    ) -> FakeUpdateResult:
        doc = self.find_one(query)
        if doc is None:
            if not upsert:
                return FakeUpdateResult(matched_count=0)
            doc = {"_id": query["_id"]}
        doc.update(update.get("$set", {}))
        self.docs[str(doc["_id"])] = doc
        return FakeUpdateResult(matched_count=1)

    def insert_one(self, doc: dict[str, Any]) -> None:
        key = str(doc["_id"])
        if key in self.docs:
            raise ValueError("duplicate key")
        self.docs[key] = dict(doc)

    def delete_one(self, query: dict[str, Any]) -> FakeDeleteResult:
        for key, doc in list(self.docs.items()):
            if _matches(doc, query):
                del self.docs[key]
                return FakeDeleteResult(deleted_count=1)
        return FakeDeleteResult(deleted_count=0)

    def delete_many(self, query: dict[str, Any]) -> None:
        for key, doc in list(self.docs.items()):
            if _matches(doc, query):
                del self.docs[key]

    def find(self, query: dict[str, Any]) -> Iterable[dict[str, Any]]:
        return [dict(doc) for doc in self.docs.values() if _matches(doc, query)]


class FakeMongoDatabase:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}
        self.client = self
        self.closed = False

    def __getitem__(self, name: str) -> FakeCollection:
        if name not in self.collections:
            self.collections[name] = FakeCollection()
        return self.collections[name]

    def close(self) -> None:
        self.closed = True


def _matches(doc: dict[str, Any], query: dict[str, Any]) -> bool:
    for key, expected in query.items():
        if key == "$or":
            if not any(_matches(doc, item) for item in expected):
                return False
            continue
        actual = doc.get(key)
        if isinstance(expected, dict):
            if "$lte" in expected and not (actual <= expected["$lte"]):
                return False
            if "$gt" in expected and not (actual > expected["$gt"]):
                return False
        elif actual != expected:
            return False
    return True


def _wait_until(predicate: Any, timeout: float = 2.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


def test_dkit_election_not_enabled_by_default():
    atomic = dkit.InMemoryAtomic()

    with pytest.raises(dkit.ElectionNotEnabledError):
        atomic.is_leader()


def test_memory_mutex_rejects_unlock_from_non_owner_and_random_exhaustion():
    atomic = dkit.InMemoryAtomic(default_ttl=1)
    mutex = atomic.new_mutex("job")
    intruder = atomic.new_mutex("job")

    assert mutex.try_lock(None) is True
    with pytest.raises(dkit.AlreadyUnlockedError):
        intruder.unlock(None)
    assert mutex.exist_lock(None) is True
    mutex.unlock(None)

    assert atomic.get_unique_random_number(None, 1) == 0
    with pytest.raises(dkit.NoAvailableNumberError):
        atomic.get_unique_random_number(None, 1)


def test_redis_atomic_id_mutex_and_election_callback():
    client = FakeRedis()
    atomic = dkit.RedisAtomic(client, key_prefix="test", default_ttl=1)
    events: list[dkit.LeaderChangedEvent] = []

    assert 0 <= atomic.get_unique_random_number(None, 10) < 10
    mutex = atomic.new_mutex("job")
    assert mutex.try_lock(None) is True
    assert mutex.exist_lock(None) is True
    mutex.unlock(None)
    assert mutex.exist_lock(None) is False

    atomic.enable_election(
        dkit.ElectionOption(
            node_key="node-a",
            keep_heartbeat=True,
            isolation_key="iso",
            on_leader_changed=events.append,
        )
    )

    assert atomic.node_key() == "node-a"
    assert atomic.is_leader() is True
    assert atomic.is_alive("node-a") is True
    assert atomic.alive_nodes() == ["node-a"]
    assert events and events[0].is_leader is True
    atomic.close()
    assert client.closed is True


def test_redis_random_exhaustion_and_lease_release():
    client = FakeRedis()
    atomic = dkit.RedisAtomic(client, key_prefix="random", random_lease_seconds=1)

    assert atomic.get_unique_random_number(None, 1) == 0
    with pytest.raises(dkit.NoAvailableNumberError):
        atomic.get_unique_random_number(None, 1)
    time.sleep(1.05)
    assert atomic.get_unique_random_number(None, 1) == 0


def test_redis_election_failover_after_leader_stops():
    client = FakeRedis()
    events: list[tuple[str, bool]] = []
    leader = dkit.RedisAtomic(client, key_prefix="failover")
    follower = dkit.RedisAtomic(client, key_prefix="failover")

    leader.enable_election(
        dkit.ElectionOption(
            node_key="leader",
            keep_heartbeat=True,
            unhealthy_time=1,
            timeout=0.05,
            on_leader_changed=lambda event: events.append((event.node_key, event.is_leader)),
        )
    )
    follower.enable_election(
        dkit.ElectionOption(
            node_key="follower",
            keep_heartbeat=True,
            unhealthy_time=1,
            timeout=0.05,
            on_leader_changed=lambda event: events.append((event.node_key, event.is_leader)),
        )
    )

    assert leader.is_leader() is True
    assert follower.is_leader() is False
    leader.close()
    assert _wait_until(follower.is_leader, timeout=2.5)
    assert ("follower", True) in events
    follower.close()


def test_mongo_atomic_mutex_election_and_failover():
    database = FakeMongoDatabase()
    leader = dkit.MongoAtomic(database, collection_prefix="test")
    follower = dkit.MongoAtomic(database, collection_prefix="test")
    events: list[tuple[str, bool]] = []

    assert 0 <= leader.get_unique_random_number(None, 10) < 10
    mutex = leader.new_mutex("job")
    assert mutex.try_lock(None) is True
    assert mutex.exist_lock(None) is True
    mutex.unlock(None)
    assert mutex.exist_lock(None) is False

    leader.enable_election(
        dkit.ElectionOption(
            node_key="leader",
            keep_heartbeat=True,
            unhealthy_time=1,
            timeout=0.05,
            on_leader_changed=lambda event: events.append((event.node_key, event.is_leader)),
        )
    )
    follower.enable_election(
        dkit.ElectionOption(
            node_key="follower",
            keep_heartbeat=True,
            unhealthy_time=1,
            timeout=0.05,
            on_leader_changed=lambda event: events.append((event.node_key, event.is_leader)),
        )
    )

    assert leader.is_leader() is True
    assert follower.is_leader() is False
    assert set(follower.alive_nodes()) == {"leader", "follower"}
    leader.close()
    assert _wait_until(follower.is_leader, timeout=2.5)
    assert ("follower", True) in events
    follower.close()
    assert database.closed is True


def test_mongo_mutex_rejects_unlock_from_non_owner_and_random_lease_release():
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="lease", random_lease_seconds=0)
    mutex = atomic.new_mutex("job")
    intruder = atomic.new_mutex("job")

    assert mutex.try_lock(None) is True
    with pytest.raises(dkit.AlreadyUnlockedError):
        intruder.unlock(None)
    assert mutex.exist_lock(None) is True
    mutex.unlock(None)

    assert atomic.get_unique_random_number(None, 1) == 0
    assert atomic.get_unique_random_number(None, 1) == 0


def test_fake_collection_query_helpers_support_datetime_ranges():
    coll = FakeCollection()
    coll.insert_one({"_id": "a", "expires_at": datetime.now(UTC)})
    assert coll.find_one({"expires_at": {"$gt": datetime.fromtimestamp(0, UTC)}}) is not None
