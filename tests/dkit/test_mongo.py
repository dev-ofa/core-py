import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from core_py import dkit
from core_py.dkit import mongo as dkit_mongo

from .helpers import FakeCollection, FakeMongoDatabase, wait_until_async


@pytest.mark.asyncio
async def test_mongo_atomic_mutex_election_and_failover() -> None:
    database = FakeMongoDatabase()
    leader = dkit.MongoAtomic(database, collection_prefix="test")
    follower = dkit.MongoAtomic(database, collection_prefix="test")
    events: list[tuple[str, bool, str]] = []

    assert leader.node_key() == ""
    assert await leader.is_leader() is False

    assert 0 <= await leader.get_unique_random_number(10) < 10
    mutex = leader.new_mutex("job")
    assert await mutex.try_lock() is True
    assert await mutex.exist_lock() is True
    await mutex.unlock()
    assert await mutex.exist_lock() is False

    await leader.enable_election(
        dkit.ElectionOption(
            node_key="leader",
            keep_heartbeat=True,
            unhealthy_time=1,
            timeout=0.05,
            on_leader_changed=lambda event: events.append(
                (event.node_key, event.is_leader, event.isolation_key)
            ),
        )
    )
    await follower.enable_election(
        dkit.ElectionOption(
            node_key="follower",
            keep_heartbeat=True,
            unhealthy_time=1,
            timeout=0.05,
            on_leader_changed=lambda event: events.append(
                (event.node_key, event.is_leader, event.isolation_key)
            ),
        )
    )

    assert await leader.is_leader() is True
    assert await follower.is_leader() is False
    assert set(await follower.alive_nodes()) == {"leader", "follower"}
    await leader.close()
    assert "leader" not in set(await follower.alive_nodes())
    assert await wait_until_async(follower.is_leader, timeout=2.5)
    assert ("follower", True, "") in events
    await follower.close()
    assert follower.node_key() == ""
    assert database.closed is True


@pytest.mark.asyncio
async def test_mongo_builder_options_create_atomic() -> None:
    database = FakeMongoDatabase()
    atomic = dkit_mongo.MongoAtomic(
        database=database,
        collection_prefix="builder",
        default_ttl=2.0,
    )
    mutex = atomic.new_mutex("job")
    assert await mutex.try_lock() is True
    assert "builder_mutex" in database.collections
    assert database["builder_random"].indexes["expires"]["expireAfterSeconds"] == 1
    assert database["builder_mutex"].indexes["expires"]["expireAfterSeconds"] == 1

    with pytest.raises(dkit.InvalidOptionError):
        dkit_mongo.MongoAtomic(database=None)


@pytest.mark.asyncio
async def test_mongo_election_requires_heartbeat_for_alive_queries() -> None:
    atomic = dkit.MongoAtomic(FakeMongoDatabase(), collection_prefix="no_heartbeat")
    await atomic.enable_election(dkit.ElectionOption(node_key="node-a", keep_heartbeat=False))

    with pytest.raises(dkit.InvalidOptionError):
        await atomic.alive_nodes()
    with pytest.raises(dkit.InvalidOptionError):
        await atomic.is_alive("node-a")

    await atomic.close()


@pytest.mark.asyncio
async def test_mongo_enable_election_twice_raises_invalid_option() -> None:
    atomic = dkit.MongoAtomic(FakeMongoDatabase(), collection_prefix="dup_election")
    await atomic.enable_election(dkit.ElectionOption(node_key="node-a"))

    with pytest.raises(dkit.InvalidOptionError, match="election already enabled"):
        await atomic.enable_election(dkit.ElectionOption(node_key="node-b"))

    await atomic.close()


@pytest.mark.asyncio
async def test_mongo_mutex_rejects_unlock_from_non_owner_and_random_lease_release() -> None:
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="lease", random_lease_seconds=0)
    mutex = atomic.new_mutex("job")
    intruder = atomic.new_mutex("job")

    assert await mutex.try_lock() is True
    with pytest.raises(dkit.AlreadyUnlockedError):
        await intruder.unlock()
    assert await mutex.exist_lock() is True
    await mutex.unlock()

    assert await atomic.get_unique_random_number(1) == 0
    assert await atomic.get_unique_random_number(1) == 0


@pytest.mark.asyncio
async def test_mongo_mutex_expired_lock_reclaim_uses_expires_cas() -> None:
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="cas", default_ttl=0.05)
    holder = atomic.new_mutex("job")
    contender = atomic.new_mutex("job")

    assert await holder.try_lock() is True
    await asyncio.sleep(0.06)

    coll = database["cas_mutex"]

    def steal_lock(query: dict[str, object], update: dict[str, object], upsert: bool) -> None:
        del query, update, upsert
        doc = coll.docs["job"]
        doc["identity"] = "other"
        doc["expires"] = datetime.now(UTC) + timedelta(seconds=1)

    coll.before_update = steal_lock

    assert await contender.try_lock() is False
    doc = coll.find_one({"_id": "job"})
    assert doc is not None
    assert doc["identity"] == "other"
    assert doc["expires"] > datetime.now(UTC)


@pytest.mark.asyncio
async def test_mongo_mutex_reentrant_identity_matches_core_go_semantics() -> None:
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="reentrant", default_ttl=0.05)
    first = atomic.new_mutex("job")
    second = atomic.new_mutex("job")

    assert await first.try_lock(dkit.reentrant("worker-a")) is True
    assert await second.try_lock(dkit.reentrant("worker-a")) is True
    await second.unlock()
    assert await first.exist_lock() is False


@pytest.mark.asyncio
async def test_mongo_election_reclaim_uses_observed_leader_state_cas() -> None:
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="elect_cas")
    atomic._election = dkit.ElectionOption(node_key="node-b", unhealthy_time=1)
    coll = database["elect_cas_elect"]
    coll.insert_one(
        {
            "_id": "leader",
            "node_key": "node-a",
            "isolation_key": "",
            "expires": datetime.now(UTC) - timedelta(seconds=1),
        }
    )

    def revive_leader(query: dict[str, object], update: dict[str, object], upsert: bool) -> None:
        del query, update, upsert
        doc = coll.docs["leader"]
        doc["expires"] = datetime.now(UTC) + timedelta(seconds=1)

    coll.before_update = revive_leader

    await atomic._tick_election()

    doc = coll.find_one({"_id": "leader"})
    assert doc is not None
    assert doc["node_key"] == "node-a"
    assert doc["expires"] > datetime.now(UTC)
    assert await atomic.is_leader() is False


@pytest.mark.asyncio
async def test_mongo_mutex_stale_holder_cannot_reacquire_after_other_owner_takes_lock() -> None:
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="stale", default_ttl=0.05)
    first = atomic.new_mutex("job")
    second = atomic.new_mutex("job")

    assert await first.try_lock() is True
    await asyncio.sleep(0.06)

    assert await second.try_lock() is True
    assert await first.try_lock() is False


def test_mongo_default_election_values_align_with_core_go() -> None:
    opt = dkit.ElectionOption(node_key="node-a")

    assert opt.unhealthy_time == 5.0
    assert opt.timeout == 2.0


@pytest.mark.asyncio
async def test_mongo_enable_election_creates_ttl_indexes() -> None:
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="ttl")

    await atomic.enable_election(dkit.ElectionOption(node_key="node-a", keep_heartbeat=True))

    assert database["ttl_elect"].indexes["expires"]["expireAfterSeconds"] == 1
    assert database["ttl_heartbeat"].indexes["expires"]["expireAfterSeconds"] == 1
    await atomic.close()


@pytest.mark.asyncio
async def test_mongo_recreates_conflicting_ttl_indexes() -> None:
    database = FakeMongoDatabase()
    database["conflict_random"].create_index([("expires", 1)], expireAfterSeconds=5)
    database["conflict_mutex"].create_index([("expires", 1)], expireAfterSeconds=9)
    database["conflict_elect"].create_index([("expires", 1)], expireAfterSeconds=7)
    database["conflict_heartbeat"].create_index([("expires", 1)], expireAfterSeconds=11)

    atomic = dkit.MongoAtomic(database, collection_prefix="conflict")
    await atomic.enable_election(dkit.ElectionOption(node_key="node-a", keep_heartbeat=True))

    assert database["conflict_random"].indexes["expires"]["expireAfterSeconds"] == 1
    assert database["conflict_mutex"].indexes["expires"]["expireAfterSeconds"] == 1
    assert database["conflict_elect"].indexes["expires"]["expireAfterSeconds"] == 1
    assert database["conflict_heartbeat"].indexes["expires"]["expireAfterSeconds"] == 1
    await atomic.close()


@pytest.mark.asyncio
async def test_mongo_ttl_indexes_auto_cleanup_expired_mutex_and_heartbeat_docs() -> None:
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="cleanup", default_ttl=0.1)
    mutex = atomic.new_mutex("job")
    assert await mutex.try_lock() is True
    await atomic.enable_election(
        dkit.ElectionOption(node_key="node-a", keep_heartbeat=True, unhealthy_time=0.1, timeout=0.05)
    )
    await atomic._stop_election_loop()

    await asyncio.sleep(1.2)

    assert database["cleanup_mutex"].find_one({"_id": "job"}) is None
    assert database["cleanup_heartbeat"].find_one({"_id": "node-a"}) is None
    await atomic.close()


def test_fake_collection_query_helpers_support_datetime_ranges() -> None:
    coll = FakeCollection()
    coll.insert_one({"_id": "a", "expires_at": datetime.now(UTC)})
    assert coll.find_one({"expires_at": {"$gt": datetime.fromtimestamp(0, UTC)}}) is not None


@pytest.mark.asyncio
async def test_mongo_atomic_uses_core_go_compatible_collections_and_fields() -> None:
    database = FakeMongoDatabase()
    atomic = dkit.MongoAtomic(database, collection_prefix="compat", default_ttl=1)

    assert await atomic.get_unique_random_number(1) == 0
    mutex = atomic.new_mutex("job")
    assert await mutex.try_lock() is True
    await atomic.enable_election(
        dkit.ElectionOption(node_key="node-a", keep_heartbeat=True, isolation_key="tenant")
    )

    assert set(database.collections) >= {"compat_random", "compat_mutex", "compat_elect", "compat_heartbeat"}
    assert database["compat_random"].find_one({"_id": 0, "expires": {"$gt": datetime.fromtimestamp(0, UTC)}})
    assert database["compat_mutex"].find_one({"_id": "job", "expires": {"$gt": datetime.fromtimestamp(0, UTC)}})
    assert database["compat_elect"].find_one({"_id": "leader-tenant", "node_key": "node-a"}) is not None
    assert database["compat_heartbeat"].find_one({"_id": "node-a", "isolation_key": "tenant"}) is not None
    await atomic.close()
