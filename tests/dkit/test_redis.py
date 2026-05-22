import asyncio

import pytest

from core_py import dkit
from core_py.dkit import redis as dkit_redis

from .helpers import FakeRedis, wait_until_async


@pytest.mark.asyncio
async def test_redis_atomic_id_mutex_and_election_callback() -> None:
    client = FakeRedis()
    atomic = dkit.RedisAtomic(client, key_prefix="test", default_ttl=1)
    events: list[dkit.LeaderChangedEvent] = []

    assert atomic.node_key() == ""
    assert await atomic.is_leader() is False

    assert 0 <= await atomic.get_unique_random_number(10) < 10
    mutex = atomic.new_mutex("job")
    assert await mutex.try_lock() is True
    assert await mutex.exist_lock() is True
    await mutex.unlock()
    assert await mutex.exist_lock() is False

    await atomic.enable_election(
        dkit.ElectionOption(
            node_key="node-a",
            keep_heartbeat=True,
            isolation_key="iso",
            on_leader_changed=events.append,
        )
    )

    assert atomic.node_key() == "node-a"
    assert await atomic.is_leader() is True
    assert await atomic.is_alive("node-a") is True
    assert await atomic.alive_nodes() == ["node-a"]
    assert events and events[0].is_leader is True
    assert events[0].isolation_key == "iso"
    await atomic.close()
    assert atomic.node_key() == ""
    assert await atomic.is_leader() is False
    with pytest.raises(dkit.ElectionNotEnabledError):
        await atomic.is_alive("node-a")
    assert client.closed is True


@pytest.mark.asyncio
async def test_redis_builder_options_create_atomic() -> None:
    client = FakeRedis()
    atomic = dkit_redis.RedisAtomic(
        client=client,
        key_prefix="builder",
        default_ttl=2.0,
    )
    mutex = atomic.new_mutex("job")
    assert await mutex.try_lock() is True
    assert "builder:mutex:job" in client.data

    with pytest.raises(dkit.InvalidOptionError):
        dkit_redis.RedisAtomic(client=None)


@pytest.mark.asyncio
async def test_redis_election_requires_heartbeat_for_alive_queries() -> None:
    atomic = dkit.RedisAtomic(FakeRedis(), key_prefix="no-heartbeat")
    await atomic.enable_election(dkit.ElectionOption(node_key="node-a", keep_heartbeat=False))

    with pytest.raises(dkit.InvalidOptionError):
        await atomic.alive_nodes()
    with pytest.raises(dkit.InvalidOptionError):
        await atomic.is_alive("node-a")

    await atomic.close()


@pytest.mark.asyncio
async def test_redis_enable_election_twice_raises_invalid_option() -> None:
    atomic = dkit.RedisAtomic(FakeRedis(), key_prefix="dup-election")
    await atomic.enable_election(dkit.ElectionOption(node_key="node-a"))

    with pytest.raises(dkit.InvalidOptionError, match="election already enabled"):
        await atomic.enable_election(dkit.ElectionOption(node_key="node-b"))

    await atomic.close()


@pytest.mark.asyncio
async def test_redis_random_exhaustion_and_lease_release() -> None:
    client = FakeRedis()
    atomic = dkit.RedisAtomic(client, key_prefix="random", random_lease_seconds=1)

    assert await atomic.get_unique_random_number(1) == 0
    with pytest.raises(dkit.NoAvailableNumberError):
        await atomic.get_unique_random_number(1)
    await asyncio.sleep(1.05)
    assert await atomic.get_unique_random_number(1) == 0


@pytest.mark.asyncio
async def test_redis_random_lease_supports_subsecond_precision() -> None:
    client = FakeRedis()
    atomic = dkit.RedisAtomic(client, key_prefix="random-ms", random_lease_seconds=0.2)

    assert await atomic.get_unique_random_number(1) == 0
    with pytest.raises(dkit.NoAvailableNumberError):
        await atomic.get_unique_random_number(1)
    await asyncio.sleep(0.25)
    assert await atomic.get_unique_random_number(1) == 0


def test_redis_default_election_values_align_with_core_go() -> None:
    opt = dkit.ElectionOption(node_key="node-a")

    assert opt.unhealthy_time == 5.0
    assert opt.timeout == 2.0


@pytest.mark.asyncio
async def test_redis_election_failover_after_leader_stops() -> None:
    client = FakeRedis()
    events: list[tuple[str, bool, str]] = []
    leader = dkit.RedisAtomic(client, key_prefix="failover")
    follower = dkit.RedisAtomic(client, key_prefix="failover")

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
    await leader.close()
    assert await wait_until_async(follower.is_leader, timeout=2.5)
    assert ("follower", True, "") in events
    await follower.close()


@pytest.mark.asyncio
async def test_redis_atomic_uses_core_go_compatible_key_layout() -> None:
    client = FakeRedis()
    atomic = dkit.RedisAtomic(client, key_prefix="compat", default_ttl=1)

    assert await atomic.get_unique_random_number(1) == 0
    mutex = atomic.new_mutex("job")
    assert await mutex.try_lock() is True
    await atomic.enable_election(
        dkit.ElectionOption(node_key="node:a", keep_heartbeat=True, isolation_key="tenant/a")
    )

    assert "compat:random:0" in client.data
    assert "compat:mutex:job" in client.data
    assert "compat:leader:dGVuYW50L2E" in client.data
    assert "compat:heartbeat:dGVuYW50L2E:bm9kZTph" in client.data
    await atomic.close()


@pytest.mark.asyncio
async def test_redis_mutex_supports_subsecond_ttl_precision() -> None:
    client = FakeRedis()
    atomic = dkit.RedisAtomic(client, key_prefix="ttl", default_ttl=0.2)
    mutex = atomic.new_mutex("job")

    assert await mutex.try_lock() is True
    await asyncio.sleep(0.25)
    assert await mutex.exist_lock() is False


@pytest.mark.asyncio
async def test_redis_election_heartbeat_supports_subsecond_unhealthy_time() -> None:
    client = FakeRedis()
    atomic = dkit.RedisAtomic(client, key_prefix="hb-ttl")
    await atomic.enable_election(
        dkit.ElectionOption(node_key="node-a", keep_heartbeat=True, unhealthy_time=0.2, timeout=0.05)
    )

    assert await atomic.is_alive("node-a") is True
    await atomic._stop_election_loop()
    await asyncio.sleep(0.25)
    assert await atomic.is_alive("node-a") is False
    await atomic.close()
