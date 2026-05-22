import os

import pytest

from core_py import dkit


@pytest.mark.asyncio
async def test_redis_atomic_integration_when_configured():
    uri = os.getenv("OFA_DKIT_REDIS_URI")
    if not uri:
        pytest.skip("OFA_DKIT_REDIS_URI is not set")
    redis = pytest.importorskip("redis")
    client = redis.Redis.from_url(uri)
    atomic = dkit.RedisAtomic(client, key_prefix="core_py_test", default_ttl=1)

    mutex = atomic.new_mutex("integration")
    assert await mutex.try_lock() is True
    await mutex.unlock()
    await atomic.close()


@pytest.mark.asyncio
async def test_mongo_atomic_integration_when_configured():
    uri = os.getenv("OFA_DKIT_MONGO_URI")
    if not uri:
        pytest.skip("OFA_DKIT_MONGO_URI is not set")
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient(uri)
    database = client["core_py_dkit_test"]
    atomic = dkit.MongoAtomic(database, collection_prefix="dkit", default_ttl=1)

    mutex = atomic.new_mutex("integration")
    assert await mutex.try_lock() is True
    await mutex.unlock()
    await atomic.close()
