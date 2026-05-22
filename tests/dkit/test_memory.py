import pytest

from core_py import dkit


@pytest.mark.asyncio
async def test_dkit_election_not_enabled_by_default() -> None:
    atomic = dkit.InMemoryAtomic()

    with pytest.raises(dkit.ElectionNotEnabledError):
        await atomic.is_leader()


@pytest.mark.asyncio
async def test_memory_mutex_rejects_unlock_from_non_owner_and_random_exhaustion() -> None:
    atomic = dkit.InMemoryAtomic(default_ttl=1)
    mutex = atomic.new_mutex("job")
    intruder = atomic.new_mutex("job")

    assert await mutex.try_lock() is True
    with pytest.raises(dkit.AlreadyUnlockedError):
        await intruder.unlock()
    assert await mutex.exist_lock() is True
    await mutex.unlock()

    assert await atomic.get_unique_random_number(1) == 0
    with pytest.raises(dkit.NoAvailableNumberError):
        await atomic.get_unique_random_number(1)
