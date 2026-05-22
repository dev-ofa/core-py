import time

import pytest

from core_py import dkit


@pytest.mark.asyncio
async def test_dkit_id_and_mutex_helpers() -> None:
    atomic = dkit.InMemoryAtomic(default_ttl=1)
    kit = await dkit.new_default_kit(atomic)
    called: list[str] = []

    await kit.mutex_do("job", lambda: called.append(kit.get_id_string()))

    assert called and called[0].isdigit()
    assert await atomic.new_mutex("job").exist_lock() is False


@pytest.mark.asyncio
async def test_default_kit_mutex_do_uses_default_ttl_as_wait_bound() -> None:
    atomic = dkit.InMemoryAtomic(default_ttl=0.05)
    kit = await dkit.new_default_kit(atomic)
    holder = atomic.new_mutex("busy")
    assert await holder.try_lock(dkit.lock_ttl(1)) is True

    started = time.time()
    try:
        try:
            await kit.mutex_do("busy", lambda: None)
        except dkit.LockNotAcquiredError:
            pass
        else:
            raise AssertionError("expected LockNotAcquiredError")
        assert time.time() - started < 0.5
    finally:
        await holder.unlock()


@pytest.mark.asyncio
async def test_default_kit_ids_do_not_repeat() -> None:
    atomic = dkit.InMemoryAtomic(default_ttl=1)
    kit = await dkit.new_default_kit(atomic)

    ids = {kit.get_id() for _ in range(100)}

    assert len(ids) == 100
