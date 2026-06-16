import time

import pytest

from core_py import data, dkit


@pytest.mark.asyncio
async def test_dkit_id_and_mutex_helpers() -> None:
    atomic = dkit.InMemoryAtomic(default_ttl=1)
    kit = await dkit.new_default_kit(atomic)
    called: list[str] = []

    await kit.mutex_do("job", lambda: called.append(kit.get_id_string()))

    assert called and called[0].isdigit()
    snowflake_id = kit.get_snowflake_id()
    assert isinstance(snowflake_id, str)
    assert int(snowflake_id) > 0
    assert len(snowflake_id) == 19
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


@pytest.mark.asyncio
async def test_default_kit_exposes_mutex_from_atomic_backend() -> None:
    atomic = dkit.InMemoryAtomic(default_ttl=1)
    kit = await dkit.new_default_kit(atomic)

    holder = kit.new_mutex("job")
    contender = atomic.new_mutex("job")
    assert await holder.try_lock(dkit.lock_ttl(1)) is True
    try:
        assert await contender.try_lock(dkit.lock_ttl(1)) is False
    finally:
        await holder.unlock()


@pytest.mark.asyncio
async def test_default_kit_locator_supports_replace_and_reset() -> None:
    dkit.reset_default_kit()
    with pytest.raises(
        dkit.DefaultKitNotConfiguredError, match="default kit not configured"
    ) as missing:
        dkit.default_kit()
    assert data.code_of(missing.value) == dkit.ERR_CODE_DKIT_DEFAULT_KIT_NOT_CONFIGURED

    first = await dkit.new_default_kit(dkit.InMemoryAtomic(default_ttl=1))
    second = await dkit.new_default_kit(dkit.InMemoryAtomic(default_ttl=1))

    dkit.set_default_kit(first)
    assert dkit.default_kit() is first

    with dkit.use_default_kit(second):
        assert dkit.default_kit() is second
    assert dkit.default_kit() is first

    dkit.reset_default_kit()
    with pytest.raises(
        dkit.DefaultKitNotConfiguredError, match="default kit not configured"
    ) as reset_missing:
        dkit.default_kit()
    assert data.code_of(reset_missing.value) == dkit.ERR_CODE_DKIT_DEFAULT_KIT_NOT_CONFIGURED
