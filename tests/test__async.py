from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from core_py import _async


class _AwaitableClose:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _ReaderBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.calls.append(size)
        if size < 0:
            return self.payload
        return self.payload[:size]


class _AsyncReaderBody:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[int] = []
        self.closed = False

    async def aread(self, size: int = -1) -> bytes:
        self.calls.append(size)
        if size < 0:
            return self.payload
        return self.payload[:size]

    async def aclose(self) -> None:
        self.closed = True


class _IterableBody:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.closed = False

    def __aiter__(self) -> AsyncIterator[bytes]:
        async def gen() -> AsyncIterator[bytes]:
            for chunk in self._chunks:
                yield chunk

        return gen()

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_maybe_await_supports_plain_values_and_awaitables() -> None:
    assert await _async.maybe_await(1) == 1

    async def provide() -> str:
        return "ok"

    assert await _async.maybe_await(provide()) == "ok"


@pytest.mark.asyncio
async def test_async_readable_adapter_prefers_read_method() -> None:
    body = _ReaderBody(b"payload")
    adapter = _async.AsyncReadableAdapter(body)

    assert await adapter.read(3) == b"pay"
    assert body.calls == [3]


@pytest.mark.asyncio
async def test_async_readable_adapter_supports_aread_with_size() -> None:
    body = _AsyncReaderBody(b"payload")
    adapter = _async.AsyncReadableAdapter(body)

    assert await adapter.read(4) == b"payl"
    assert await adapter.read() == b"payload"
    assert body.calls == [4, -1]
    await adapter.aclose()
    assert body.closed is True


@pytest.mark.asyncio
async def test_async_readable_adapter_reads_from_async_iterator_with_buffering() -> None:
    body = _IterableBody([b"ab", b"cd", b"ef"])
    adapter = _async.AsyncReadableAdapter(body)

    assert await adapter.read(3) == b"abc"
    assert await adapter.read(2) == b"de"
    assert await adapter.read() == b"f"
    await adapter.aclose()
    assert body.closed is True


@pytest.mark.asyncio
async def test_async_readable_adapter_rejects_unreadable_body() -> None:
    with pytest.raises(TypeError, match="body does not support async reads"):
        await _async.AsyncReadableAdapter(object()).read()


@pytest.mark.asyncio
async def test_ensure_async_readable_and_collect_async_iterable() -> None:
    adapter = _async.AsyncReadableAdapter(_ReaderBody(b"x"))
    assert _async.ensure_async_readable(adapter) is adapter

    wrapped = _async.ensure_async_readable(_ReaderBody(b"y"))
    assert await wrapped.read() == b"y"

    async def gen() -> AsyncIterator[int]:
        yield 1
        yield 2

    assert await _async.collect_async_iterable(gen()) == [1, 2]
    assert await _async.collect_async_iterable((3, 4)) == [3, 4]


@pytest.mark.asyncio
async def test_async_readable_adapter_aclose_falls_back_to_close() -> None:
    body = _AwaitableClose()
    await _async.AsyncReadableAdapter(body).aclose()
    assert body.closed is True
