"""Small async helpers used by async-first public APIs."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterable, AsyncIterator, Awaitable
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar

T = TypeVar("T")


async def maybe_await(value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return await value
    return value


class AsyncReadable(Protocol):
    async def read(self, size: int = -1) -> bytes: ...
    async def aclose(self) -> None: ...


@dataclass(slots=True)
class AsyncReadableAdapter:
    body: Any
    _buffer: bytearray = field(default_factory=bytearray)
    _iter: AsyncIterator[bytes] | None = None
    _eof: bool = False

    async def read(self, size: int = -1) -> bytes:
        reader = getattr(self.body, "read", None)
        if callable(reader):
            return await maybe_await(reader(size))
        aread = getattr(self.body, "aread", None)
        if callable(aread):
            return await maybe_await(aread(size))
        if self._iter is None:
            if hasattr(self.body, "__aiter__"):
                self._iter = self.body.__aiter__()
            else:
                raise TypeError("body does not support async reads")
        if size < 0:
            chunks = [bytes(self._buffer)]
            self._buffer.clear()
            async for chunk in self._iter:
                chunks.append(chunk)
            self._eof = True
            return b"".join(chunks)
        while len(self._buffer) < size and not self._eof:
            try:
                chunk = await self._iter.__anext__()
            except StopAsyncIteration:
                self._eof = True
                break
            self._buffer.extend(chunk)
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data

    async def aclose(self) -> None:
        aclose = getattr(self.body, "aclose", None)
        if callable(aclose):
            await maybe_await(aclose())
            return
        close = getattr(self.body, "close", None)
        if callable(close):
            await maybe_await(close())


def ensure_async_readable(body: Any) -> AsyncReadable:
    if isinstance(body, AsyncReadableAdapter):
        return body
    return AsyncReadableAdapter(body)


async def collect_async_iterable(value: Any) -> list[Any]:
    if isinstance(value, AsyncIterable):
        return [item async for item in value]
    return list(value)
