"""Resource identifier parsing and opening aligned with the OFA resource spec.

This module is ported from ``core-go/resource`` and adapted to Python APIs.
"""

from __future__ import annotations

import base64
import os
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from email.message import Message
from io import BytesIO
from typing import Any, Protocol

import aiofiles  # type: ignore[import-untyped]

from core_py import httpx
from core_py._async import AsyncReadable, ensure_async_readable, maybe_await

DEFAULT_MAX_BYTES = 32 << 20
DEFAULT_DATA_MAX_BYTES = 1 << 20
DEFAULT_TIMEOUT_QUOTA = 5.0
DEFAULT_CONNECT_TIMEOUT = 3.0
DEFAULT_REDIRECT_LIMIT = 5
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BASE_DELAY = 0.1
DEFAULT_RETRY_MAX_DELAY = 1.0

_PARAM_NAME_RE = re.compile(r"^[a-z0-9_]+$")
_SCHEME_RE = re.compile(r"^[a-z][a-z0-9_+.-]*$")


class ResourceError(Exception):
    pass


class UnsupportedSchemeError(ResourceError):
    pass


class OpenUnsupportedError(ResourceError):
    pass


class UploadUnsupportedError(ResourceError):
    pass


class SizeLimitExceededError(ResourceError):
    pass


class TimeoutBudgetExhaustedError(ResourceError):
    pass


ERR_UNSUPPORTED_SCHEME = UnsupportedSchemeError("resource: unsupported scheme")
ERR_OPEN_UNSUPPORTED = OpenUnsupportedError("resource: open unsupported")
ERR_UPLOAD_UNSUPPORTED = UploadUnsupportedError("resource: upload unsupported")
ERR_SIZE_LIMIT_EXCEEDED = SizeLimitExceededError("resource: size limit exceeded")
ERR_TIMEOUT_BUDGET_EXHAUSTED = TimeoutBudgetExhaustedError("resource: timeout budget exhausted")


@dataclass(slots=True)
class ParseError(ResourceError):
    raw: str
    err: Exception

    def __str__(self) -> str:
        return f"resource parse failed: {self.err}"


@dataclass(slots=True)
class OpenError(ResourceError):
    identifier: Identifier
    err: Exception

    def __str__(self) -> str:
        return f"resource open scheme={self.identifier.scheme} failed: {self.err}"


@dataclass(slots=True)
class DownloadError(ResourceError):
    dst_path: str
    err: Exception

    def __str__(self) -> str:
        return f"resource download dst={self.dst_path} failed: {self.err}"


@dataclass(slots=True)
class UploadError(ResourceError):
    scheme: str
    err: Exception

    def __str__(self) -> str:
        return f"resource upload scheme={self.scheme} failed: {self.err}"


@dataclass(slots=True)
class HTTPStatusError(ResourceError):
    status_code: int
    body: bytes = b""

    def __str__(self) -> str:
        return f"http status {self.status_code} is not expected, body: {self.body.decode(errors='replace')}"


@dataclass(slots=True)
class Identifier:
    raw: str
    params: dict[str, str]
    source_uri: str
    scheme: str
    auth_id: str = ""
    media_type: str = ""


@dataclass(slots=True)
class Stream:
    body: AsyncReadable | Any
    media_type: str = ""
    filename: str = ""
    size: int = -1
    source_uri: str = ""
    headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.body = ensure_async_readable(self.body)

    async def close(self) -> None:
        await self.body.aclose()

    async def __aenter__(self) -> Stream:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> None:
        await self.close()


@dataclass(slots=True)
class UploadInput:
    body: Any | None = None
    media_type: str = ""
    filename: str = ""
    auth_id: str = ""
    target_hint: str = ""


class ResourceHandler(Protocol):
    def open(self, identifier: Identifier) -> Stream | Awaitable[Stream]: ...
    def upload(self, value: UploadInput) -> Identifier | Awaitable[Identifier]: ...


OpenFunc = Callable[[Identifier], Stream | Awaitable[Stream]]
UploadFunc = Callable[[UploadInput], Identifier | Awaitable[Identifier]]


@dataclass(slots=True)
class HandlerFuncs:
    open_func: OpenFunc | None = None
    upload_func: UploadFunc | None = None

    async def open(self, identifier: Identifier) -> Stream:
        if self.open_func is None:
            raise OpenUnsupportedError("resource: open unsupported")
        return await maybe_await(self.open_func(identifier))

    async def upload(self, value: UploadInput) -> Identifier:
        if self.upload_func is None:
            raise UploadUnsupportedError("resource: upload unsupported")
        return await maybe_await(self.upload_func(value))


@dataclass(slots=True)
class Options:
    http_client: Any | None = None
    max_bytes: int = DEFAULT_MAX_BYTES
    data_max_bytes: int = DEFAULT_DATA_MAX_BYTES
    timeout_quota: float = DEFAULT_TIMEOUT_QUOTA
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    redirect_limit: int = DEFAULT_REDIRECT_LIMIT
    retry_attempts: int = DEFAULT_RETRY_ATTEMPTS
    retry_base_delay: float = DEFAULT_RETRY_BASE_DELAY
    retry_max_delay: float = DEFAULT_RETRY_MAX_DELAY
    enable_http: bool = True


Option = Callable[[Options], None]


def with_max_bytes(max_bytes: int) -> Option:
    def op(options: Options) -> None:
        if max_bytes > 0:
            options.max_bytes = max_bytes

    return op


def with_data_max_bytes(max_bytes: int) -> Option:
    def op(options: Options) -> None:
        if max_bytes > 0:
            options.data_max_bytes = max_bytes

    return op


def with_timeout_quota(timeout: float) -> Option:
    def op(options: Options) -> None:
        if timeout > 0:
            options.timeout_quota = timeout

    return op


def with_redirect_limit(limit: int) -> Option:
    def op(options: Options) -> None:
        if limit >= 0:
            options.redirect_limit = limit

    return op


def with_retry(attempts: int, base_delay: float, max_delay: float) -> Option:
    def op(options: Options) -> None:
        if attempts > 0:
            options.retry_attempts = attempts
        if base_delay > 0:
            options.retry_base_delay = base_delay
        if max_delay > 0:
            options.retry_max_delay = max_delay

    return op


def with_http_client(client: Any | None) -> Option:
    def op(options: Options) -> None:
        if client is not None:
            options.http_client = client

    return op


def with_http_enabled(enabled: bool) -> Option:
    def op(options: Options) -> None:
        options.enable_http = enabled

    return op


class Manager:
    def __init__(self, *ops: Option) -> None:
        self.opts = Options()
        for op in ops:
            op(self.opts)
        http_handler = _HTTPHandler(self.opts)
        self._handlers: dict[str, ResourceHandler] = {
            "https": http_handler,
            "data": _DataHandler(self.opts),
        }
        if self.opts.enable_http:
            self._handlers["http"] = http_handler

    def register(self, scheme: str, handler: ResourceHandler) -> None:
        if handler is None:
            raise ValueError("handler is nil")
        if not scheme or scheme.lower() != scheme or not _SCHEME_RE.fullmatch(scheme):
            raise ValueError(f"invalid scheme {scheme!r}")
        self._handlers[scheme] = handler

    async def open(self, raw: str) -> Stream:
        identifier = parse(raw)
        try:
            handler = self._handler(identifier.scheme)
            stream = await maybe_await(handler.open(identifier))
        except Exception as exc:
            raise OpenError(identifier, exc) from exc
        if stream is None or stream.body is None:
            raise OpenError(identifier, ValueError("handler returned empty stream"))
        return stream

    async def download(self, raw: str, dst_path: str) -> None:
        if not dst_path:
            raise DownloadError(dst_path, ValueError("dst_path is empty"))
        try:
            stream = await self.open(raw)
            parent = os.path.dirname(dst_path) or "."
            if not os.path.isdir(parent):
                raise ValueError("destination parent is not a directory")
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".{os.path.basename(dst_path)}.tmp-", dir=parent
            )
            os.close(fd)
            ok = False
            try:
                async with aiofiles.open(tmp_path, "wb") as tmp:
                    while chunk := await stream.body.read(1024 * 64):
                        await tmp.write(chunk)
                if (
                    stream.size >= 0
                    and self.opts.max_bytes > 0
                    and stream.size > self.opts.max_bytes
                ):
                    raise SizeLimitExceededError("resource: size limit exceeded")
                os.replace(tmp_path, dst_path)
                ok = True
            finally:
                await stream.close()
                if not ok:
                    with contextlib_suppress_file_not_found():
                        os.remove(tmp_path)
        except DownloadError:
            raise
        except Exception as exc:
            raise DownloadError(dst_path, exc) from exc

    async def upload(self, scheme: str, value: UploadInput) -> Identifier:
        if not scheme or scheme.lower() != scheme or not _SCHEME_RE.fullmatch(scheme):
            raise UploadError(scheme, ValueError(f"invalid scheme {scheme!r}"))
        try:
            handler = self._handler(scheme)
            return await maybe_await(handler.upload(value))
        except Exception as exc:
            raise UploadError(scheme, exc) from exc

    def _handler(self, scheme: str) -> ResourceHandler:
        handler = self._handlers.get(scheme)
        if handler is None:
            raise UnsupportedSchemeError("resource: unsupported scheme")
        return handler


class contextlib_suppress_file_not_found:
    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: object,
    ) -> bool:
        return isinstance(exc, FileNotFoundError)


def parse(raw: str) -> Identifier:
    if not raw:
        raise _parse_error(raw, "empty identifier")
    if not raw.startswith("ofa-res"):
        raise _parse_error(raw, "identifier must start with ofa-res")
    meta, found, source_uri = raw.partition("#")
    if not found:
        raise _parse_error(raw, "missing source_uri separator")
    if not source_uri:
        raise _parse_error(raw, "empty source_uri")
    if meta != "ofa-res" and not meta.startswith("ofa-res?"):
        raise _parse_error(raw, "invalid metadata prefix")
    params = _parse_params(meta, raw)
    try:
        scheme = _source_scheme(source_uri)
        _validate_identifier_params(params)
    except Exception as exc:
        raise ParseError(raw, exc) from exc
    return Identifier(
        raw=raw,
        params=params,
        source_uri=source_uri,
        scheme=scheme,
        auth_id=params.get("auth_id", ""),
        media_type=params.get("media_type", ""),
    )


def _parse_params(meta: str, raw: str) -> dict[str, str]:
    if meta == "ofa-res":
        return {}
    query = meta.removeprefix("ofa-res?")
    try:
        values = urllib.parse.parse_qs(query, keep_blank_values=True, strict_parsing=False)
    except ValueError as exc:
        raise ParseError(raw, ValueError(f"parse params: {exc}")) from exc
    params: dict[str, str] = {}
    for key, vals in values.items():
        if not _PARAM_NAME_RE.fullmatch(key):
            raise ParseError(raw, ValueError(f"invalid param name {key!r}"))
        if len(vals) != 1:
            raise ParseError(raw, ValueError(f"duplicate param {key!r}"))
        params[key] = vals[0]
    return params


def _source_scheme(source_uri: str) -> str:
    scheme, found, _ = source_uri.partition(":")
    if not found or not scheme:
        raise ValueError("source_uri scheme is required")
    if not _SCHEME_RE.fullmatch(scheme):
        raise ValueError(f"invalid source_uri scheme {scheme!r}")
    if scheme.lower() != scheme:
        raise ValueError("source_uri scheme must be lowercase")
    return scheme


def _validate_identifier_params(params: Mapping[str, str]) -> None:
    filename = params.get("filename", "")
    if not filename:
        return
    if "/" in filename or "\\" in filename or "\x00" in filename or ".." in filename:
        raise ValueError("invalid filename")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in filename):
        raise ValueError("invalid filename")


def _parse_error(raw: str, msg: str) -> ParseError:
    return ParseError(raw, ValueError(msg))


class _DataHandler:
    def __init__(self, options: Options) -> None:
        self.max_bytes = options.data_max_bytes

    async def open(self, identifier: Identifier) -> Stream:
        media_type, body = _parse_data_url(identifier.source_uri)
        if (
            identifier.media_type
            and media_type
            and not _same_media_type(identifier.media_type, media_type)
        ):
            raise ValueError(
                f"data media type {media_type!r} does not match identifier media_type {identifier.media_type!r}"
            )
        if self.max_bytes > 0 and len(body) > self.max_bytes:
            raise SizeLimitExceededError("resource: size limit exceeded")
        if identifier.media_type:
            media_type = identifier.media_type
        return Stream(
            body=BytesIO(body),
            media_type=media_type,
            size=len(body),
            source_uri=identifier.source_uri,
        )

    def upload(self, _value: UploadInput) -> Identifier:
        raise UploadUnsupportedError("resource: upload unsupported")


def _parse_data_url(raw: str) -> tuple[str, bytes]:
    if not raw.startswith("data:"):
        raise ValueError("invalid data URL")
    meta, found, payload = raw[len("data:") :].partition(",")
    if not found:
        raise ValueError("invalid data URL payload")
    parts = meta.split(";")
    media_type = parts[0] or "text/plain;charset=US-ASCII"
    is_base64 = any(part.lower() == "base64" for part in parts[1:])
    if is_base64:
        return media_type, base64.b64decode(payload)
    return media_type, urllib.parse.unquote_to_bytes(payload)


class _HTTPHandler:
    def __init__(self, options: Options) -> None:
        self.max_bytes = options.max_bytes
        self.timeout_quota = options.timeout_quota
        self.redirect_limit = options.redirect_limit
        self.retry_attempts = options.retry_attempts
        self.retry_base_delay = options.retry_base_delay
        self.retry_max_delay = options.retry_max_delay
        self._opener = options.http_client

    async def open(self, identifier: Identifier) -> Stream:
        ops = [
            httpx.timeout_quota(self.timeout_quota),
            httpx.expected_status_codes(list(_success_status_codes())),
            httpx.max_redirects(self.redirect_limit),
            httpx.retry(
                httpx.RetryOpt(
                    attempts=self.retry_attempts,
                    base_delay=self.retry_base_delay,
                    max_delay=self.retry_max_delay,
                )
            ),
        ]
        if self._opener is not None:
            ops.append(httpx.url_opener(self._opener))
        resp = await httpx.get(identifier.source_uri, *ops).do_stream()

        headers = dict(resp.headers)
        content_length = str(headers.get("Content-Length", ""))
        size = _parse_content_length(content_length)
        if self.max_bytes > 0 and size > self.max_bytes:
            await resp.body.aclose()
            raise SizeLimitExceededError("resource: size limit exceeded")
        media_type = str(headers.get("Content-Type", "")) or identifier.media_type
        filename = identifier.params.get("filename", "") or _filename_from_disposition(
            str(headers.get("Content-Disposition", ""))
        )
        body: AsyncReadable = resp.body
        if self.max_bytes > 0:
            body = _LimitReadCloser(resp.body, self.max_bytes)
        return Stream(
            body=body,
            media_type=media_type,
            filename=filename,
            size=size,
            source_uri=identifier.source_uri,
            headers=headers,
        )

    def upload(self, _value: UploadInput) -> Identifier:
        raise UploadUnsupportedError("resource: upload unsupported")


class _LimitReadCloser:
    def __init__(self, body: AsyncReadable, max_bytes: int) -> None:
        self.body = body
        self.max_bytes = max_bytes
        self.read_bytes = 0
        self.over = False

    async def read(self, size: int = -1) -> bytes:
        if self.over:
            raise SizeLimitExceededError("resource: size limit exceeded")
        remaining = self.max_bytes - self.read_bytes
        if remaining <= 0:
            self.over = True
            raise SizeLimitExceededError("resource: size limit exceeded")
        if size < 0:
            data = await self.body.read(remaining + 1)
            self.read_bytes += len(data)
            if self.read_bytes > self.max_bytes:
                self.over = True
                raise SizeLimitExceededError("resource: size limit exceeded")
            return data
        read_limit = remaining + 1
        if size < read_limit:
            read_limit = size
        data = await self.body.read(read_limit)
        self.read_bytes += len(data)
        if self.read_bytes > self.max_bytes:
            self.over = True
            self.read_bytes = self.max_bytes
            raise SizeLimitExceededError("resource: size limit exceeded")
        return data

    async def aclose(self) -> None:
        await self.body.aclose()


def _parse_content_length(raw: str) -> int:
    if not raw:
        return -1
    try:
        return int(raw)
    except ValueError:
        return -1


def _filename_from_disposition(disposition: str) -> str:
    if not disposition:
        return ""
    msg = Message()
    msg["Content-Disposition"] = disposition
    filename = msg.get_filename() or ""
    if not filename:
        return ""
    if "/" in filename or "\\" in filename or "\x00" in filename or ".." in filename:
        return ""
    return filename


def _same_media_type(a: str, b: str) -> bool:
    return a.split(";", 1)[0].strip().lower() == b.split(";", 1)[0].strip().lower()


def _success_status_codes() -> set[int]:
    return {200, 201, 202, 203, 204, 205, 206, 207, 208, 226}
