"""HTTP client with trace propagation, retry and pluggable service discovery."""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from email.message import Message
from typing import Any, Protocol, TypeAlias

import httpx as _httpx_lib

from core_py import context, data, logging, trace
from core_py._async import AsyncReadable, ensure_async_readable, maybe_await

HEADER_TRACE_ID = trace.HEADER_TRACE_ID
HEADER_OPERATOR = trace.HEADER_OPERATOR
HEADER_TENANT_ID = trace.HEADER_TENANT_ID
HEADER_APP_ID = trace.HEADER_APP_ID
HEADER_REQUEST_ID = trace.HEADER_REQUEST_ID
HEADER_REMAINING_TIMEOUT_MS = trace.HEADER_REMAINING_TIMEOUT_MS

DEFAULT_TIMEOUT_QUOTA = 5.0
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BASE = 0.1
DEFAULT_RETRY_MAX_DELAY = 1.0

ERR_CODE_HTTP_TIMEOUT_BUDGET_EXHAUSTED = 10110
ERR_CODE_HTTP_NO_HEALTHY_INSTANCE = 10111
ERR_CODE_HTTP_SERVICE_DISCOVERY_DISABLED = 20110
ERR_CODE_HTTP_WRAPPER_DEFAULT = data.ERR_CODE_EXPECTED


class TimeoutBudgetExhaustedError(data.Error):
    def __init__(self, message: str = "httpx: timeout budget exhausted") -> None:
        super().__init__(ERR_CODE_HTTP_TIMEOUT_BUDGET_EXHAUSTED, message)


class NoHealthyInstanceError(data.Error):
    def __init__(self, message: str = "httpx: no healthy service instance") -> None:
        super().__init__(ERR_CODE_HTTP_NO_HEALTHY_INSTANCE, message)


class ServiceDiscoveryDisabledError(data.Error):
    def __init__(self, message: str = "httpx: service discovery is disabled") -> None:
        super().__init__(ERR_CODE_HTTP_SERVICE_DISCOVERY_DISABLED, message)


ERR_TIMEOUT_BUDGET_EXHAUSTED = TimeoutBudgetExhaustedError("httpx: timeout budget exhausted")
ERR_NO_HEALTHY_INSTANCE = NoHealthyInstanceError("httpx: no healthy service instance")
ERR_SERVICE_DISCOVERY_DISABLED = ServiceDiscoveryDisabledError(
    "httpx: service discovery is disabled"
)


class HTTPStatusError(data.HTTPValidationError):
    __slots__ = ("expected_status_codes", "read_body_err")

    def __init__(
        self,
        status_code: int,
        expected_status_codes: list[int],
        body: bytes = b"",
        read_body_err: Exception | None = None,
    ) -> None:
        super().__init__(status_code, body)
        self.expected_status_codes = expected_status_codes
        self.read_body_err = read_body_err

    def __str__(self) -> str:
        if self.read_body_err:
            return f"http status {self.status_code} is not expected({self.expected_status_codes}), read body failed: {self.read_body_err}"
        return f"http status {self.status_code} is not expected({self.expected_status_codes}), body: {self.body.decode(errors='replace')}"


class CallError(data.UpstreamError):
    __slots__ = ("method", "url", "err", "status_code")

    def __init__(
        self,
        method: str,
        url: str,
        request_id: str,
        err: Exception,
        status_code: int = 0,
    ) -> None:
        super().__init__(url, method, request_id, err)
        self.method = method
        self.url = url
        self.err = _compat_error(err)
        self.status_code = status_code

    def __str__(self) -> str:
        if self.status_code:
            return f"httpx call {self.method} {self.url} request_id={self.request_id} status_code={self.status_code} failed: {self.err}"
        return (
            f"httpx call {self.method} {self.url} request_id={self.request_id} failed: {self.err}"
        )


@dataclass(slots=True)
class Response:
    status_code: int
    headers: Mapping[str, str]
    body: bytes
    url: str

    def json(self) -> Any:
        if not self.body:
            return None
        return json.loads(self.body.decode("utf-8"))


@dataclass(slots=True)
class StreamResponse:
    status_code: int
    headers: Mapping[str, str]
    body: AsyncReadable | Any
    url: str

    def __post_init__(self) -> None:
        self.body = ensure_async_readable(self.body)

    async def close(self) -> None:
        await self.body.aclose()

    async def __aenter__(self) -> StreamResponse:
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> None:
        await self.close()


class Wrapper(Protocol):
    def set_data(self, ret: Any) -> None: ...
    def validate(self) -> None: ...


class WrapperError(data.Error):
    __slots__ = ("request_id", "data")

    def __init__(
        self,
        code: int,
        message: str,
        request_id: str = "",
        data: Any = None,
    ) -> None:
        if not code:
            code = ERR_CODE_HTTP_WRAPPER_DEFAULT
        if not message:
            message = "httpx wrapper validate failed"
        super().__init__(code, message)
        self.request_id = request_id
        self.data = data

    def __str__(self) -> str:
        if self.request_id:
            return f"httpx wrapper validate failed code={self.code} request_id={self.request_id}: {self.message}"
        return f"httpx wrapper validate failed code={self.code}: {self.message}"


@dataclass(slots=True)
class CommonWrapper:
    code: int = 0
    message: str = ""
    request_id: str = ""
    data: Any = None
    allow_codes: list[int] = field(default_factory=list)

    def set_data(self, ret: Any) -> None:
        self.data = ret

    def load(self, payload: Any) -> None:
        if not isinstance(payload, Mapping):
            return
        self.code = int(payload.get("code", 0) or 0)
        self.message = str(payload.get("message", ""))
        self.request_id = str(payload.get("request_id", ""))
        if "data" not in payload:
            return
        data = payload["data"]
        if data is None:
            if self.code != 0:
                self.data = None
            return
        if self.data is None:
            self.data = data
            return
        _assign_payload(self.data, data)

    def validate(self) -> None:
        if self.code == 0 or self.code in self.allow_codes:
            return
        err = WrapperError(self.code, self.message, self.request_id, self.data)
        if self.data is not None:
            with_data = data.with_error_data(err, self.data)
            if with_data is not None:
                raise with_data
        raise err


class RespHandler(Protocol):
    def handle_response(
        self, resp: Response, resp_wrapper: Wrapper | None = None
    ) -> None | Awaitable[None]: ...


AgentOp = Callable[["Agent"], None]


@dataclass(slots=True)
class RetryOpt:
    max_delay: float = DEFAULT_RETRY_MAX_DELAY
    base_delay: float = DEFAULT_RETRY_BASE
    attempts: int = DEFAULT_RETRY_ATTEMPTS
    idempotent: bool = False


@dataclass(slots=True)
class PreparedRequest:
    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None
    timeout: float | None = None
    host: str = ""


ReqPreHandler: TypeAlias = Callable[[PreparedRequest], PreparedRequest | Awaitable[PreparedRequest]]


@dataclass(slots=True)
class Instance:
    instance_id: str = ""
    host: str = ""
    port: int = 0
    scheme: str = ""
    health_status: str = ""
    weight: int = 0
    zone: str = ""
    labels: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ResolveRequest:
    service_name: str
    namespace: str
    label_selector: dict[str, str] = field(default_factory=dict)
    preferred_label_selector: dict[str, str] = field(default_factory=dict)
    preferred_zone: str = ""
    resolve_mode: str = "healthy_only"
    request_id: str = ""
    trace_id: str = ""


@dataclass(slots=True)
class ResolveResponse:
    service_name: str
    namespace: str
    instances: list[Instance]
    resolve_time: float = field(default_factory=time.time)
    version: str = ""
    cache_ttl: float = 0.0
    partial: bool = False
    warnings: list[str] = field(default_factory=list)


class Resolver(Protocol):
    def resolve(self, req: ResolveRequest) -> ResolveResponse | Awaitable[ResolveResponse]: ...


ResolverCallable = Callable[[ResolveRequest], ResolveResponse]


@dataclass(slots=True)
class ResolverFunc:
    fn: ResolverCallable

    def resolve(self, req: ResolveRequest) -> ResolveResponse:
        return self.fn(req)


class InstancePicker(Protocol):
    def pick(
        self, req: ResolveRequest, resp: ResolveResponse
    ) -> Instance | Awaitable[Instance]: ...


InstancePickerCallable = Callable[[ResolveRequest, ResolveResponse], Instance]


@dataclass(slots=True)
class InstancePickerFunc:
    fn: InstancePickerCallable

    def pick(self, req: ResolveRequest, resp: ResolveResponse) -> Instance:
        return self.fn(req, resp)


@dataclass(slots=True)
class ServiceOptions:
    enable_discovery: bool = False
    service_name: str = ""
    namespace: str = ""
    preferred_zone: str = ""
    label_selector: dict[str, str] = field(default_factory=dict)
    preferred_label_selector: dict[str, str] = field(default_factory=dict)
    resolve_mode: str = "healthy_only"
    resolver: Resolver | None = None
    picker: InstancePicker | None = None
    instance_override: Instance | None = None


class RandomPicker:
    def pick(self, req: ResolveRequest, resp: ResolveResponse) -> Instance:
        candidates: list[Instance] = []
        for inst in resp.instances if resp else []:
            if req.resolve_mode != "all" and inst.health_status and inst.health_status != "healthy":
                continue
            if req.preferred_zone and inst.zone and inst.zone != req.preferred_zone:
                continue
            candidates.append(inst)
        if not candidates and req.preferred_zone:
            candidates = [
                inst
                for inst in resp.instances
                if req.resolve_mode == "all"
                or not inst.health_status
                or inst.health_status == "healthy"
            ]
        if not candidates:
            raise NoHealthyInstanceError("httpx: no healthy service instance")
        total = sum(max(0, inst.weight) for inst in candidates)
        if total <= 0:
            return random.choice(candidates)
        pick = random.randrange(total)
        for inst in candidates:
            if inst.weight <= 0:
                continue
            pick -= inst.weight
            if pick < 0:
                return inst
        return candidates[-1]


class JSONRespHandler:
    def __init__(self, ret: Any) -> None:
        self.ret = ret

    def handle_response(self, resp: Response, resp_wrapper: Wrapper | None = None) -> None:
        payload = resp.json()
        if resp_wrapper is not None:
            resp_wrapper.set_data(self.ret)
            if hasattr(resp_wrapper, "load"):
                resp_wrapper.load(payload)
            elif isinstance(payload, Mapping):
                for k, v in payload.items():
                    setattr(resp_wrapper, k, v)
            resp_wrapper.validate()
            return
        _assign_payload(self.ret, payload)

    def __call__(self, agent: Agent) -> None:
        agent.resp_handler = self


class RawRespHandler:
    def __init__(self, target: dict[str, Any] | bytearray | None = None) -> None:
        self.target = target

    def handle_response(self, resp: Response, resp_wrapper: Wrapper | None = None) -> None:
        if isinstance(self.target, bytearray):
            self.target.extend(resp.body)
        elif isinstance(self.target, dict):
            self.target.update(
                {
                    "status_code": resp.status_code,
                    "headers": dict(resp.headers),
                    "body": resp.body,
                    "url": resp.url,
                }
            )

    def __call__(self, agent: Agent) -> None:
        agent.resp_handler = self


@dataclass(slots=True)
class RespHandlerPredicate:
    predicate: Callable[[Response], bool]
    resp_handler: RespHandler


class HybridHandler:
    def __init__(self, predicates: list[RespHandlerPredicate]) -> None:
        self.predicates = predicates

    async def handle_response(self, resp: Response, resp_wrapper: Wrapper | None = None) -> None:
        for idx, pred in enumerate(self.predicates):
            if pred.predicate(resp):
                if pred.resp_handler is None:
                    raise data.new_validation_error(f"hybrid resp handler is nil at {idx}")
                await maybe_await(pred.resp_handler.handle_response(resp, resp_wrapper))
                return

    def __call__(self, agent: Agent) -> None:
        agent.resp_handler = self


class Agent:
    def __init__(self, url: str, method: str, *ops: AgentOp) -> None:
        self.url = url
        self.method = method.upper()
        self.req_pre_handlers: list[ReqPreHandler] = []
        self.resp_handler: RespHandler | None = None
        self.resp_wrapper: Wrapper | None = None
        self.expected_status_codes: list[int] = []
        self.retry_status_codes: list[int] = []
        self.retry_opt: RetryOpt | None = None
        self.timeout_quota = DEFAULT_TIMEOUT_QUOTA
        self.redirect_limit: int | None = None
        self.service = ServiceOptions()
        self._ops = list(ops)
        self._deadline: float | None = None
        self._opener: Any | None = None

    def ops(self, *ops: AgentOp) -> Agent:
        self._ops.extend(ops)
        return self

    async def init(self) -> None:
        for op in self._ops:
            op(self)
        if not self.expected_status_codes:
            self.expected_status_codes = [200]
        ensure_trace_context()
        if self._deadline is None:
            self._deadline = self._init_deadline()

    async def do(self) -> None:
        try:
            await self.init()
        except Exception as exc:
            raise CallError(self.method, self.url, "", exc) from exc
        if not self.retry_opt:
            await self._do_http()
            return
        attempts = self._retry_attempts()
        if not self._can_retry_method():
            attempts = 1
        last_err: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                await self._do_http()
                return
            except Exception as exc:
                last_err = exc
                if attempt == attempts or not self._should_retry(exc):
                    raise
                delay = self._retry_delay(attempt)
                if self._deadline and time.time() + delay >= self._deadline:
                    raise
                await asyncio.sleep(delay)
        if last_err:
            raise last_err

    async def do_stream(self) -> StreamResponse:
        try:
            await self.init()
        except Exception as exc:
            raise CallError(self.method, self.url, "", exc) from exc
        if not self.retry_opt:
            return await self._do_http_stream()
        attempts = self._retry_attempts()
        if not self._can_retry_method():
            attempts = 1
        last_err: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await self._do_http_stream()
            except Exception as exc:
                last_err = exc
                if attempt == attempts or not self._should_retry(exc):
                    raise
                delay = self._retry_delay(attempt)
                if self._deadline and time.time() + delay >= self._deadline:
                    raise
                await asyncio.sleep(delay)
        if last_err:
            raise last_err
        raise CallError(self.method, self.url, "", RuntimeError("httpx stream call failed"))

    def _retry_attempts(self) -> int:
        if self.retry_opt is None or self.retry_opt.attempts <= 0:
            return DEFAULT_RETRY_ATTEMPTS
        return self.retry_opt.attempts

    async def _prepare_request(self) -> tuple[PreparedRequest, str]:
        if self._deadline and self._deadline <= time.time():
            raise TimeoutBudgetExhaustedError("httpx: timeout budget exhausted")
        req = PreparedRequest(self.method, self.url)
        for handler in self.req_pre_handlers:
            req = await maybe_await(handler(req))
        request_id = inject_trace_headers(req.headers, self._deadline)
        if self.service.enable_discovery:
            trace_id = req.headers.get(HEADER_TRACE_ID, "")
            req.host = _request_host(req.url)
            req.url = await resolve_url(req.url, self.service, trace_id, request_id)
            if req.host:
                req.headers["Host"] = req.host
        if self._deadline:
            req.timeout = max(0.001, self._deadline - time.time())
        return req, request_id

    async def _do_http(self) -> None:
        with context.use_context():
            req: PreparedRequest | None = None
            request_id = ""
            start = time.time()
            try:
                req, request_id = await self._prepare_request()
                logging.info(
                    "httpx request start method=%s path=%s",
                    req.method,
                    urllib.parse.urlparse(req.url).path,
                )
                resp = await self._send_request(req)
                if resp.status_code not in self.expected_status_codes:
                    status_err = HTTPStatusError(
                        resp.status_code, self.expected_status_codes, resp.body
                    )
                    if resp.status_code in self.retry_status_codes:
                        retryable = data.with_retryable_error(status_err)
                        if retryable is not None:
                            raise retryable from status_err
                    raise status_err
                if self.resp_handler:
                    await maybe_await(self.resp_handler.handle_response(resp, self.resp_wrapper))
                self._log_end(req, resp.status_code, start, None)
            except Exception as exc:
                err: Exception = exc
                if _is_retryable_network_error(exc):
                    retryable = data.with_retryable_error(exc)
                    if retryable is not None:
                        err = retryable
                status_code = getattr(resp, "status_code", 0) if "resp" in locals() else 0
                method = req.method if req is not None else self.method
                url = req.url if req is not None else self.url
                wrapped = CallError(
                    method,
                    url,
                    request_id,
                    err,
                    status_code,
                )
                if req is not None:
                    self._log_end(req, status_code, start, wrapped)
                raise wrapped from exc

    def _log_end(
        self, req: PreparedRequest, status_code: int, start: float, err: Exception | None
    ) -> None:
        duration_ms = int((time.time() - start) * 1000)
        path = urllib.parse.urlparse(req.url).path
        if err is None:
            logging.info(
                "httpx request end method=%s path=%s status_code=%d duration_ms=%d",
                req.method,
                path,
                status_code,
                duration_ms,
            )
            return
        log = logging.warning if data.is_expected(err) else logging.error
        log(
            "httpx request end method=%s path=%s status_code=%d duration_ms=%d error=%s",
            req.method,
            path,
            status_code,
            duration_ms,
            err,
        )

    async def _send_request(self, req: PreparedRequest) -> Response:
        if self._opener is not None:
            request = _build_request(req)
            try:
                response = await maybe_await(self._opener.open(request, timeout=req.timeout))
                body = await _read_response_body(response)
                return Response(
                    getattr(response, "status", 200),
                    _header_dict(getattr(response, "headers", {})),
                    body,
                    getattr(response, "url", req.url),
                )
            except urllib.error.HTTPError as exc:
                body = await maybe_await(exc.read())
                return Response(exc.code, _header_dict(exc.headers), body, exc.url)

        async with _httpx_lib.AsyncClient(
            follow_redirects=True,
            max_redirects=self.redirect_limit if self.redirect_limit is not None else 20,
            timeout=req.timeout,
        ) as client:
            response = await client.request(
                req.method,
                req.url,
                headers=req.headers,
                content=req.body,
            )
            return Response(
                response.status_code,
                _header_dict(response.headers),
                response.content,
                str(response.url),
            )

    async def _open_stream(self, req: PreparedRequest) -> StreamResponse:
        if self._opener is not None:
            request = _build_request(req)
            try:
                response = await maybe_await(self._opener.open(request, timeout=req.timeout))
                return StreamResponse(
                    getattr(response, "status", 200),
                    _header_dict(getattr(response, "headers", {})),
                    ensure_async_readable(response),
                    getattr(response, "url", req.url),
                )
            except urllib.error.HTTPError as exc:
                return StreamResponse(
                    exc.code,
                    _header_dict(exc.headers),
                    ensure_async_readable(exc),
                    exc.url,
                )

        client = _httpx_lib.AsyncClient(
            follow_redirects=True,
            max_redirects=self.redirect_limit if self.redirect_limit is not None else 20,
            timeout=req.timeout,
        )
        response = await client.send(
            client.build_request(
                req.method,
                req.url,
                headers=req.headers,
                content=req.body,
            ),
            stream=True,
        )
        return StreamResponse(
            response.status_code,
            _header_dict(response.headers),
            _HTTPXStreamBody(response, client),
            str(response.url),
        )

    async def _do_http_stream(self) -> StreamResponse:
        with context.use_context():
            req: PreparedRequest | None = None
            request_id = ""
            start = time.time()
            try:
                req, request_id = await self._prepare_request()
                logging.info(
                    "httpx request start method=%s path=%s",
                    req.method,
                    urllib.parse.urlparse(req.url).path,
                )
                response = await self._open_stream(req)
                status_code = response.status_code
                if status_code not in self.expected_status_codes:
                    body = await response.body.read()
                    await response.close()
                    status_err = HTTPStatusError(status_code, self.expected_status_codes, body)
                    if status_code in self.retry_status_codes:
                        retryable = data.with_retryable_error(status_err)
                        if retryable is not None:
                            raise retryable from status_err
                    raise status_err
                self._log_end(req, status_code, start, None)
                return response
            except Exception as exc:
                err: Exception = exc
                if _is_retryable_network_error(exc):
                    retryable = data.with_retryable_error(exc)
                    if retryable is not None:
                        err = retryable
                status_code = response.status_code if "response" in locals() else 0
                method = req.method if req is not None else self.method
                url = req.url if req is not None else self.url
                wrapped = CallError(method, url, request_id, err, status_code)
                if req is not None:
                    self._log_end(req, status_code, start, wrapped)
                raise wrapped from exc

    def _can_retry_method(self) -> bool:
        return bool(self.retry_opt and self.retry_opt.idempotent) or self.method in {
            "GET",
            "HEAD",
            "OPTIONS",
        }

    def _should_retry(self, err: Exception) -> bool:
        return data.is_retryable_error(err)

    def _retry_delay(self, attempt: int) -> float:
        opt = self.retry_opt or RetryOpt()
        delay = min(
            opt.max_delay or DEFAULT_RETRY_MAX_DELAY,
            (opt.base_delay or DEFAULT_RETRY_BASE) * (2 ** (attempt - 1)),
        )
        return float(delay / 2 + random.random() * delay / 2)

    def _init_deadline(self) -> float:
        now = time.time()
        inherited_deadline = _deadline_from_context(now)
        if inherited_deadline is not None:
            return inherited_deadline
        return now + self.timeout_quota


def _assign_payload(target: Any, payload: Any) -> None:
    if target is None:
        return
    if isinstance(target, dict) and isinstance(payload, Mapping):
        target.update(payload)
    elif isinstance(target, list) and isinstance(payload, list):
        target.extend(payload)
    elif hasattr(target, "__dict__") and isinstance(payload, Mapping):
        for k, v in payload.items():
            setattr(target, k, v)
    else:
        raise data.new_validation_error("unsupported response target")


def _compat_error(err: Exception) -> Exception:
    if isinstance(err, (data.ExtraDataError, data.RetryableError)) and isinstance(
        err.cause, Exception
    ):
        return err.cause
    return err


@dataclass(slots=True)
class _HTTPXStreamBody:
    response: _httpx_lib.Response
    client: _httpx_lib.AsyncClient
    _reader: AsyncReadable = field(init=False)

    def __post_init__(self) -> None:
        self._reader = ensure_async_readable(self.response.aiter_bytes())

    async def read(self, size: int = -1) -> bytes:
        return await self._reader.read(size)

    async def aclose(self) -> None:
        await self.response.aclose()
        await self.client.aclose()


async def _read_response_body(response: Any) -> bytes:
    reader = getattr(response, "read", None)
    if callable(reader):
        return await maybe_await(reader())
    aread = getattr(response, "aread", None)
    if callable(aread):
        return await maybe_await(aread())
    raise TypeError("response body is not readable")


def _header_dict(headers: Mapping[str, str] | Message[str, str]) -> dict[str, str]:
    return {
        "-".join(part.capitalize() for part in key.split("-")): value
        for key, value in headers.items()
    }


def ensure_trace_context() -> str:
    trace_id, ok = context.get_trace_id()
    if ok:
        return trace_id
    trace_id = trace.new_trace_id()
    context.set_trace_id(trace_id)
    return trace_id


def inject_trace_headers(headers: dict[str, str], deadline: float | None = None) -> str:
    trace_id = ensure_trace_context()
    request_id = trace.new_request_id()
    context.set_request_id(request_id)
    headers.update(context.pass_headers())
    headers[HEADER_TRACE_ID] = trace_id
    headers[HEADER_REQUEST_ID] = request_id
    if deadline is not None:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise TimeoutBudgetExhaustedError("httpx: timeout budget exhausted")
        remaining_ms = str(int(remaining * 1000))
        headers[HEADER_REMAINING_TIMEOUT_MS] = remaining_ms
    return request_id


def _deadline_from_context(now: float | None = None) -> float | None:
    deadline, ok = context.get_request_deadline()
    if not ok:
        return None
    base = time.time() if now is None else now
    if deadline <= base:
        raise TimeoutBudgetExhaustedError("httpx: timeout budget exhausted")
    return deadline


def context_from_headers(
    headers: Mapping[str, str],
    default_timeout: float = DEFAULT_TIMEOUT_QUOTA,
    max_timeout: float = 0.0,
) -> float | None:
    current = context.current_context()
    normalized_headers = {key.lower(): val for key, val in headers.items()}
    for key, val in normalized_headers.items():
        if key.startswith("ofa-pass-"):
            current[context.fixed_key(key)] = val
    if request_id := normalized_headers.get(HEADER_REQUEST_ID):
        current[context.fixed_key_direct(HEADER_REQUEST_ID)] = request_id
    context.set_current_context(current)

    timeout = default_timeout
    if raw := normalized_headers.get(HEADER_REMAINING_TIMEOUT_MS):
        with contextlib.suppress(ValueError):
            timeout = int(raw) / 1000
    if max_timeout > 0 and (timeout == 0 or timeout > max_timeout):
        timeout = max_timeout
    if timeout > 0:
        deadline = time.time() + timeout
        context.set_request_deadline(deadline)
        return deadline
    return None


@contextlib.contextmanager
def use_context_from_headers(
    headers: Mapping[str, str],
    default_timeout: float = DEFAULT_TIMEOUT_QUOTA,
    max_timeout: float = 0.0,
) -> Iterator[float | None]:
    with context.use_context():
        yield context_from_headers(headers, default_timeout, max_timeout)


async def resolve_url(original: str, opt: ServiceOptions, trace_id: str, request_id: str) -> str:
    if not opt.enable_discovery:
        return original
    if opt.instance_override:
        return _rewrite_url_to_instance(original, opt.instance_override)
    if opt.resolver is None:
        raise ServiceDiscoveryDisabledError("httpx: service discovery is disabled")
    parsed = urllib.parse.urlparse(original)
    service_name, namespace = opt.service_name, opt.namespace
    if not service_name:
        service_name, namespace = _parse_service_identifier(parsed.hostname or "", namespace)
    if not service_name or not namespace:
        raise data.new_validation_error("service discovery requires service name and namespace")
    req = ResolveRequest(
        service_name,
        namespace,
        opt.label_selector,
        opt.preferred_label_selector,
        opt.preferred_zone,
        opt.resolve_mode or "healthy_only",
        request_id,
        trace_id,
    )
    resp = await maybe_await(opt.resolver.resolve(req))
    picker = opt.picker or RandomPicker()
    inst = await maybe_await(picker.pick(req, resp))
    return _rewrite_url_to_instance(original, inst)


def _parse_service_identifier(host: str, namespace: str) -> tuple[str, str]:
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 2:
        return parts[0], namespace or parts[1]
    return host, namespace


def _rewrite_url_to_instance(original: str, inst: Instance) -> str:
    parsed = urllib.parse.urlparse(original)
    scheme = inst.scheme or parsed.scheme
    host = f"{inst.host}:{inst.port}" if inst.port > 0 else inst.host
    return urllib.parse.urlunparse(
        (scheme, host, parsed.path, parsed.params, parsed.query, parsed.fragment)
    )


def _request_host(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if not parsed.hostname:
        return ""
    if parsed.port:
        return f"{parsed.hostname}:{parsed.port}"
    return parsed.hostname


def _build_request(req: PreparedRequest) -> urllib.request.Request:
    request = urllib.request.Request(req.url, data=req.body, headers=req.headers, method=req.method)
    if req.host:
        request.add_unredirected_header("Host", req.host)
    return request


def _is_retryable_network_error(err: Exception) -> bool:
    current: Exception | None = err
    while current is not None:
        if isinstance(
            current,
            (
                urllib.error.URLError,
                TimeoutError,
                ConnectionError,
                OSError,
                socket.timeout,
                _httpx_lib.TimeoutException,
                _httpx_lib.NetworkError,
                _httpx_lib.ProtocolError,
                _httpx_lib.TransportError,
            ),
        ):
            return True
        current = current.__cause__ if isinstance(current.__cause__, Exception) else None
    return False


def retry(opt: RetryOpt | None = None) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.retry_opt = opt or RetryOpt()

    return op


def expected_status_codes(codes: list[int]) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.expected_status_codes = list(codes)

    return op


def retry_status_codes(codes: list[int]) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.retry_status_codes = list(codes)

    return op


def timeout_quota(seconds: float) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.timeout_quota = seconds

    return op


def max_redirects(limit: int) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.redirect_limit = limit

    return op


def service(opt: ServiceOptions) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.service = opt

    return op


def set_header(headers: Mapping[str, str]) -> AgentOp:
    def op(agent: Agent) -> None:
        def handle(req: PreparedRequest) -> PreparedRequest:
            req.headers.update(headers)
            return req

        agent.req_pre_handlers.append(handle)

    return op


def url_opener(opener: Any) -> AgentOp:
    def op(agent: Agent) -> None:
        agent._opener = opener

    return op


def resp_wrapper(wrapper: Wrapper) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.resp_wrapper = wrapper

    return op


def custom_resp_handler(handler: RespHandler) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.resp_handler = handler

    return op


def text_req(req_body: str) -> AgentOp:
    return raw_req("text/plain; charset=utf-8", req_body.encode())


def json_req(req_body: Any) -> AgentOp:
    return raw_req("application/json; charset=utf-8", json.dumps(req_body).encode())


def raw_req(content_type: str, body: bytes) -> AgentOp:
    def op(agent: Agent) -> None:
        def handle(req: PreparedRequest) -> PreparedRequest:
            if content_type:
                req.headers["Content-Type"] = content_type
            req.body = body
            return req

        agent.req_pre_handlers.append(handle)

    return op


def reader_req(content_type: str, body: Any | None) -> AgentOp:
    body_buf: bytes | None = None

    def op(agent: Agent) -> None:
        async def handle(req: PreparedRequest) -> PreparedRequest:
            nonlocal body_buf
            if body_buf is None:
                body_buf = await maybe_await(body.read()) if body is not None else b""
            if content_type:
                req.headers["Content-Type"] = content_type
            req.body = body_buf
            return req

        agent.req_pre_handlers.append(handle)

    return op


def form_req(values: Mapping[str, str | list[str]]) -> AgentOp:
    def op(agent: Agent) -> None:
        def handle(req: PreparedRequest) -> PreparedRequest:
            pairs: list[tuple[str, str]] = []
            for key, value in values.items():
                if isinstance(value, list):
                    pairs.extend((key, v) for v in value)
                else:
                    pairs.append((key, value))
            encoded = urllib.parse.urlencode(pairs)
            if agent.method in {"GET", "HEAD"}:
                sep = "&" if urllib.parse.urlparse(req.url).query else "?"
                req.url = req.url + sep + encoded
            else:
                req.headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
                req.body = encoded.encode()
            return req

        agent.req_pre_handlers.append(handle)

    return op


def json_resp(ret: Any) -> JSONRespHandler:
    return JSONRespHandler(ret)


def raw_resp(target: dict[str, Any] | bytearray | None = None) -> RawRespHandler:
    return RawRespHandler(target)


def hybrid_resp(*predicates: RespHandlerPredicate) -> HybridHandler:
    return HybridHandler(list(predicates))


def resolver_func(fn: ResolverCallable) -> ResolverFunc:
    return ResolverFunc(fn)


def instance_picker_func(fn: InstancePickerCallable) -> InstancePickerFunc:
    return InstancePickerFunc(fn)


def get(url: str, *ops: AgentOp) -> Agent:
    return Agent(url, "GET", *ops)


def post(url: str, *ops: AgentOp) -> Agent:
    return Agent(url, "POST", *ops)


def put(url: str, *ops: AgentOp) -> Agent:
    return Agent(url, "PUT", *ops)


def patch(url: str, *ops: AgentOp) -> Agent:
    return Agent(url, "PATCH", *ops)


def delete(url: str, *ops: AgentOp) -> Agent:
    return Agent(url, "DELETE", *ops)
