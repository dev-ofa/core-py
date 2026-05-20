"""HTTP client with trace propagation, retry and pluggable service discovery."""

from __future__ import annotations

import contextlib
import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import IO, Any, Protocol

from core_py import context, logging, trace

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


class TimeoutBudgetExhaustedError(Exception):
    pass


class NoHealthyInstanceError(Exception):
    pass


class ServiceDiscoveryDisabledError(Exception):
    pass


ERR_TIMEOUT_BUDGET_EXHAUSTED = TimeoutBudgetExhaustedError("httpx: timeout budget exhausted")
ERR_NO_HEALTHY_INSTANCE = NoHealthyInstanceError("httpx: no healthy service instance")
ERR_SERVICE_DISCOVERY_DISABLED = ServiceDiscoveryDisabledError(
    "httpx: service discovery is disabled"
)


@dataclass(slots=True)
class HTTPStatusError(Exception):
    status_code: int
    expected_status_codes: list[int]
    body: bytes = b""
    read_body_err: Exception | None = None

    def __str__(self) -> str:
        if self.read_body_err:
            return f"http status {self.status_code} is not expected({self.expected_status_codes}), read body failed: {self.read_body_err}"
        return f"http status {self.status_code} is not expected({self.expected_status_codes}), body: {self.body.decode(errors='replace')}"


@dataclass(slots=True)
class CallError(Exception):
    method: str
    url: str
    request_id: str
    err: Exception
    status_code: int = 0

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
    body: IO[bytes]
    url: str


class Wrapper(Protocol):
    def set_data(self, ret: Any) -> None: ...
    def validate(self) -> None: ...


class RespHandler(Protocol):
    def handle_response(self, resp: Response, resp_wrapper: Wrapper | None = None) -> None: ...


AgentOp = Callable[["Agent"], None]
ReqPreHandler = Callable[["PreparedRequest"], "PreparedRequest"]


@dataclass(slots=True)
class RetryOpt:
    max_delay: float = DEFAULT_RETRY_MAX_DELAY
    base_delay: float = DEFAULT_RETRY_BASE
    retry_app_error: bool = False
    attempts: int = DEFAULT_RETRY_ATTEMPTS
    idempotent: bool = False


@dataclass(slots=True)
class PreparedRequest:
    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None
    timeout: float | None = None


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
    def resolve(self, ctx: context.Context | None, req: ResolveRequest) -> ResolveResponse: ...


class InstancePicker(Protocol):
    def pick(
        self, ctx: context.Context | None, req: ResolveRequest, resp: ResolveResponse
    ) -> Instance: ...


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
    def pick(
        self, ctx: context.Context | None, req: ResolveRequest, resp: ResolveResponse
    ) -> Instance:
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

    def handle_response(self, resp: Response, resp_wrapper: Wrapper | None = None) -> None:
        for idx, pred in enumerate(self.predicates):
            if pred.predicate(resp):
                if pred.resp_handler is None:
                    raise ValueError(f"hybrid resp handler is nil at {idx}")
                pred.resp_handler.handle_response(resp, resp_wrapper)
                return

    def __call__(self, agent: Agent) -> None:
        agent.resp_handler = self


class Agent:
    def __init__(self, url: str, method: str, *ops: AgentOp) -> None:
        self.url = url
        self.method = method.upper()
        self.ctx: context.Context | None = None
        self.req_pre_handlers: list[ReqPreHandler] = []
        self.resp_handler: RespHandler | None = None
        self.resp_wrapper: Wrapper | None = None
        self.expected_status_codes: list[int] = []
        self.retry_opt: RetryOpt | None = None
        self.timeout_quota = DEFAULT_TIMEOUT_QUOTA
        self.service = ServiceOptions()
        self._ops = list(ops)
        self._deadline: float | None = None
        self._opener: Any | None = None

    def ops(self, *ops: AgentOp) -> Agent:
        self._ops.extend(ops)
        return self

    def init(self) -> None:
        for op in self._ops:
            op(self)
        if not self.expected_status_codes:
            self.expected_status_codes = [200]
        self.ctx, _ = ensure_trace_context(self.ctx)
        if self._deadline is None:
            self._deadline = time.time() + self.timeout_quota

    def do(self) -> None:
        self.init()
        if not self.retry_opt:
            self._do_http()
            return
        attempts = self.retry_opt.attempts or DEFAULT_RETRY_ATTEMPTS
        if not self._can_retry_method():
            attempts = 1
        last_err: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                self._do_http()
                return
            except Exception as exc:
                last_err = exc
                if attempt == attempts or not self._should_retry(exc):
                    raise
                delay = self._retry_delay(attempt)
                if self._deadline and time.time() + delay >= self._deadline:
                    raise
                time.sleep(delay)
        if last_err:
            raise last_err

    def do_stream(self) -> StreamResponse:
        self.init()
        if not self.retry_opt:
            return self._do_http_stream()
        attempts = self.retry_opt.attempts or DEFAULT_RETRY_ATTEMPTS
        if not self._can_retry_method():
            attempts = 1
        last_err: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return self._do_http_stream()
            except Exception as exc:
                last_err = exc
                if attempt == attempts or not self._should_retry(exc):
                    raise
                delay = self._retry_delay(attempt)
                if self._deadline and time.time() + delay >= self._deadline:
                    raise
                time.sleep(delay)
        if last_err:
            raise last_err
        raise RuntimeError("httpx stream call failed")

    def _prepare_request(self) -> tuple[PreparedRequest, str]:
        if self._deadline and self._deadline <= time.time():
            raise TimeoutBudgetExhaustedError("httpx: timeout budget exhausted")
        req = PreparedRequest(self.method, self.url)
        for handler in self.req_pre_handlers:
            req = handler(req)
        self.ctx, request_id = inject_trace_headers(self.ctx, req.headers, self._deadline)
        if self.service.enable_discovery:
            trace_id = req.headers.get(HEADER_TRACE_ID, "")
            req.url = resolve_url(self.ctx, req.url, self.service, trace_id, request_id)
        if self._deadline:
            req.timeout = max(0.001, self._deadline - time.time())
        return req, request_id

    def _do_http(self) -> None:
        req, request_id = self._prepare_request()
        start = time.time()
        logging.ctx_infof(
            self.ctx,
            "httpx request start method=%s path=%s",
            req.method,
            urllib.parse.urlparse(req.url).path,
        )
        try:
            request = urllib.request.Request(
                req.url, data=req.body, headers=req.headers, method=req.method
            )
            response_ctx = self._open_request(request, req.timeout)
            with response_ctx as response:  # noqa: S310 internal client wrapper
                body = response.read()
                resp = Response(response.status, dict(response.headers), body, response.url)
        except urllib.error.HTTPError as exc:
            body = exc.read()
            resp = Response(exc.code, dict(exc.headers), body, exc.url)
        except Exception as exc:
            err = CallError(req.method, req.url, request_id, exc)
            self._log_end(req, 0, start, err)
            raise err from exc
        if resp.status_code not in self.expected_status_codes:
            status_err = HTTPStatusError(resp.status_code, self.expected_status_codes, resp.body)
            err = CallError(req.method, req.url, request_id, status_err, resp.status_code)
            self._log_end(req, resp.status_code, start, err)
            raise err from status_err
        if self.resp_handler:
            try:
                self.resp_handler.handle_response(resp, self.resp_wrapper)
            except Exception as exc:
                err = CallError(req.method, req.url, request_id, exc, resp.status_code)
                self._log_end(req, resp.status_code, start, err)
                raise err from exc
        self._log_end(req, resp.status_code, start, None)

    def _log_end(
        self, req: PreparedRequest, status_code: int, start: float, err: Exception | None
    ) -> None:
        duration_ms = int((time.time() - start) * 1000)
        path = urllib.parse.urlparse(req.url).path
        if err is None:
            logging.ctx_infof(
                self.ctx,
                "httpx request end method=%s path=%s status_code=%d duration_ms=%d",
                req.method,
                path,
                status_code,
                duration_ms,
            )
        else:
            logging.ctx_errorf(
                self.ctx,
                "httpx request end method=%s path=%s status_code=%d duration_ms=%d error=%s",
                req.method,
                path,
                status_code,
                duration_ms,
                err,
            )

    def _do_http_stream(self) -> StreamResponse:
        req, request_id = self._prepare_request()
        start = time.time()
        logging.ctx_infof(
            self.ctx,
            "httpx request start method=%s path=%s",
            req.method,
            urllib.parse.urlparse(req.url).path,
        )
        try:
            request = urllib.request.Request(
                req.url, data=req.body, headers=req.headers, method=req.method
            )
            response = self._open_request(request, req.timeout)
        except urllib.error.HTTPError as exc:
            if exc.code in self.expected_status_codes:
                self._log_end(req, exc.code, start, None)
                return StreamResponse(exc.code, dict(exc.headers), exc, exc.url)
            body = exc.read()
            exc.close()
            status_err = HTTPStatusError(exc.code, self.expected_status_codes, body)
            err = CallError(req.method, req.url, request_id, status_err, exc.code)
            self._log_end(req, exc.code, start, err)
            raise err from status_err
        except Exception as exc:
            err = CallError(req.method, req.url, request_id, exc)
            self._log_end(req, 0, start, err)
            raise err from exc
        status_code = getattr(response, "status", 200)
        if status_code not in self.expected_status_codes:
            body = response.read()
            response.close()
            status_err = HTTPStatusError(status_code, self.expected_status_codes, body)
            err = CallError(req.method, req.url, request_id, status_err, status_code)
            self._log_end(req, status_code, start, err)
            raise err from status_err
        self._log_end(req, status_code, start, None)
        return StreamResponse(status_code, dict(response.headers), response, response.url)

    def _open_request(self, request: urllib.request.Request, timeout: float | None) -> Any:
        if self._opener is not None:
            return self._opener.open(request, timeout=timeout)
        return urllib.request.urlopen(request, timeout=timeout)  # noqa: S310 internal client wrapper

    def _can_retry_method(self) -> bool:
        return bool(self.retry_opt and self.retry_opt.idempotent) or self.method in {
            "GET",
            "HEAD",
            "OPTIONS",
        }

    def _should_retry(self, err: Exception) -> bool:
        if isinstance(err, CallError) and isinstance(err.err, HTTPStatusError):
            return err.err.status_code >= 500
        return not isinstance(err, CallError) or not isinstance(err.err, HTTPStatusError)

    def _retry_delay(self, attempt: int) -> float:
        opt = self.retry_opt or RetryOpt()
        delay = min(
            opt.max_delay or DEFAULT_RETRY_MAX_DELAY,
            (opt.base_delay or DEFAULT_RETRY_BASE) * (2 ** (attempt - 1)),
        )
        return float(delay / 2 + random.random() * delay / 2)


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
        raise ValueError("unsupported response target")


def ensure_trace_context(ctx: context.Context | None) -> tuple[dict[str, str], str]:
    ret = dict(ctx or {})
    trace_id, ok = context.ctx_get_trace_id(ret)
    if ok:
        return ret, trace_id
    trace_id = trace.new_trace_id()
    return context.ctx_set_trace_id(ret, trace_id), trace_id


def inject_trace_headers(
    ctx: context.Context | None, headers: dict[str, str], deadline: float | None = None
) -> tuple[dict[str, str], str]:
    ctx, trace_id = ensure_trace_context(ctx)
    request_id = trace.new_request_id()
    ctx = context.ctx_set_request_id(ctx, request_id)
    headers.update(context.ctx_pass_headers(ctx))
    headers[HEADER_TRACE_ID] = trace_id
    headers[HEADER_REQUEST_ID] = request_id
    if deadline is not None:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise TimeoutBudgetExhaustedError("httpx: timeout budget exhausted")
        remaining_ms = str(int(remaining * 1000))
        ctx = context.ctx_set_remaining_timeout_ms(ctx, remaining_ms)
        headers[HEADER_REMAINING_TIMEOUT_MS] = remaining_ms
    return ctx, request_id


def context_from_headers(
    headers: Mapping[str, str],
    default_timeout: float = DEFAULT_TIMEOUT_QUOTA,
    max_timeout: float = 0.0,
    ctx: context.Context | None = None,
) -> tuple[dict[str, str], float | None]:
    ret = dict(ctx or {})
    for key, val in headers.items():
        if key.upper().startswith("OFA_PASS_"):
            ret = context.ctx_set_pass_val(ret, key, val)
    if request_id := headers.get(HEADER_REQUEST_ID):
        ret = context.ctx_set_request_id(ret, request_id)
    timeout = default_timeout
    if raw := headers.get(HEADER_REMAINING_TIMEOUT_MS):
        with contextlib.suppress(ValueError):
            timeout = int(raw) / 1000
    if max_timeout > 0 and (timeout == 0 or timeout > max_timeout):
        timeout = max_timeout
    if timeout > 0:
        ret = context.ctx_set_remaining_timeout_ms(ret, str(int(timeout * 1000)))
        return ret, time.time() + timeout
    return ret, None


def resolve_url(
    ctx: context.Context | None, original: str, opt: ServiceOptions, trace_id: str, request_id: str
) -> str:
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
        raise ValueError("service discovery requires service name and namespace")
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
    resp = opt.resolver.resolve(ctx, req)
    picker = opt.picker or RandomPicker()
    return _rewrite_url_to_instance(original, picker.pick(ctx, req, resp))


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


def retry(opt: RetryOpt | None = None) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.retry_opt = opt or RetryOpt()

    return op


def expected_status_codes(codes: list[int]) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.expected_status_codes = list(codes)

    return op


def Context(ctx: context.Context | None) -> AgentOp:  # noqa: N802 compatibility
    def op(agent: Agent) -> None:
        agent.ctx = ctx

    return op


def timeout_quota(seconds: float) -> AgentOp:
    def op(agent: Agent) -> None:
        agent.timeout_quota = seconds

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


# Go-style aliases.
ErrTimeoutBudgetExhausted = ERR_TIMEOUT_BUDGET_EXHAUSTED
ErrNoHealthyInstance = ERR_NO_HEALTHY_INSTANCE
ErrServiceDiscoveryDisabled = ERR_SERVICE_DISCOVERY_DISABLED
Retry = retry
ExpectedStatusCodes = expected_status_codes
TimeoutQuota = timeout_quota
Service = service
SetHeader = set_header
URLOpener = url_opener
RespWrapper = resp_wrapper
CustomRespHandler = custom_resp_handler
TextReq = text_req
JSONReq = json_req
JsonReq = json_req
RawReq = raw_req
FormReq = form_req
JSONResp = json_resp
JsonResp = json_resp
RawResp = raw_resp
HybridResp = hybrid_resp
Get = get
Post = post
Put = put
Patch = patch
Delete = delete
ContextFromHeaders = context_from_headers
