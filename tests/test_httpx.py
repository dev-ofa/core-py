import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from typing import Any

import pytest

from core_py import context, data, httpx


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length).decode()
        self._write_json({"content_type": self.headers.get("Content-Type"), "body": body})

    def do_GET(self) -> None:
        if self.path == "/wrapped-ok":
            self._write_json(
                {"code": 0, "message": "ok", "request_id": "req-1", "data": {"name": "n1"}}
            )
            return
        if self.path == "/wrapped-allowed":
            self._write_json(
                {"code": 10001, "message": "cached", "request_id": "req-2", "data": {"name": "n2"}}
            )
            return
        if self.path == "/wrapped-error":
            self._write_json(
                {"code": 12345, "message": "failed", "request_id": "req-3", "data": {"name": "n3"}}
            )
            return
        if self.path == "/wrapped-error-null-data":
            self._write_json(
                {
                    "code": 10000,
                    "message": "failed with null",
                    "request_id": "req-null",
                    "data": None,
                }
            )
            return
        if self.path == "/wrapped-ok-null-data":
            self._write_json(
                {"code": 0, "message": "ok", "request_id": "req-ok-null", "data": None}
            )
            return
        if self.path == "/wrapped-allowed-null-data":
            self._write_json(
                {"code": 10001, "message": "cached", "request_id": "req-allowed-null", "data": None}
            )
            return
        body = json.dumps(
            {
                "trace_id": self.headers.get(httpx.HEADER_TRACE_ID),
                "operator": self.headers.get(httpx.HEADER_OPERATOR),
                "request_id": self.headers.get(httpx.HEADER_REQUEST_ID),
                "host": self.headers.get("Host"),
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_json(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        return


class RetryableWrapper(httpx.CommonWrapper):
    def validate(self) -> None:
        try:
            super().validate()
        except Exception as exc:
            retryable = data.with_retryable_error(exc)
            if retryable is not None:
                raise retryable from exc
            raise


@pytest.mark.asyncio
async def test_httpx_injects_trace_headers() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        with context.use_context():
            context.set_trace_id("trace-1")
            context.set_operator("user-1")
            await httpx.get(
                f"http://127.0.0.1:{server.server_port}/ping",
                httpx.json_resp(payload),
            ).do()
        assert payload["trace_id"] == "trace-1"
        assert payload["operator"] == "user-1"
        assert payload["request_id"].startswith("req_")
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_common_wrapper_decodes_data_and_allows_codes() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        await httpx.get(
            f"http://127.0.0.1:{server.server_port}/wrapped-ok",
            httpx.json_resp(payload),
            httpx.resp_wrapper(httpx.CommonWrapper()),
        ).do()
        assert payload == {"name": "n1"}

        allowed_payload: dict[str, Any] = {}
        await httpx.get(
            f"http://127.0.0.1:{server.server_port}/wrapped-allowed",
            httpx.json_resp(allowed_payload),
            httpx.resp_wrapper(httpx.CommonWrapper(allow_codes=[10001])),
        ).do()
        assert allowed_payload == {"name": "n2"}
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_common_wrapper_rejects_unexpected_code() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        try:
            await httpx.get(
                f"http://127.0.0.1:{server.server_port}/wrapped-error",
                httpx.json_resp(payload),
                httpx.resp_wrapper(httpx.CommonWrapper()),
            ).do()
        except httpx.CallError as exc:
            assert isinstance(exc.err, httpx.WrapperError)
            assert exc.err.code == 12345
            assert exc.err.message == "failed"
            assert exc.err.request_id == "req-3"
            assert payload == {"name": "n3"}
        else:
            raise AssertionError("expected CallError")
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_common_wrapper_rejects_unexpected_code_with_null_data() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        with pytest.raises(httpx.CallError) as exc_info:
            await httpx.get(
                f"http://127.0.0.1:{server.server_port}/wrapped-error-null-data",
                httpx.json_resp(payload),
                httpx.resp_wrapper(httpx.CommonWrapper()),
            ).do()

        err = exc_info.value.err
        assert isinstance(err, httpx.WrapperError)
        assert err.code == 10000
        assert err.message == "failed with null"
        assert err.request_id == "req-null"
        assert err.data is None
        assert payload == {}
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_common_wrapper_allows_null_data_for_success_and_allowed_codes() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {"existing": "value"}
        await httpx.get(
            f"http://127.0.0.1:{server.server_port}/wrapped-ok-null-data",
            httpx.json_resp(payload),
            httpx.resp_wrapper(httpx.CommonWrapper()),
        ).do()
        assert payload == {"existing": "value"}

        allowed_payload: dict[str, Any] = {"existing": "value"}
        await httpx.get(
            f"http://127.0.0.1:{server.server_port}/wrapped-allowed-null-data",
            httpx.json_resp(allowed_payload),
            httpx.resp_wrapper(httpx.CommonWrapper(allow_codes=[10001])),
        ).do()
        assert allowed_payload == {"existing": "value"}
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_wrapper_business_error_is_not_retried_by_default() -> None:
    attempts = 0

    class RetryHandler(Handler):
        def do_GET(self) -> None:
            nonlocal attempts
            attempts += 1
            self._write_json(
                {
                    "code": 30001,
                    "message": "business failed",
                    "request_id": "req-retry-1",
                    "data": {"name": "ignored"},
                }
            )

    server = HTTPServer(("127.0.0.1", 0), RetryHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        with pytest.raises(httpx.CallError) as exc:
            await httpx.get(
                f"http://127.0.0.1:{server.server_port}/wrapped-error",
                httpx.json_resp(payload),
                httpx.resp_wrapper(httpx.CommonWrapper()),
                httpx.retry(httpx.RetryOpt(attempts=3, base_delay=0.001, max_delay=0.001)),
            ).do()
        assert attempts == 1
        assert isinstance(exc.value.err, httpx.WrapperError)
        assert exc.value.err.code == 30001
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_retryable_wrapper_error_retries_business_failure() -> None:
    attempts = 0

    class RetryHandler(Handler):
        def do_GET(self) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                self._write_json(
                    {
                        "code": 30001,
                        "message": "temporary business failed",
                        "request_id": "req-retry-2",
                        "data": {"name": "bad"},
                    }
                )
                return
            self._write_json(
                {
                    "code": 0,
                    "message": "ok",
                    "request_id": "req-retry-2",
                    "data": {"name": "core-py"},
                }
            )

    server = HTTPServer(("127.0.0.1", 0), RetryHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        await httpx.get(
            f"http://127.0.0.1:{server.server_port}/wrapped-error",
            httpx.json_resp(payload),
            httpx.resp_wrapper(RetryableWrapper()),
            httpx.retry(
                httpx.RetryOpt(
                    attempts=2,
                    base_delay=0.001,
                    max_delay=0.001,
                )
            ),
        ).do()
        assert attempts == 2
        assert payload == {"name": "core-py"}
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_reader_req_sends_cached_reader_body() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        body = BytesIO(b"stream-body")
        req = httpx.reader_req("application/octet-stream", body)

        first_payload: dict[str, Any] = {}
        await httpx.post(
            f"http://127.0.0.1:{server.server_port}/echo",
            req,
            httpx.json_resp(first_payload),
        ).do()

        second_payload: dict[str, Any] = {}
        await httpx.post(
            f"http://127.0.0.1:{server.server_port}/echo",
            req,
            httpx.json_resp(second_payload),
        ).do()

        assert first_payload == {
            "content_type": "application/octet-stream",
            "body": "stream-body",
        }
        assert second_payload == first_payload
        assert body.tell() == len(b"stream-body")
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_service_discovery_func_adapters_rewrite_url() -> None:
    captured: dict[str, Any] = {}

    def resolve(req: httpx.ResolveRequest) -> httpx.ResolveResponse:
        captured["req"] = req
        return httpx.ResolveResponse(
            service_name=req.service_name,
            namespace=req.namespace,
            instances=[
                httpx.Instance(
                    instance_id="inst-1",
                    host="10.0.0.1",
                    port=8443,
                    scheme="https",
                    health_status="healthy",
                )
            ],
        )

    def pick(req: httpx.ResolveRequest, resp: httpx.ResolveResponse) -> httpx.Instance:
        captured["picker_req"] = req
        return resp.instances[0]

    opt = httpx.ServiceOptions(
        enable_discovery=True,
        resolver=httpx.resolver_func(resolve),
        picker=httpx.instance_picker_func(pick),
    )
    resolved = await httpx.resolve_url("http://svc.ns/v1?q=1", opt, "trace-1", "req-1")

    assert resolved == "https://10.0.0.1:8443/v1?q=1"
    assert captured["req"].service_name == "svc"
    assert captured["req"].namespace == "ns"
    assert captured["picker_req"].trace_id == "trace-1"
    assert captured["picker_req"].request_id == "req-1"


@pytest.mark.asyncio
async def test_service_discovery_preserves_original_host_header() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:

        def resolve(req: httpx.ResolveRequest) -> httpx.ResolveResponse:
            return httpx.ResolveResponse(
                service_name=req.service_name,
                namespace=req.namespace,
                instances=[
                    httpx.Instance(
                        instance_id="inst-1",
                        host="127.0.0.1",
                        port=server.server_port,
                        scheme="http",
                        health_status="healthy",
                    )
                ],
            )

        payload: dict[str, Any] = {}
        await httpx.get(
            "http://inventory.prod/ping",
            httpx.service(
                httpx.ServiceOptions(enable_discovery=True, resolver=httpx.resolver_func(resolve))
            ),
            httpx.json_resp(payload),
        ).do()

        assert payload["host"] == "inventory.prod"
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_instance_override_preserves_original_host_header() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        await httpx.get(
            "http://inventory.prod/ping",
            httpx.service(
                httpx.ServiceOptions(
                    enable_discovery=True,
                    resolver=httpx.resolver_func(
                        lambda req: (_ for _ in ()).throw(
                            AssertionError("resolver should not be called")
                        )
                    ),
                    instance_override=httpx.Instance(
                        host="127.0.0.1", port=server.server_port, scheme="http"
                    ),
                )
            ),
            httpx.json_resp(payload),
        ).do()

        assert payload["host"] == "inventory.prod"
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_service_discovery_resolver_errors_are_wrapped_at_agent_boundary() -> None:
    def resolve(req: httpx.ResolveRequest) -> httpx.ResolveResponse:
        del req
        raise ValueError("resolver boom")

    with pytest.raises(httpx.CallError) as exc:
        await httpx.get(
            "http://svc.ns/v1",
            httpx.service(
                httpx.ServiceOptions(
                    enable_discovery=True,
                    resolver=httpx.resolver_func(resolve),
                )
            ),
        ).do()

    assert isinstance(exc.value.err, ValueError)
    assert str(exc.value.err) == "resolver boom"
    assert data.code_of(exc.value) == data.ERR_CODE_UNEXPECTED


@pytest.mark.asyncio
async def test_resolve_url_returns_picker_errors_without_boundary_wrapping() -> None:
    def resolve(req: httpx.ResolveRequest) -> httpx.ResolveResponse:
        return httpx.ResolveResponse(req.service_name, req.namespace, [])

    def pick(req: httpx.ResolveRequest, resp: httpx.ResolveResponse) -> httpx.Instance:
        del req, resp
        raise httpx.NoHealthyInstanceError("no instances")

    opt = httpx.ServiceOptions(
        enable_discovery=True,
        resolver=httpx.resolver_func(resolve),
        picker=httpx.instance_picker_func(pick),
    )

    with pytest.raises(httpx.NoHealthyInstanceError) as exc:
        await httpx.resolve_url("http://svc.ns/v1", opt, "trace-1", "req-1")

    assert str(exc.value) == "no instances"
    assert data.code_of(exc.value) == httpx.ERR_CODE_HTTP_NO_HEALTHY_INSTANCE


@pytest.mark.asyncio
async def test_prepare_phase_errors_are_not_retried() -> None:
    agent = httpx.get(
        "http://inventory.prod/ping",
        httpx.service(httpx.ServiceOptions(enable_discovery=True)),
        httpx.retry(httpx.RetryOpt(attempts=3, base_delay=0.001, max_delay=0.001)),
    )
    calls = 0
    original_prepare = agent._prepare_request

    async def wrapped_prepare() -> tuple[httpx.PreparedRequest, str]:
        nonlocal calls
        calls += 1
        return await original_prepare()

    agent._prepare_request = wrapped_prepare  # type: ignore[method-assign]

    with pytest.raises(httpx.CallError) as exc:
        await agent.do()

    assert isinstance(exc.value.err, httpx.ServiceDiscoveryDisabledError)
    assert data.code_of(exc.value) == httpx.ERR_CODE_HTTP_SERVICE_DISCOVERY_DISABLED
    assert calls == 1


@pytest.mark.asyncio
async def test_retry_attempts_less_than_one_use_default_attempts() -> None:
    attempts = 0

    class RetryHandler(Handler):
        def do_GET(self) -> None:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"temporary"}')
                return
            self._write_json({"ok": True})

    server = HTTPServer(("127.0.0.1", 0), RetryHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        await httpx.get(
            f"http://127.0.0.1:{server.server_port}/retry-default",
            httpx.json_resp(payload),
            httpx.expected_status_codes([200]),
            httpx.retry_status_codes([500]),
            httpx.retry(httpx.RetryOpt(attempts=-1, base_delay=0.001, max_delay=0.001)),
        ).do()
        assert attempts == 3
        assert payload == {"ok": True}
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_http_5xx_does_not_retry_without_retryable_marker() -> None:
    attempts = 0

    class RetryHandler(Handler):
        def do_GET(self) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"temporary"}')
                return
            self._write_json({"ok": True})

    server = HTTPServer(("127.0.0.1", 0), RetryHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        with pytest.raises(httpx.CallError) as exc:
            await httpx.get(
                f"http://127.0.0.1:{server.server_port}/retry-500",
                httpx.json_resp(payload),
                httpx.retry(httpx.RetryOpt(attempts=2, base_delay=0.001, max_delay=0.001)),
            ).do()
        assert attempts == 1
        assert isinstance(exc.value.err, httpx.HTTPStatusError)
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_httpx_do_stream_returns_unread_body() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        stream = await httpx.get(f"http://127.0.0.1:{server.server_port}/ping").do_stream()
        try:
            assert stream.status_code == 200
            assert stream.headers["Content-Type"] == "application/json"
            payload = json.loads((await stream.body.read()).decode())
            assert payload["request_id"].startswith("req_")
        finally:
            await stream.body.aclose()
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_httpx_restores_request_scoped_context_after_call() -> None:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        payload: dict[str, Any] = {}
        with context.use_context():
            context.set_trace_id("trace-restore")
            context.set_operator("user-restore")

            await httpx.get(
                f"http://127.0.0.1:{server.server_port}/ping",
                httpx.timeout_quota(0.5),
                httpx.json_resp(payload),
            ).do()

            assert context.get_trace_id() == ("trace-restore", True)
            assert context.get_operator() == ("user-restore", True)
            assert context.get_request_id() == ("", False)
            assert context.get_remaining_timeout_ms() == ("", False)
            assert context.get_request_deadline() == (0.0, False)
        assert payload["request_id"].startswith("req_")
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_use_context_from_headers_restores_outer_context() -> None:
    headers = {
        httpx.HEADER_OPERATOR: "user-inbound",
        httpx.HEADER_REQUEST_ID: "req-inbound",
        httpx.HEADER_REMAINING_TIMEOUT_MS: "200",
    }

    context.clear_current_context()
    with context.use_context():
        context.set_trace_id("trace-outer")

        with httpx.use_context_from_headers(headers, default_timeout=1.0) as deadline:
            assert deadline is not None
            assert context.get_trace_id() == ("trace-outer", True)
            assert context.get_operator() == ("user-inbound", True)
            assert context.get_request_id() == ("req-inbound", True)
            request_deadline, ok = context.get_request_deadline()
            assert ok is True
            assert request_deadline > time.time()

        assert context.get_trace_id() == ("trace-outer", True)
        assert context.get_operator() == ("", False)
        assert context.get_request_id() == ("", False)
        assert context.get_remaining_timeout_ms() == ("", False)
        assert context.get_request_deadline() == (0.0, False)


def test_context_from_headers_accepts_lowercase_direct_headers() -> None:
    headers = {
        httpx.HEADER_OPERATOR.lower(): "user-inbound",
        httpx.HEADER_REQUEST_ID.lower(): "req-inbound",
        httpx.HEADER_REMAINING_TIMEOUT_MS.lower(): "250",
    }

    context.clear_current_context()
    with context.use_context():
        deadline = httpx.context_from_headers(headers, default_timeout=1.0)

        assert deadline is not None
        assert context.get_operator() == ("user-inbound", True)
        assert context.get_request_id() == ("req-inbound", True)
        request_deadline, ok = context.get_request_deadline()
        assert ok is True
        assert request_deadline > time.time()


@pytest.mark.asyncio
async def test_httpx_inherits_inbound_remaining_timeout_budget() -> None:
    captured: dict[str, float | None] = {}
    agent = httpx.get("http://example.test/ping")

    async def fake_send(req: httpx.PreparedRequest) -> httpx.Response:
        captured["timeout"] = req.timeout
        return httpx.Response(200, {}, b"", req.url)

    agent._send_request = fake_send  # type: ignore[method-assign]

    with context.use_context():
        deadline = httpx.context_from_headers({httpx.HEADER_REMAINING_TIMEOUT_MS: "80"})
        assert deadline is not None
        await agent.do()

    assert captured["timeout"] is not None
    assert captured["timeout"] <= 0.2


@pytest.mark.asyncio
async def test_httpx_explicit_timeout_quota_can_only_tighten_inbound_budget() -> None:
    captured: dict[str, float | None] = {}
    agent = httpx.get("http://example.test/ping", httpx.timeout_quota(0.05))

    async def fake_send(req: httpx.PreparedRequest) -> httpx.Response:
        captured["timeout"] = req.timeout
        return httpx.Response(200, {}, b"", req.url)

    agent._send_request = fake_send  # type: ignore[method-assign]

    with context.use_context():
        deadline = httpx.context_from_headers({httpx.HEADER_REMAINING_TIMEOUT_MS: "1000"})
        assert deadline is not None
        await agent.do()

    assert captured["timeout"] is not None
    assert captured["timeout"] <= 0.1


@pytest.mark.asyncio
async def test_stream_response_context_manager_closes_body() -> None:
    body = BytesIO(b"payload")
    response = httpx.StreamResponse(200, {}, body, "http://example.test/stream")

    async with response as stream:
        assert await stream.body.read() == b"payload"

    assert body.closed is True
