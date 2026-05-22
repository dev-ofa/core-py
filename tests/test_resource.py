import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from core_py import resource


def test_parse_identifier_extracts_params_and_preserves_source_uri():
    identifier = resource.parse(
        "ofa-res?auth_id=tenant&media_type=image/png&filename=a.png&x_tag=v#aws_s3://bucket/path/a.png"
    )

    assert identifier.scheme == "aws_s3"
    assert identifier.source_uri == "aws_s3://bucket/path/a.png"
    assert identifier.auth_id == "tenant"
    assert identifier.media_type == "image/png"
    assert identifier.params["filename"] == "a.png"
    assert identifier.params["x_tag"] == "v"

    with_query = resource.parse("ofa-res#https://example.com/a.png?version=1")
    assert with_query.source_uri == "https://example.com/a.png?version=1"


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "http://example.com/a.png",
        "ofa-res",
        "ofa-res?bad-name=v#https://example.com/a.png",
        "ofa-res?a=1&a=2#https://example.com/a.png",
        "ofa-res?filename=../a.png#https://example.com/a.png",
        "ofa-res#HTTPS://example.com/a.png",
    ],
)
def test_parse_rejects_invalid_inputs(raw):
    with pytest.raises(resource.ParseError):
        resource.parse(raw)


@pytest.mark.asyncio
async def test_data_handler_open_and_limits():
    manager = resource.Manager(resource.with_data_max_bytes(16))

    stream = await manager.open("ofa-res?media_type=text/plain#data:text/plain;base64,aGVsbG8=")
    try:
        assert await stream.body.read() == b"hello"
        assert stream.media_type == "text/plain"
        assert stream.size == 5
    finally:
        await stream.body.aclose()

    with pytest.raises(resource.OpenError) as mismatch:
        await manager.open("ofa-res?media_type=image/png#data:text/plain;base64,aGVsbG8=")
    assert isinstance(mismatch.value.err, ValueError)

    with pytest.raises(resource.OpenError) as too_large:
        await manager.open("ofa-res#data:text/plain," + ("a" * 17))
    assert isinstance(too_large.value.err, resource.SizeLimitExceededError)


@pytest.mark.asyncio
async def test_manager_register_open_upload():
    manager = resource.Manager()

    manager.register(
        "custom",
        resource.HandlerFuncs(
            open_func=lambda identifier: resource.Stream(
                body=_bytes("custom-body"),
                media_type=identifier.media_type,
                size=len("custom-body"),
                source_uri=identifier.source_uri,
            ),
            upload_func=lambda value: resource.parse("ofa-res#custom://uploaded"),
        ),
    )

    stream = await manager.open("ofa-res?media_type=text/plain#custom://item")
    try:
        assert await stream.body.read() == b"custom-body"
    finally:
        await stream.body.aclose()

    uploaded = await manager.upload("custom", resource.UploadInput(body=_bytes("x")))
    assert uploaded.scheme == "custom"

    with pytest.raises(resource.OpenError) as missing:
        await manager.open("ofa-res#missing://item")
    assert isinstance(missing.value.err, resource.UnsupportedSchemeError)

    with pytest.raises(resource.UploadError) as unsupported:
        await manager.upload("http", resource.UploadInput())
    assert isinstance(unsupported.value.err, resource.UploadUnsupportedError)


class _RetryHandler(BaseHTTPRequestHandler):
    attempts = 0

    def do_GET(self):
        type(self).attempts += 1
        if type(self).attempts == 1:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"temporary")
            return
        body = b"hello"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Disposition", 'attachment; filename="hello.txt"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


@pytest.mark.asyncio
async def test_http_handler_open_download_retry_and_headers(tmp_path):
    _RetryHandler.attempts = 0
    server, thread = _start_server(_RetryHandler)
    try:
        manager = resource.Manager(resource.with_retry(2, 0.001, 0.001))
        raw = f"ofa-res#http://127.0.0.1:{server.server_port}/file"

        stream = await manager.open(raw)
        try:
            assert stream.media_type == "text/plain"
            assert stream.filename == "hello.txt"
            assert await stream.body.read() == b"hello"
        finally:
            await stream.body.aclose()
        assert _RetryHandler.attempts == 2

        dst = tmp_path / "out.txt"
        await manager.download(raw, str(dst))
        assert dst.read_bytes() == b"hello"
    finally:
        server.shutdown()
        thread.join(timeout=2)


class _LargeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"too-large")

    def log_message(self, format, *args):
        return


@pytest.mark.asyncio
async def test_http_handler_limits_and_can_disable_http():
    server, thread = _start_server(_LargeHandler)
    try:
        manager = resource.Manager(resource.with_max_bytes(3))
        stream = await manager.open(f"ofa-res#http://127.0.0.1:{server.server_port}/file")
        try:
            with pytest.raises(resource.SizeLimitExceededError):
                await stream.body.read()
        finally:
            await stream.body.aclose()

        no_http = resource.Manager(resource.with_http_enabled(False))
        with pytest.raises(resource.OpenError) as disabled:
            await no_http.open("ofa-res#http://example.com/a.png")
        assert isinstance(disabled.value.err, resource.UnsupportedSchemeError)
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_download_rejects_over_limit_stream_without_content_length(tmp_path):
    server, thread = _start_server(_LargeHandler)
    try:
        manager = resource.Manager(resource.with_max_bytes(3))
        dst = tmp_path / "out.bin"

        with pytest.raises(resource.DownloadError) as exc:
            await manager.download(f"ofa-res#http://127.0.0.1:{server.server_port}/file", str(dst))

        assert isinstance(exc.value.err, resource.SizeLimitExceededError)
        assert not dst.exists()
        assert list(tmp_path.iterdir()) == []
    finally:
        server.shutdown()
        thread.join(timeout=2)


class _FakeHTTPResponse:
    status = 200
    url = "https://example.test/file"
    headers = {
        "Content-Type": "text/plain",
        "Content-Length": "6",
        "Content-Disposition": 'attachment; filename="client.txt"',
    }

    def __init__(self) -> None:
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        del size
        return b"client"

    def close(self) -> None:
        self.closed = True


class _FakeHTTPClient:
    def __init__(self) -> None:
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append((request, timeout))
        return _FakeHTTPResponse()


@pytest.mark.asyncio
async def test_http_handler_uses_custom_http_client():
    client = _FakeHTTPClient()
    manager = resource.Manager(resource.with_http_client(client), resource.with_timeout_quota(1.5))

    stream = await manager.open("ofa-res#https://example.test/file")
    try:
        assert await stream.body.read() == b"client"
        assert stream.media_type == "text/plain"
        assert stream.filename == "client.txt"
    finally:
        await stream.body.aclose()

    assert len(client.requests) == 1
    request, timeout = client.requests[0]
    assert request.full_url == "https://example.test/file"
    assert timeout is not None


class _RedirectHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(302)
        self.send_header("Location", "/next")
        self.end_headers()

    def log_message(self, format, *args):
        return


@pytest.mark.asyncio
async def test_http_handler_limits_redirects():
    server, thread = _start_server(_RedirectHandler)
    try:
        manager = resource.Manager(resource.with_redirect_limit(0))
        with pytest.raises(resource.OpenError):
            await manager.open(f"ofa-res#http://127.0.0.1:{server.server_port}/file")
    finally:
        server.shutdown()
        thread.join(timeout=2)


@pytest.mark.asyncio
async def test_download_cleans_temp_on_failure(tmp_path):
    class ErrReader:
        def read(self, size=-1):
            raise OSError("read failed")

        def close(self):
            return None

    manager = resource.Manager()
    manager.register(
        "fail",
        resource.HandlerFuncs(
            open_func=lambda identifier: resource.Stream(
                body=ErrReader(),
                size=-1,
                source_uri=identifier.source_uri,
            )
        ),
    )

    dst = tmp_path / "out.bin"
    with pytest.raises(resource.DownloadError):
        await manager.download("ofa-res#fail://x", str(dst))
    assert not dst.exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_stream_context_manager_closes_body():
    class Reader:
        def __init__(self) -> None:
            self.closed = False

        def read(self, size: int = -1) -> bytes:
            del size
            return b""

        def close(self) -> None:
            self.closed = True

    body = Reader()
    async with resource.Stream(body=body) as stream:
        assert await stream.body.read() == b""

    assert body.closed is True


def _bytes(value: str):
    from io import BytesIO

    return BytesIO(value.encode())


def _start_server(handler):
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread
