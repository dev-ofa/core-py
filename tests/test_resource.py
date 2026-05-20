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


def test_data_handler_open_and_limits():
    manager = resource.new_manager(resource.with_data_max_bytes(16))

    stream = manager.open(None, "ofa-res?media_type=text/plain#data:text/plain;base64,aGVsbG8=")
    try:
        assert stream.body.read() == b"hello"
        assert stream.media_type == "text/plain"
        assert stream.size == 5
    finally:
        stream.body.close()

    with pytest.raises(resource.OpenError) as mismatch:
        manager.open(None, "ofa-res?media_type=image/png#data:text/plain;base64,aGVsbG8=")
    assert isinstance(mismatch.value.err, ValueError)

    with pytest.raises(resource.OpenError) as too_large:
        manager.open(None, "ofa-res#data:text/plain," + ("a" * 17))
    assert isinstance(too_large.value.err, resource.SizeLimitExceededError)


def test_manager_register_open_upload():
    manager = resource.new_manager()

    manager.register(
        "custom",
        resource.HandlerFuncs(
            open_func=lambda ctx, identifier: resource.Stream(
                body=_bytes("custom-body"),
                media_type=identifier.media_type,
                size=len("custom-body"),
                source_uri=identifier.source_uri,
            ),
            upload_func=lambda ctx, value: resource.parse("ofa-res#custom://uploaded"),
        ),
    )

    stream = manager.open(None, "ofa-res?media_type=text/plain#custom://item")
    try:
        assert stream.body.read() == b"custom-body"
    finally:
        stream.body.close()

    uploaded = manager.upload(None, "custom", resource.UploadInput(body=_bytes("x")))
    assert uploaded.scheme == "custom"

    with pytest.raises(resource.OpenError) as missing:
        manager.open(None, "ofa-res#missing://item")
    assert isinstance(missing.value.err, resource.UnsupportedSchemeError)

    with pytest.raises(resource.UploadError) as unsupported:
        manager.upload(None, "http", resource.UploadInput())
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


def test_http_handler_open_download_retry_and_headers(tmp_path):
    _RetryHandler.attempts = 0
    server, thread = _start_server(_RetryHandler)
    try:
        manager = resource.new_manager(resource.with_retry(2, 0.001, 0.001))
        raw = f"ofa-res#http://127.0.0.1:{server.server_port}/file"

        stream = manager.open(None, raw)
        try:
            assert stream.media_type == "text/plain"
            assert stream.filename == "hello.txt"
            assert stream.body.read() == b"hello"
        finally:
            stream.body.close()
        assert _RetryHandler.attempts == 2

        dst = tmp_path / "out.txt"
        manager.download(None, raw, str(dst))
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


def test_http_handler_limits_and_can_disable_http():
    server, thread = _start_server(_LargeHandler)
    try:
        manager = resource.new_manager(resource.with_max_bytes(3))
        stream = manager.open(None, f"ofa-res#http://127.0.0.1:{server.server_port}/file")
        try:
            with pytest.raises(resource.SizeLimitExceededError):
                stream.body.read()
        finally:
            stream.body.close()

        no_http = resource.new_manager(resource.with_http_enabled(False))
        with pytest.raises(resource.OpenError) as disabled:
            no_http.open(None, "ofa-res#http://example.com/a.png")
        assert isinstance(disabled.value.err, resource.UnsupportedSchemeError)
    finally:
        server.shutdown()
        thread.join(timeout=2)


class _RedirectHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(302)
        self.send_header("Location", "/next")
        self.end_headers()

    def log_message(self, format, *args):
        return


def test_http_handler_limits_redirects():
    server, thread = _start_server(_RedirectHandler)
    try:
        manager = resource.new_manager(resource.with_redirect_limit(0))
        with pytest.raises(resource.OpenError):
            manager.open(None, f"ofa-res#http://127.0.0.1:{server.server_port}/file")
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_download_cleans_temp_on_failure(tmp_path):
    class ErrReader:
        def read(self, size=-1):
            raise OSError("read failed")

        def close(self):
            return None

    manager = resource.new_manager()
    manager.register(
        "fail",
        resource.HandlerFuncs(
            open_func=lambda ctx, identifier: resource.Stream(
                body=ErrReader(),
                size=-1,
                source_uri=identifier.source_uri,
            )
        ),
    )

    dst = tmp_path / "out.bin"
    with pytest.raises(resource.DownloadError):
        manager.download(None, "ofa-res#fail://x", str(dst))
    assert not dst.exists()
    assert list(tmp_path.iterdir()) == []


def _bytes(value: str):
    from io import BytesIO

    return BytesIO(value.encode())


def _start_server(handler):
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread
