import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from core_py import context, httpx


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(
            {
                "trace_id": self.headers.get(httpx.HEADER_TRACE_ID),
                "operator": self.headers.get(httpx.HEADER_OPERATOR),
                "request_id": self.headers.get(httpx.HEADER_REQUEST_ID),
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def test_httpx_injects_trace_headers():
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        ctx = context.ctx_set_trace_id(context.empty_context(), "trace-1")
        ctx = context.ctx_set_operator(ctx, "user-1")
        payload = {}
        httpx.get(
            f"http://127.0.0.1:{server.server_port}/ping",
            httpx.Context(ctx),
            httpx.JSONResp(payload),
        ).do()
        assert payload["trace_id"] == "trace-1"
        assert payload["operator"] == "user-1"
        assert payload["request_id"].startswith("req_")
    finally:
        server.shutdown()
        thread.join(timeout=2)
