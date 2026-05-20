from datetime import datetime

from core_py import context, trace


def test_context_pass_and_direct_headers():
    ctx = context.empty_context()
    ctx = context.ctx_set_trace_id(ctx, "trace-1")
    ctx = context.ctx_set_request_id(ctx, "req-1")
    ctx = context.ctx_set_operator(ctx, "user-1")

    assert context.ctx_get_trace_id(ctx) == ("trace-1", True)
    assert context.ctx_get_request_id(ctx) == ("req-1", True)
    assert context.ctx_pass_headers(ctx) == {
        "OFA_PASS_TRACE_ID": "trace-1",
        "OFA_PASS_OPERATOR": "user-1",
    }
    assert context.fixed_key("OFA_TRACE_ID") == "OFA_PASS_TRACE_ID"
    assert context.fixed_key_direct("REQUEST_ID") == "OFA_DIRECT_REQUEST_ID"


def test_trace_id_formats():
    assert len(trace.new_trace_id()) == 32
    request_id = trace.new_request_id_with_time(datetime(2024, 1, 2, 3, 4, 5))
    assert request_id.startswith("req_20240102_030405_")
