import asyncio

from core_py import context


def test_context_pass_and_direct_headers() -> None:
    context.clear_current_context()

    with context.use_context():
        context.set_trace_id("trace-1")
        context.set_request_id("req-1")
        context.set_request_deadline(100.0)
        context.set_operator("user-1")
        context.set_locale("en-US")

        assert context.get_trace_id() == ("trace-1", True)
        assert context.get_request_id() == ("req-1", True)
        assert context.get_request_deadline() == (100.0, True)
        assert context.get_locale() == ("en-US", True)
        assert context.pass_headers() == {
            "ofa-pass-trace-id": "trace-1",
            "ofa-pass-operator": "user-1",
            "ofa-pass-locale": "en-US",
        }
        assert context.KEY_TRACE_ID == "ofa-pass-trace-id"
        assert context.KEY_REQUEST_ID == "ofa-direct-request-id"
        assert context.KEY_REQUEST_DEADLINE == "ofa-request-deadline"
        assert context.fixed_key("ofa-trace-id") == "ofa-pass-trace-id"
        assert context.fixed_key_direct("request_id") == "ofa-direct-request-id"
        assert context.fixed_key_value("request_deadline") == "ofa-request-deadline"


def test_implicit_context_in_sync_scope() -> None:
    context.clear_current_context()

    with context.use_context():
        context.set_trace_id("trace-sync")
        context.set_request_id("req-sync")
        context.set_operator("user-sync")
        context.set_locale("zh-CN")

        assert context.get_trace_id() == ("trace-sync", True)
        assert context.get_request_id() == ("req-sync", True)
        assert context.pass_headers() == {
            "ofa-pass-trace-id": "trace-sync",
            "ofa-pass-operator": "user-sync",
            "ofa-pass-locale": "zh-CN",
        }

    assert context.get_trace_id() == ("", False)
    assert context.get_request_id() == ("", False)


def test_implicit_context_propagates_in_async_tasks() -> None:
    async def child() -> tuple[tuple[str, bool], tuple[str, bool]]:
        await asyncio.sleep(0)
        return context.get_trace_id(), context.get_request_id()

    async def main() -> tuple[tuple[str, bool], tuple[str, bool]]:
        with context.use_context():
            context.set_trace_id("trace-async")
            context.set_request_id("req-async")
            task = asyncio.create_task(child())
            return await task

    context.clear_current_context()
    assert asyncio.run(main()) == (("trace-async", True), ("req-async", True))
    assert context.get_trace_id() == ("", False)
