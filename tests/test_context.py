import asyncio

from core_py import context


def test_context_pass_and_direct_headers() -> None:
    context.clear_current_context()

    with context.use_context():
        context.set_trace_id("trace-1")
        context.set_request_id("req-1")
        context.set_operator("user-1")

        assert context.get_trace_id() == ("trace-1", True)
        assert context.get_request_id() == ("req-1", True)
        assert context.pass_headers() == {
            "OFA_PASS_TRACE_ID": "trace-1",
            "OFA_PASS_OPERATOR": "user-1",
        }
        assert context.fixed_key("OFA_TRACE_ID") == "OFA_PASS_TRACE_ID"
        assert context.fixed_key_direct("REQUEST_ID") == "OFA_DIRECT_REQUEST_ID"


def test_implicit_context_in_sync_scope() -> None:
    context.clear_current_context()

    with context.use_context():
        context.set_trace_id("trace-sync")
        context.set_request_id("req-sync")
        context.set_operator("user-sync")

        assert context.get_trace_id() == ("trace-sync", True)
        assert context.get_request_id() == ("req-sync", True)
        assert context.pass_headers() == {
            "OFA_PASS_TRACE_ID": "trace-sync",
            "OFA_PASS_OPERATOR": "user-sync",
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
