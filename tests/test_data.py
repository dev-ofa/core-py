import pytest

from core_py import data


def test_base_error_and_factory_helpers_render_expected_messages() -> None:
    assert str(data.BaseError(data.ERR_CODE_INTERNAL, "boom")) == "boom"
    assert (
        str(data.BaseError(data.ERR_CODE_VALIDATE, "bad request", source_srv="order"))
        == "call: order failed, code: 20000, msg: bad request"
    )
    assert data.new_not_found_error().code == data.ERR_CODE_NOT_FOUND
    assert data.new_conflict_error("conflict").message == "conflict"
    assert data.new_internal_error().message == "internal server error"
    assert data.new_friendly_error("friendly").code == data.ERR_CODE_FRIENDLY


def test_validation_error_variants_render_details() -> None:
    item = data.ValidateErrItem(param_name="tenant_id", reason="missing", detail={"required": True})
    err = data.new_validate_error(items=[item])

    assert err.code == data.ERR_CODE_VALIDATE
    assert err.message == "parameter validate failed"
    assert err.data == [item]
    assert (
        str(data.WrapperValidationError(123, "invalid", {"field": "name"}))
        == "wrapper validate failed, code: [123], msg : [invalid], data: [{'field': 'name'}]"
    )
    assert (
        str(data.WrapperValidationError(123, "invalid"))
        == "wrapper validate failed, code: [123], msg : [invalid]"
    )


def test_http_and_upstream_errors_render_readable_messages() -> None:
    assert str(data.HTTPValidationError(502)) == "http validate failed, status: [502]"
    assert (
        str(data.HTTPValidationError(400, b'{"error":"bad"}'))
        == 'http validate failed, status: [400], body : [{"error":"bad"}]'
    )
    assert (
        str(data.UpstreamCallError("https://example.test", "req-1", "GET", ValueError("timeout")))
        == "GET [https://example.test] failed, reqid:[req-1], source err: [timeout]"
    )


def test_is_err_code_traverses_nested_causes_and_upstream_source_error() -> None:
    source = data.new_validate_error("bad request")
    upstream = data.UpstreamCallError("https://example.test", "req-1", "POST", source)

    assert data.is_err_code(data.ERR_CODE_VALIDATE, upstream) is True

    try:
        raise RuntimeError("wrapper") from data.new_conflict_error("version changed")
    except RuntimeError as err:
        assert data.is_err_code(data.ERR_CODE_CONFLICT, err) is True
        assert data.is_err_code(data.ERR_CODE_NOT_FOUND, err) is False


@pytest.mark.parametrize(
    ("order_by", "expected"),
    [
        pytest.param("", [], id="空排序"),
        pytest.param(
            "created_at desc, updated_at , score asc",
            [
                data.SortPair("created_at", True),
                data.SortPair("updated_at", False),
                data.SortPair("score", False),
            ],
            id="解析多字段排序",
        ),
    ],
)
def test_sortable_parses_sort_pairs(order_by: str, expected: list[data.SortPair]) -> None:
    assert data.Sortable(order_by=order_by).get_sort_info() == expected
