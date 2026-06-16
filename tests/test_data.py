import pytest

from core_py import data


def test_factory_helpers_render_expected_messages() -> None:
    assert isinstance(data.new_resource_not_found_error(), data.ResourceError)
    assert isinstance(data.new_resource_conflict_error("users/u1"), data.ResourceError)
    assert str(data.new_resource_conflict_error("users/u1")) == "resource conflict: users/u1"
    assert isinstance(data.new_internal_error(), data.InternalError)
    assert str(data.new_internal_error()) == "internal server error"
    assert data.is_expected(data.Error(data.ERR_CODE_EXPECTED, "expected")) is True


def test_validation_error_variants_render_details() -> None:
    item = data.ValidateErrItem(param_name="tenant_id", reason="missing", detail={"required": True})
    err = data.new_validate_error(items=[item])

    assert err.message == "parameter validate failed"
    assert err.items == [item]


def test_http_and_upstream_errors_render_readable_messages() -> None:
    http_err = data.HTTPValidationError(502)
    assert str(http_err) == "http validate failed, status: [502]"
    assert data.code_of(http_err) == data.ERR_CODE_UNEXPECTED
    assert data.is_unexpected(http_err) is True
    assert (
        str(data.HTTPValidationError(400, b'{"error":"bad"}'))
        == 'http validate failed, status: [400], body : [{"error":"bad"}]'
    )
    assert (
        str(data.new_upstream_error("https://example.test", "GET", "req-1", ValueError("timeout")))
        == "upstream call failed: GET https://example.test: timeout"
    )


def test_is_err_code_uses_new_error_mapping() -> None:
    assert data.is_err_code(data.ERR_CODE_VALIDATE, data.new_validation_error()) is True
    assert data.is_err_code(data.ERR_CODE_CONFLICT, data.new_resource_conflict_error()) is True
    assert data.is_err_code(data.ERR_CODE_NOT_FOUND, data.new_resource_conflict_error()) is False


def test_core_error_classes() -> None:
    root = ValueError("root cause")
    expected = data.Error(data.ERR_CODE_EXPECTED, "expected failure", root)
    unexpected = data.Error(data.ERR_CODE_UNEXPECTED, "unexpected failure", root)

    assert str(expected) == "expected failure: root cause"
    assert expected.__cause__ is root
    assert data.is_expected(expected) is True
    assert data.is_unexpected(expected) is False

    assert str(unexpected) == "unexpected failure: root cause"
    assert unexpected.__cause__ is root
    assert data.is_expected(unexpected) is False
    assert data.is_unexpected(unexpected) is True


def test_unclassified_errors_are_unexpected() -> None:
    assert data.is_unexpected(RuntimeError("plain")) is True
    assert data.is_expected(None) is False


def test_compatibility_constructors_return_common_errors() -> None:
    assert data.is_expected(data.new_validate_error()) is True
    assert isinstance(data.new_validate_error(), data.ValidationError)
    assert data.is_unexpected(data.new_internal_error()) is True
    assert isinstance(data.new_internal_error(), data.InternalError)


def test_common_expected_errors() -> None:
    root = ValueError("root cause")
    item = data.ValidateErrItem(param_name="tenant_id", reason="missing")
    validation_err = data.new_validation_error("bad request", [item], root)

    assert data.is_expected(validation_err) is True
    assert str(validation_err) == "bad request: root cause"
    assert validation_err.__cause__ is root
    assert validation_err.items == [item]

    not_found_err = data.new_resource_not_found_error("users/u1", root)
    assert data.is_expected(not_found_err) is True
    assert str(not_found_err) == "resource not found: users/u1: root cause"
    assert not_found_err.resource == "users/u1"

    conflict_err = data.new_resource_conflict_error("users/u1", root)
    assert data.is_expected(conflict_err) is True
    assert str(conflict_err) == "resource conflict: users/u1: root cause"
    assert conflict_err.resource == "users/u1"


def test_common_unexpected_errors() -> None:
    root = ValueError("root cause")
    internal_err = data.new_internal_failure("state corrupted", root)

    assert data.is_unexpected(internal_err) is True
    assert str(internal_err) == "state corrupted: root cause"
    assert internal_err.__cause__ is root

    upstream_err = data.new_upstream_error(
        "order-service",
        "GET /v1/orders/o1",
        "req-1",
        root,
    )
    assert data.is_unexpected(upstream_err) is True
    assert str(upstream_err) == "upstream call failed: GET /v1/orders/o1 order-service: root cause"
    assert upstream_err.__cause__ is root
    assert upstream_err.target == "order-service"
    assert upstream_err.operation == "GET /v1/orders/o1"
    assert upstream_err.request_id == "req-1"

    upstream_validation_err = data.new_upstream_error(
        "order-service",
        "POST /v1/orders",
        "req-2",
        data.new_validation_error("bad request"),
    )
    assert data.code_of(upstream_validation_err) == data.ERR_CODE_VALIDATE
    assert data.is_expected(upstream_validation_err) is True


def test_code_of_maps_common_errors() -> None:
    item = data.ValidateErrItem(param_name="tenant_id", reason="missing")
    validation_err = data.new_validation_error("bad request", [item])

    assert data.code_of(validation_err) == data.ERR_CODE_VALIDATE
    assert data.is_err_code(data.ERR_CODE_VALIDATE, validation_err) is True

    not_found_err = data.new_resource_not_found_error("users/u1")
    assert data.code_of(not_found_err) == data.ERR_CODE_NOT_FOUND
    assert data.is_err_code(data.ERR_CODE_NOT_FOUND, not_found_err) is True

    conflict_err = data.new_resource_conflict_error("users/u1")
    assert data.code_of(conflict_err) == data.ERR_CODE_CONFLICT

    expected_err = data.Error(data.ERR_CODE_EXPECTED, "can show this")
    assert data.code_of(expected_err) == data.ERR_CODE_EXPECTED

    internal_err = data.new_internal_failure("state corrupted")
    assert data.code_of(internal_err) == data.ERR_CODE_UNEXPECTED

    assert data.code_of(RuntimeError("plain")) == data.ERR_CODE_UNEXPECTED
    assert data.code_of(None) == 0


def test_code_of_default_expected_error() -> None:
    expected_err = data.Error(data.ERR_CODE_EXPECTED, "expected")
    assert data.code_of(expected_err) == data.ERR_CODE_EXPECTED
    assert data.is_expected(expected_err) is True


def test_extra_data_error() -> None:
    root = data.new_validation_error("bad request")
    with_data = data.with_error_data(root, {"field": "tenant_id"})

    assert with_data is not None
    assert str(with_data) == "bad request"
    assert with_data.__cause__ is root
    assert data.code_of(with_data) == data.ERR_CODE_VALIDATE
    assert data.error_data(with_data) == {"field": "tenant_id"}
    assert data.with_error_data(None, "ignored") is None


def test_retryable_error() -> None:
    root = data.new_internal_failure("temporary failure")
    retryable = data.with_retryable_error(root)

    assert retryable is not None
    assert str(retryable) == "temporary failure"
    assert retryable.__cause__ is root
    assert data.is_retryable_error(retryable) is True
    assert data.code_of(retryable) == data.ERR_CODE_UNEXPECTED
    assert data.with_retryable_error(None) is None
    assert data.is_retryable_error(None) is False


def test_custom_code_error() -> None:
    err = data.Error(29999, "custom")
    assert data.code_of(err) == 29999
    assert data.is_err_code(29999, err) is True
    assert data.is_expected(err) is True


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
