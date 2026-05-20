"""Context propagation helpers for OFA pass/direct headers."""

from __future__ import annotations

from collections.abc import Mapping

Context = Mapping[str, str]

KEY_TRACE_ID = "TRACE_ID"
KEY_REQUEST_ID = "REQUEST_ID"
KEY_REMAINING_TIMEOUT_MS = "REMAINING_TIMEOUT_MS"
KEY_OPERATOR = "OPERATOR"
KEY_TENANT_ID = "TENANT_ID"
KEY_APP_ID = "APP_ID"
_PASS_HEADERS_KEY = "__ofa_pass_headers__"


def empty_context() -> dict[str, str]:
    return {}


def fixed_key(key: str) -> str:
    normalized = key.upper()
    if normalized.startswith("OFA_PASS_"):
        return normalized
    return "OFA_PASS_" + normalized.removeprefix("OFA_")


def fixed_key_direct(key: str) -> str:
    normalized = key.upper()
    if normalized.startswith("OFA_DIRECT_"):
        return normalized
    return "OFA_DIRECT_" + normalized.removeprefix("OFA_")


def ctx_get_pass_val(ctx: Context | None, key: str) -> tuple[str, bool]:
    if not ctx:
        return "", False
    value = ctx.get(fixed_key(key), "")
    return value, value != ""


def ctx_set_pass_val(ctx: Context | None, key: str, value: str) -> dict[str, str]:
    ret = dict(ctx or {})
    header = fixed_key(key)
    ret[header] = value
    ret[_PASS_HEADERS_KEY + header] = value
    return ret


def ctx_pass_headers(ctx: Context | None) -> dict[str, str]:
    if not ctx:
        return {}
    prefix = _PASS_HEADERS_KEY
    return {k.removeprefix(prefix): v for k, v in ctx.items() if k.startswith(prefix)}


def ctx_get_direct_val(ctx: Context | None, key: str) -> tuple[str, bool]:
    if not ctx:
        return "", False
    value = ctx.get(fixed_key_direct(key), "")
    return value, value != ""


def ctx_set_direct_val(ctx: Context | None, key: str, value: str) -> dict[str, str]:
    ret = dict(ctx or {})
    ret[fixed_key_direct(key)] = value
    return ret


def ctx_get_trace_id(ctx: Context | None) -> tuple[str, bool]:
    return ctx_get_pass_val(ctx, KEY_TRACE_ID)


def ctx_set_trace_id(ctx: Context | None, value: str) -> dict[str, str]:
    return ctx_set_pass_val(ctx, KEY_TRACE_ID, value)


def ctx_get_request_id(ctx: Context | None) -> tuple[str, bool]:
    return ctx_get_direct_val(ctx, KEY_REQUEST_ID)


def ctx_set_request_id(ctx: Context | None, value: str) -> dict[str, str]:
    return ctx_set_direct_val(ctx, KEY_REQUEST_ID, value)


def ctx_get_remaining_timeout_ms(ctx: Context | None) -> tuple[str, bool]:
    return ctx_get_direct_val(ctx, KEY_REMAINING_TIMEOUT_MS)


def ctx_set_remaining_timeout_ms(ctx: Context | None, value: str) -> dict[str, str]:
    return ctx_set_direct_val(ctx, KEY_REMAINING_TIMEOUT_MS, value)


def ctx_get_operator(ctx: Context | None) -> tuple[str, bool]:
    return ctx_get_pass_val(ctx, KEY_OPERATOR)


def ctx_set_operator(ctx: Context | None, value: str) -> dict[str, str]:
    return ctx_set_pass_val(ctx, KEY_OPERATOR, value)


def ctx_get_tenant_id(ctx: Context | None) -> tuple[str, bool]:
    return ctx_get_pass_val(ctx, KEY_TENANT_ID)


def ctx_set_tenant_id(ctx: Context | None, value: str) -> dict[str, str]:
    return ctx_set_pass_val(ctx, KEY_TENANT_ID, value)


def ctx_get_app_id(ctx: Context | None) -> tuple[str, bool]:
    return ctx_get_pass_val(ctx, KEY_APP_ID)


def ctx_set_app_id(ctx: Context | None, value: str) -> dict[str, str]:
    return ctx_set_pass_val(ctx, KEY_APP_ID, value)


# Go-style compatibility aliases.
FixedKey = fixed_key
FixedKeyDirect = fixed_key_direct
CtxGetTraceID = ctx_get_trace_id
CtxSetTraceID = ctx_set_trace_id
CtxGetRequestID = ctx_get_request_id
CtxSetRequestID = ctx_set_request_id
CtxGetRemainingTimeoutMS = ctx_get_remaining_timeout_ms
CtxSetRemainingTimeoutMS = ctx_set_remaining_timeout_ms
CtxGetOperator = ctx_get_operator
CtxSetOperator = ctx_set_operator
CtxGetTenantID = ctx_get_tenant_id
CtxSetTenantID = ctx_set_tenant_id
CtxGetAppID = ctx_get_app_id
CtxSetAppID = ctx_set_app_id
CtxPassHeaders = ctx_pass_headers
