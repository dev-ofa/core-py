"""Implicit context propagation helpers for OFA tracing and context keys."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import Any

Context = Mapping[str, Any]

KEY_TRACE_ID = "ofa-pass-trace-id"
KEY_REQUEST_ID = "ofa-direct-request-id"
KEY_REMAINING_TIMEOUT_MS = "ofa-direct-remaining-timeout-ms"
KEY_REQUEST_DEADLINE = "ofa-request-deadline"
KEY_OPERATOR = "ofa-pass-operator"
KEY_TENANT_ID = "ofa-pass-tenant-id"
KEY_APP_ID = "ofa-pass-app-id"
KEY_LOCALE = "ofa-pass-locale"

_PASS_PREFIX = "ofa-pass-"
_DIRECT_PREFIX = "ofa-direct-"
_CURRENT_CONTEXT: ContextVar[dict[str, Any] | None] = ContextVar("core_py_context", default=None)


def current_context() -> dict[str, Any]:
    return _copy_context(_CURRENT_CONTEXT.get())


def set_current_context(ctx: Context | None) -> Token[dict[str, Any] | None]:
    return _CURRENT_CONTEXT.set(_copy_context(ctx))


def reset_current_context(token: Token[dict[str, Any] | None]) -> None:
    _CURRENT_CONTEXT.reset(token)


def clear_current_context() -> None:
    _CURRENT_CONTEXT.set({})


@contextmanager
def use_context(ctx: Context | None = None) -> Iterator[dict[str, Any]]:
    token = set_current_context(current_context() if ctx is None else ctx)
    try:
        yield current_context()
    finally:
        reset_current_context(token)


def fixed_key(key: str) -> str:
    normalized = _normalize_key(key)
    if normalized.startswith(_PASS_PREFIX):
        return normalized
    return _PASS_PREFIX + normalized.removeprefix("ofa-")


def fixed_key_direct(key: str) -> str:
    normalized = _normalize_key(key)
    if normalized.startswith(_DIRECT_PREFIX):
        return normalized
    return _DIRECT_PREFIX + normalized.removeprefix("ofa-")


def fixed_key_value(key: str) -> str:
    normalized = _normalize_key(key)
    if normalized.startswith("ofa-"):
        return normalized
    return "ofa-" + normalized


def get_pass_value(key: str) -> tuple[str, bool]:
    return _get_string_value(fixed_key(key))


def set_pass_value(key: str, value: str) -> None:
    _update_value(fixed_key(key), value)


def pass_headers() -> dict[str, str]:
    return {
        key: value
        for key, value in current_context().items()
        if key.startswith(_PASS_PREFIX) and isinstance(value, str)
    }


def get_direct_value(key: str) -> tuple[str, bool]:
    return _get_string_value(fixed_key_direct(key))


def set_direct_value(key: str, value: str) -> None:
    _update_value(fixed_key_direct(key), value)


def get_trace_id() -> tuple[str, bool]:
    return get_pass_value(KEY_TRACE_ID)


def set_trace_id(value: str) -> None:
    set_pass_value(KEY_TRACE_ID, value)


def get_request_id() -> tuple[str, bool]:
    return get_direct_value(KEY_REQUEST_ID)


def set_request_id(value: str) -> None:
    set_direct_value(KEY_REQUEST_ID, value)


def get_remaining_timeout_ms() -> tuple[str, bool]:
    return get_direct_value(KEY_REMAINING_TIMEOUT_MS)


def set_remaining_timeout_ms(value: str) -> None:
    set_direct_value(KEY_REMAINING_TIMEOUT_MS, value)


def get_request_deadline() -> tuple[float, bool]:
    value = current_context().get(fixed_key_value(KEY_REQUEST_DEADLINE))
    if not isinstance(value, float) or value <= 0:
        return 0.0, False
    return value, True


def set_request_deadline(value: float) -> None:
    _update_value(fixed_key_value(KEY_REQUEST_DEADLINE), value)


def get_operator() -> tuple[str, bool]:
    return get_pass_value(KEY_OPERATOR)


def set_operator(value: str) -> None:
    set_pass_value(KEY_OPERATOR, value)


def get_tenant_id() -> tuple[str, bool]:
    return get_pass_value(KEY_TENANT_ID)


def set_tenant_id(value: str) -> None:
    set_pass_value(KEY_TENANT_ID, value)


def get_app_id() -> tuple[str, bool]:
    return get_pass_value(KEY_APP_ID)


def set_app_id(value: str) -> None:
    set_pass_value(KEY_APP_ID, value)


def get_locale() -> tuple[str, bool]:
    return get_pass_value(KEY_LOCALE)


def set_locale(value: str) -> None:
    set_pass_value(KEY_LOCALE, value)


def _copy_context(ctx: Context | None) -> dict[str, Any]:
    return dict(ctx) if ctx else {}


def _normalize_key(key: str) -> str:
    return key.strip().lower().replace("_", "-")


def _get_string_value(key: str) -> tuple[str, bool]:
    value = current_context().get(key, "")
    if not isinstance(value, str):
        return "", False
    return value, value != ""


def _update_value(key: str, value: Any) -> None:
    ctx = current_context()
    ctx[key] = value
    _CURRENT_CONTEXT.set(ctx)
