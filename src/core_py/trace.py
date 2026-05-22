"""Trace and request ID helpers."""

from __future__ import annotations

import base64
import secrets
from datetime import UTC, datetime

HEADER_TRACE_ID = "OFA_PASS_TRACE_ID"
HEADER_OPERATOR = "OFA_PASS_OPERATOR"
HEADER_TENANT_ID = "OFA_PASS_TENANT_ID"
HEADER_APP_ID = "OFA_PASS_APP_ID"
HEADER_REQUEST_ID = "OFA_DIRECT_REQUEST_ID"
HEADER_REMAINING_TIMEOUT_MS = "OFA_DIRECT_REMAINING_TIMEOUT_MS"


def new_trace_id() -> str:
    return secrets.token_hex(16)


def new_request_id(now: datetime | None = None) -> str:
    return new_request_id_with_time(now or datetime.now(UTC))


def new_request_id_with_time(now: datetime) -> str:
    if now.tzinfo is not None:
        now = now.astimezone(UTC).replace(tzinfo=None)
    suffix = base64.b32encode(secrets.token_bytes(10)).decode("ascii").rstrip("=").lower()
    return f"req_{now.strftime('%Y%m%d_%H%M%S')}_{suffix}"
