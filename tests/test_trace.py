import re
from datetime import datetime, timedelta, timezone

from core_py import trace


def test_trace_id_formats() -> None:
    assert len(trace.new_trace_id()) == 32
    request_id = trace.new_request_id_with_time(datetime(2024, 1, 2, 3, 4, 5))
    assert request_id.startswith("req_20240102_030405_")


def test_request_id_normalizes_aware_datetime_to_utc() -> None:
    request_id = trace.new_request_id_with_time(
        datetime(2024, 1, 2, 11, 4, 5, tzinfo=timezone(timedelta(hours=8)))
    )

    assert request_id.startswith("req_20240102_030405_")


def test_request_id_suffix_uses_lowercase_base32_shape() -> None:
    request_id = trace.new_request_id_with_time(datetime(2024, 1, 2, 3, 4, 5))

    assert re.fullmatch(r"req_20240102_030405_[a-z2-7]{16}", request_id) is not None
