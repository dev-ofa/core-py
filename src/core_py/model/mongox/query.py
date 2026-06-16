from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import UnionType
from typing import Any, Union, get_args, get_origin

from core_py import data
from core_py.model.snowflake_id import SnowflakeID

DEFAULT_FEED_CURSOR_FIELD = "_id"


@dataclass(slots=True)
class PageQueryInput:
    filter: dict[str, Any] = field(default_factory=dict)
    pager: Any = None
    sort: Any = None


@dataclass(slots=True)
class FeedQueryInput:
    filter: dict[str, Any] = field(default_factory=dict)
    pager: Any = None
    cursor_field: str = ""
    is_descending: bool = False


@dataclass(slots=True)
class PatchRawInput:
    filter: dict[str, Any] = field(default_factory=dict)
    patch_payload: dict[str, Any] = field(default_factory=dict)
    is_many: bool = False
    skip_inject_cond: bool = False


def page_limit_skip(pager: Any) -> tuple[int, int]:
    if pager is None:
        return 0, 0
    if hasattr(pager, "get_page_info"):
        page_size, page_num, _ = pager.get_page_info()
    else:
        page_size = int(getattr(pager, "page_size", 0) or 0)
        page_num = int(getattr(pager, "page_num", 0) or 0)
    skip = page_size * (page_num - 1) if page_size > 0 and page_num > 0 else 0
    return page_size, skip


def sort_conf(sort: Any) -> list[tuple[str, int]]:
    if sort is None:
        return []
    pairs = sort.get_sort_info() if hasattr(sort, "get_sort_info") else []
    return [(pair.field, -1 if pair.is_descending else 1) for pair in pairs]


def merge_feed_filter(base: dict[str, Any], cursor: dict[str, Any]) -> dict[str, Any]:
    if not base:
        return cursor
    if not cursor:
        return base
    return {"$and": [base, cursor]}


def normalize_feed_cursor_field(cursor_field: str) -> str:
    if not cursor_field or cursor_field == "id":
        return DEFAULT_FEED_CURSOR_FIELD
    return cursor_field


def feed_cursor_filter(cursor_field: str, page_token: Any, is_descending: bool) -> dict[str, Any]:
    op = "$lt" if is_descending else "$gt"
    return {cursor_field: {op: page_token}}


def feed_sort_conf(cursor_field: str, is_descending: bool) -> list[tuple[str, int]]:
    return [(cursor_field, -1 if is_descending else 1)]


def parse_feed_token(page_token: str, field_type: Any | None) -> Any:
    field_type = unwrap_optional_type(field_type)
    if field_type in (None, Any, str):
        return page_token
    try:
        if field_type is int:
            return int(page_token)
        if field_type is SnowflakeID:
            return SnowflakeID(page_token).int_value
        if field_type is float:
            return float(page_token)
        if field_type is datetime:
            return datetime.fromisoformat(page_token)
        if field_type is bool:
            normalized = page_token.lower()
            if normalized in ("1", "true", "yes"):
                return True
            if normalized in ("0", "false", "no"):
                return False
            raise data.new_validation_error("parse feed token failed")
    except data.ValidationError:
        raise
    except Exception as exc:
        raise data.new_validation_error("parse feed token failed", cause=exc) from exc
    return page_token


def unwrap_optional_type(field_type: Any | None) -> Any | None:
    if field_type is None:
        return None
    origin = get_origin(field_type)
    if origin not in (UnionType, Union):
        return field_type
    args = [arg for arg in get_args(field_type) if arg is not type(None)]
    return args[0] if len(args) == 1 else field_type
