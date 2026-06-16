from __future__ import annotations

import dataclasses
from typing import Any

from core_py.model.mongox.codec import gen_db_key, lookup_dataclass_field_meta, parse_bson_meta


def build_patch_payload(value: Any) -> dict[str, Any]:
    # Keep explicit Python zero-values so callers can intentionally clear fields.
    return build_patch_payload_with_parent(value, "")


def build_patch_payload_with_parent(value: Any, parent: str) -> dict[str, Any]:
    items: list[tuple[str, Any]]
    if dataclasses.is_dataclass(value):
        items = [(field_.name, getattr(value, field_.name)) for field_ in dataclasses.fields(value)]
    elif isinstance(value, dict):
        items = list(value.items())
    else:
        items = list(vars(value).items()) if hasattr(value, "__dict__") else []

    payload: dict[str, Any] = {}
    for raw_name, field_value in items:
        if raw_name.startswith("_") or field_value is None:
            continue
        meta = lookup_dataclass_field_meta(value, raw_name)
        name, inline, skip = parse_bson_meta(raw_name, meta)
        if skip:
            continue
        key = parent if inline else gen_db_key(parent, name)
        if dataclasses.is_dataclass(field_value):
            payload.update(
                build_patch_payload_with_parent(field_value, key if not inline else parent)
            )
        elif inline:
            continue
        else:
            payload[key] = field_value
    return payload
