from __future__ import annotations

import dataclasses
from types import UnionType
from typing import Any, Union, cast, get_args, get_origin, get_type_hints

from core_py.model.snowflake_id import SnowflakeID


def to_document(doc: Any, id_key: str) -> dict[str, Any]:
    if isinstance(doc, dict):
        payload = dict(doc)
    elif dataclasses.is_dataclass(doc):
        payload = {}
        for field_ in dataclasses.fields(doc):
            name = field_.name
            value = getattr(doc, name)
            if value is None:
                continue
            mapped_name, inline, skip = parse_bson_meta(
                name,
                field_bson_meta(field_),
            )
            if skip:
                continue
            if dataclasses.is_dataclass(value):
                nested = to_document(value, id_key)
                if inline:
                    payload.update(nested)
                else:
                    payload[mapped_name] = nested
            else:
                payload[mapped_name] = to_bson_value(value, id_key)
    else:
        payload = {
            field_name(k): to_bson_value(v, id_key)
            for k, v in vars(doc).items()
            if not k.startswith("_") and v is not None
        }
    if "id" in payload:
        payload[id_key] = payload.pop("id")
    return payload


def to_bson_value(value: Any, id_key: str = "_id") -> Any:
    if isinstance(value, SnowflakeID):
        return value.int_value
    if dataclasses.is_dataclass(value):
        return to_document(value, id_key)
    if isinstance(value, dict):
        return {key: to_bson_value(item, id_key) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_bson_value(item, id_key) for item in value]
    return value


def decode_document(entity_type: type[Any] | None, doc: Any, id_key: str) -> Any:
    if entity_type is None or not isinstance(doc, dict):
        return doc
    try:
        if dataclasses.is_dataclass(entity_type):
            return decode_dataclass(entity_type, doc, id_key)
        payload = dict(doc)
        if id_key in payload:
            payload["id"] = payload.pop(id_key)
        return entity_type(**payload)
    except TypeError:
        return doc


def get_id(doc: Any) -> Any:
    if hasattr(doc, "get_id"):
        return doc.get_id()
    if isinstance(doc, dict):
        return doc.get("_id", doc.get("id"))
    return getattr(doc, "id", getattr(doc, "_id", None))


def get_creator_info(doc: Any) -> tuple[Any, Any]:
    method = getattr(doc, "get_creator_info", None)
    if callable(method):
        return cast(tuple[Any, Any], method())
    return None, None


def type_supports(entity_type: type[Any] | None, *names: str) -> bool:
    if entity_type is None:
        return False
    return all(hasattr(entity_type, name) for name in names)


def supports(value: Any, name: str) -> bool:
    return hasattr(value, name)


def field_name(name: str) -> str:
    return "_id" if name == "id" else name


def gen_db_key(parent: str, field_key: str) -> str:
    return field_key if not parent else f"{parent}.{field_key}"


def field_bson_meta(field_: dataclasses.Field[Any]) -> str | None:
    value = field_.metadata.get("bson")
    return value if isinstance(value, str) else None


def lookup_dataclass_field_meta(value: Any, name: str) -> str | None:
    if not dataclasses.is_dataclass(value):
        return None
    for field_ in dataclasses.fields(value):
        if field_.name == name:
            return field_bson_meta(field_)
    return None


def parse_bson_meta(raw_name: str, meta: str | None) -> tuple[str, bool, bool]:
    name = field_name(raw_name)
    inline = False
    skip = False
    if meta:
        parts = [part.strip() for part in meta.split(",")]
        head = parts[0] if parts else ""
        options = set(parts[1:])
        if head == "-":
            skip = True
        elif head == "inline":
            inline = True
        elif head:
            name = head
        if "inline" in options:
            inline = True
    return name, inline, skip


def decode_dataclass(entity_type: type[Any], doc: dict[str, Any], id_key: str) -> Any:
    kwargs: dict[str, Any] = {}
    type_hints = get_type_hints(entity_type)
    typevar_map = generic_typevar_map(entity_type)
    for field_ in dataclasses.fields(entity_type):
        mapped_name, inline, skip = parse_bson_meta(field_.name, field_bson_meta(field_))
        if skip:
            continue
        field_type = resolve_field_type(type_hints.get(field_.name, field_.type), typevar_map)
        if inline:
            if is_dataclass_type(field_type):
                kwargs[field_.name] = decode_dataclass(field_type, doc, id_key)
            continue
        source_key = id_key if mapped_name == "_id" else mapped_name
        if source_key not in doc:
            continue
        kwargs[field_.name] = decode_value(field_type, doc[source_key], id_key)
    return entity_type(**kwargs)


def decode_value(field_type: Any, value: Any, id_key: str) -> Any:
    field_type = unwrap_optional_type(field_type)
    if field_type is SnowflakeID:
        return SnowflakeID(value)
    if is_dataclass_type(field_type) and isinstance(value, dict):
        return decode_dataclass(field_type, value, id_key)
    origin = get_origin(field_type)
    if origin is list and isinstance(value, list):
        item_type = unwrap_optional_type(get_args(field_type)[0]) if get_args(field_type) else Any
        return [decode_value(item_type, item, id_key) for item in value]
    return value


def unwrap_optional_type(field_type: Any) -> Any:
    origin = get_origin(field_type)
    if origin not in (UnionType, Union):
        return field_type
    args = [arg for arg in get_args(field_type) if arg is not type(None)]
    return args[0] if len(args) == 1 else field_type


def resolve_field_type(field_type: Any, typevar_map: dict[Any, Any]) -> Any:
    field_type = unwrap_optional_type(field_type)
    return typevar_map.get(field_type, field_type)


def generic_typevar_map(entity_type: type[Any]) -> dict[Any, Any]:
    ret: dict[Any, Any] = {}
    for base in getattr(entity_type, "__orig_bases__", ()):
        origin = get_origin(base)
        if origin is None:
            continue
        params = getattr(origin, "__parameters__", ())
        for param, arg in zip(params, get_args(base), strict=False):
            if hasattr(param, "__constraints__"):
                ret[param] = arg
    return ret


def is_dataclass_type(field_type: Any) -> bool:
    return isinstance(field_type, type) and dataclasses.is_dataclass(field_type)


def is_duplicate_error(exc: Exception) -> bool:
    if exc.__class__.__name__ == "DuplicateKeyError":
        return True
    return int(getattr(exc, "code", 0) or 0) == 11000
