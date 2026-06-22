"""Configuration loading with deterministic merge, validation and masking."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import UnionType
from typing import Any, TypeVar, Union, cast, get_args, get_origin, overload

import yaml

from core_py import data as data_mod
from core_py import logging

T = TypeVar("T")
DEFAULT_SENSITIVE_KEYS = ("password", "passwd", "secret", "token", "key", "uri")


@dataclass(slots=True)
class Options:
    default_config_path: str = "configs/config.yaml"
    env_prefix: str = "APP"
    env_separator: str = "__"
    # deploy_env_key is the logical key of deployment profile, e.g. ENV -> APP__ENV.
    deploy_env_key: str = "ENV"
    args: Sequence[str] | None = None
    required_keys: Sequence[str] = field(default_factory=tuple)
    sensitive_keys: Sequence[str] = field(default_factory=lambda: DEFAULT_SENSITIVE_KEYS)
    strict: bool = True
    log_enabled: bool = True
    validate_map: Callable[[dict[str, Any]], None] | None = None
    validate_config: Callable[[Any], None] | None = None


@dataclass(slots=True)
class Meta:
    sources: list[str]
    hash: str
    summary: dict[str, Any]


@overload
def load(model: type[T], opts: Options | None = None) -> tuple[T, Meta]: ...


@overload
def load(model: None = None, opts: Options | None = None) -> tuple[dict[str, Any], Meta]: ...


def load(
    model: type[T] | None = None, opts: Options | None = None
) -> tuple[T | dict[str, Any], Meta]:
    opts = opts or Options()
    args = list(sys.argv[1:] if opts.args is None else opts.args)
    merged: dict[str, Any] = {}
    source_map: dict[str, str] = {}
    sources: list[str] = []
    base_dir = Path(opts.default_config_path).parent

    default_map, ok = _load_config_if_exists(opts.default_config_path)
    if ok:
        merged = _merge_maps(merged, default_map)
        _record_sources(source_map, default_map, "default")
        sources.append("default")

    deploy_env = _resolve_deploy_env(opts)
    if deploy_env:
        env_file, ok = _load_config_if_exists(
            str(base_dir / f"config.{deploy_env.lower()}.yaml")
        )
        if ok:
            merged = _merge_maps(merged, env_file)
            _record_sources(source_map, env_file, "env-file")
            sources.append("env-file")

    local_file, ok = _load_config_if_exists(str(base_dir / "config.local.yaml"))
    if ok:
        merged = _merge_maps(merged, local_file)
        _record_sources(source_map, local_file, "local")
        sources.append("local")

    env_map = _env_to_map(opts.env_prefix, opts.env_separator, opts.deploy_env_key)
    if env_map:
        merged = _merge_maps(merged, _apply_typed_overrides(merged, env_map))
        _record_sources(source_map, env_map, "env")
        sources.append("env")

    flag_map = _args_to_map(args)
    if flag_map:
        merged = _merge_maps(merged, _apply_typed_overrides(merged, flag_map))
        _record_sources(source_map, flag_map, "flags")
        sources.append("flags")

    _validate_required(merged, opts.required_keys)
    _validate_sensitive_sources(merged, source_map, opts.sensitive_keys)
    if opts.validate_map:
        opts.validate_map(merged)

    cfg_hash = _hash_map(merged)
    summary = _mask_map(merged, opts.sensitive_keys)
    if opts.log_enabled:
        logging.info("config sources: %s", ",".join(sources))
        logging.info("config_hash=%s summary=%s", cfg_hash, json.dumps(summary, sort_keys=True))

    decode_model: type[Any] = dict if model is None else model
    out = _decode(decode_model, merged, strict=opts.strict)
    if opts.validate_config:
        opts.validate_config(out)
    return out, Meta(sources=sources, hash=cfg_hash, summary=summary)


def _load_config_if_exists(path: str) -> tuple[dict[str, Any], bool]:
    if not path:
        return {}, False
    p = Path(path)
    if not p.exists():
        return {}, False
    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise data_mod.new_validation_error(f"config file {path} should contain a mapping")
    return _normalize_map(raw), True


def _normalize_map(value: Mapping[Any, Any]) -> dict[str, Any]:
    return {str(k).lower(): _normalize_value(v) for k, v in value.items()}


def _normalize_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _normalize_map(value)
    if isinstance(value, list):
        return [_normalize_value(v) for v in value]
    return value


def _merge_maps(dst: dict[str, Any], src: Mapping[str, Any]) -> dict[str, Any]:
    ret = dict(dst)
    for key, value in src.items():
        ret_value = ret.get(key)
        if isinstance(ret_value, dict) and isinstance(value, Mapping):
            ret[key] = _merge_maps(ret_value, cast(Mapping[str, Any], value))
        else:
            ret[key] = value
    return ret


def _set_path(data: dict[str, Any], nodes: Sequence[str], value: Any) -> None:
    if not nodes:
        return
    cur = data
    for node in nodes[:-1]:
        nxt = cur.get(node)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[node] = nxt
        cur = nxt
    cur[nodes[-1]] = value


def _get_path(data: Mapping[str, Any], nodes: Sequence[str]) -> tuple[Any, bool]:
    cur: Any = data
    for node in nodes:
        if not isinstance(cur, Mapping) or node not in cur:
            return None, False
        cur = cur[node]
    return cur, True


def _env_to_map(prefix: str, sep: str, deploy_env_key: str = "") -> dict[str, Any]:
    ret: dict[str, Any] = {}
    if not prefix or not sep:
        return ret
    env_prefix = prefix + sep
    deploy_env_var = _deploy_env_var_name(
        Options(env_prefix=prefix, env_separator=sep, deploy_env_key=deploy_env_key)
    )
    for key, value in os.environ.items():
        if not key.startswith(env_prefix):
            continue
        if not _is_valid_env_name(key):
            continue
        if deploy_env_key and key == deploy_env_var:
            _set_path(ret, [prefix.lower(), *[p.lower() for p in deploy_env_key.split(sep)]], value)
            continue
        path = key.removeprefix(env_prefix)
        nodes = path.split(sep)
        if path and all(nodes):
            _set_path(ret, [p.lower() for p in nodes], value)
    return ret


def _resolve_deploy_env(opts: Options) -> str:
    deploy_env_var = _deploy_env_var_name(opts)
    if deploy_env_var:
        deploy_env = os.getenv(deploy_env_var, "").strip()
        if deploy_env:
            return deploy_env
    return ""


def _deploy_env_var_name(opts: Options) -> str:
    if not opts.deploy_env_key:
        return ""
    if not opts.env_prefix or not opts.env_separator:
        return opts.deploy_env_key
    return f"{opts.env_prefix}{opts.env_separator}{opts.deploy_env_key}"


def _is_valid_env_name(name: str) -> bool:
    return bool(name) and all("A" <= ch <= "Z" or "0" <= ch <= "9" or ch == "_" for ch in name)


def _args_to_map(args: Sequence[str]) -> dict[str, Any]:
    ret: dict[str, Any] = {}
    for arg in args:
        if not arg.startswith("--") or "=" not in arg:
            continue
        key, value = arg[2:].split("=", 1)
        key = key.strip().lower()
        if key:
            _set_path(ret, key.split("."), value)
    return ret


def _apply_typed_overrides(base: Mapping[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    ret: dict[str, Any] = {}
    for key, value in overrides.items():
        hint = base.get(key)
        if isinstance(value, Mapping):
            ret[key] = _apply_typed_overrides(hint if isinstance(hint, Mapping) else {}, value)
        elif isinstance(value, str):
            ret[key] = _parse_with_hint(hint, value)
        else:
            ret[key] = value
    return ret


def _parse_with_hint(hint: Any, value: str) -> Any:
    if isinstance(hint, bool):
        parsed = _parse_bool(value)
        return parsed if parsed is not None else value
    if isinstance(hint, int) and not isinstance(hint, bool):
        try:
            return int(value)
        except ValueError:
            return value
    if isinstance(hint, float):
        try:
            return float(value)
        except ValueError:
            return value
    parsed_bool = _parse_bool(value)
    if parsed_bool is not None:
        return parsed_bool
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _parse_bool(value: str) -> bool | None:
    low = value.lower()
    if low in {"true", "1", "t", "yes", "y", "on"}:
        return True
    if low in {"false", "0", "f", "no", "n", "off"}:
        return False
    return None


def _validate_required(data: Mapping[str, Any], keys: Sequence[str]) -> None:
    for key in keys:
        value, ok = _get_path(data, key.lower().split("."))
        if not ok or _is_empty(value):
            raise data_mod.new_validation_error(f"missing {key}")


def _is_empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def _record_sources(
    source_map: dict[str, str], data: Mapping[str, Any], source: str, prefix: str = ""
) -> None:
    for key, value in data.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, Mapping):
            _record_sources(source_map, value, source, path)
        else:
            source_map[path] = source


def _validate_sensitive_sources(
    data: Mapping[str, Any], sources: Mapping[str, str], sensitive: Sequence[str]
) -> None:
    for path, value in _flatten_map(data).items():
        if _is_sensitive_path(path, sensitive) and not _is_empty(value):
            if sources.get(path) not in {"env", "local"}:
                raise data_mod.new_validation_error(
                    f"sensitive config {path} must come from env or config.local.yaml"
                )
            if _is_placeholder(value):
                raise data_mod.new_validation_error(
                    f"sensitive config {path} must not be a placeholder"
                )


def _mask_map(data: Mapping[str, Any], sensitive: Sequence[str]) -> dict[str, Any]:
    return {k: _mask_value(k, v, sensitive) for k, v in data.items()}


def _is_placeholder(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if not text.strip("*"):
        return True
    if "***" in text:
        return True
    return text.lower() in {"redacted", "<redacted>", "changeme", "replace_me", "placeholder"}


def _mask_value(path: str, value: Any, sensitive: Sequence[str]) -> Any:
    if isinstance(value, Mapping):
        return {k: _mask_value(f"{path}.{k}", v, sensitive) for k, v in value.items()}
    if isinstance(value, list):
        return [_mask_value(path, v, sensitive) for v in value]
    if _is_sensitive_path(path, sensitive):
        return "***"
    if isinstance(value, str):
        return _mask_uri(value)
    return value


def _is_sensitive_path(path: str, sensitive: Sequence[str]) -> bool:
    low = path.lower()
    return any(s and s.lower() in low for s in sensitive)


def _mask_uri(value: str) -> str:
    if "://" not in value or "@" not in value:
        return value
    scheme, rest = value.split("://", 1)
    user_info, host = rest.split("@", 1)
    if not user_info:
        return value
    user = user_info.split(":", 1)[0]
    return f"{scheme}://{user}:***@{host}"


def _flatten_map(data: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    ret: dict[str, Any] = {}
    for key, value in data.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, Mapping):
            ret.update(_flatten_map(value, path))
        else:
            ret[path] = value
    return ret


def _hash_map(data: Mapping[str, Any]) -> str:
    flat = _flatten_map(data)
    payload = "".join(f"{k}={flat[k]}\n" for k in sorted(flat))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _decode(model: type[T], data: Mapping[str, Any], *, strict: bool) -> T:
    if model in (dict, Mapping) or model is Any:
        return cast(T, dict(data))
    return cast(T, _decode_value(model, data, strict=strict))


def _decode_value(tp: Any, value: Any, *, strict: bool) -> Any:
    origin = get_origin(tp)
    if origin is UnionType or origin is Union:
        args = [a for a in get_args(tp) if a is not type(None)]
        return None if value is None else _decode_value(args[0], value, strict=strict)
    if isinstance(tp, type) and dataclasses.is_dataclass(tp):
        if not isinstance(value, Mapping):
            raise data_mod.new_validation_error(f"expected mapping for {tp}")
        fields = {f.name: f for f in dataclasses.fields(tp)}
        if strict:
            unknown = set(value) - set(fields)
            if unknown:
                raise data_mod.new_validation_error(
                    f"unknown config fields for {tp.__name__}: {sorted(unknown)}"
                )
        kwargs = {}
        for name, f in fields.items():
            if name in value:
                kwargs[name] = _decode_value(f.type, value[name], strict=strict)
        return tp(**kwargs)
    if origin in (list, Sequence):
        item_tp = get_args(tp)[0] if get_args(tp) else Any
        return [_decode_value(item_tp, v, strict=strict) for v in value]
    if origin is dict:
        return dict(value)
    if tp is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            parsed = _parse_bool(value)
            if parsed is not None:
                return parsed
        raise data_mod.new_validation_error(f"expected bool, got {value!r}")
    if tp is int:
        if isinstance(value, bool):
            raise data_mod.new_validation_error(f"expected int, got {value!r}")
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise data_mod.new_validation_error(f"expected int, got {value!r}", cause=exc) from exc
    if tp is float:
        if isinstance(value, bool):
            raise data_mod.new_validation_error(f"expected float, got {value!r}")
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise data_mod.new_validation_error(
                f"expected float, got {value!r}", cause=exc
            ) from exc
    try:
        if tp is str and value is not None:
            return tp(value)
    except (TypeError, ValueError):
        return value
    return value
