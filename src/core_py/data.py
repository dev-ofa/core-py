"""Shared data errors and pagination/sorting helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

ERR_CODE_INTERNAL = 10000
ERR_CODE_NOT_FOUND = 10001
ERR_CODE_CONFLICT = 10002
ERR_CODE_VALIDATE = 20000
ERR_CODE_FRIENDLY = 20001


@dataclass(slots=True)
class BaseError(Exception):
    code: int
    message: str
    data: Any = None
    source_srv: str = ""

    def __str__(self) -> str:
        if self.source_srv:
            return f"call: {self.source_srv} failed, code: {self.code}, msg: {self.message}"
        return self.message


@dataclass(slots=True)
class WrapperValidationError(Exception):
    code: int
    msg: str
    data: Any = None

    def __str__(self) -> str:
        if self.data is not None:
            return f"wrapper validate failed, code: [{self.code}], msg : [{self.msg}], data: [{self.data!r}]"
        return f"wrapper validate failed, code: [{self.code}], msg : [{self.msg}]"


@dataclass(slots=True)
class HTTPValidationError(Exception):
    status_code: int
    body: bytes = b""

    def __str__(self) -> str:
        if not self.body:
            return f"http validate failed, status: [{self.status_code}]"
        return f"http validate failed, status: [{self.status_code}], body : [{self.body.decode(errors='replace')}]"


@dataclass(slots=True)
class UpstreamCallError(Exception):
    url: str
    request_id: str
    method: str
    src_err: Exception

    def __str__(self) -> str:
        return f"{self.method} [{self.url}] failed, reqid:[{self.request_id}], source err: [{self.src_err}]"


ERR_NOT_FOUND = BaseError(ERR_CODE_NOT_FOUND, "data not found")
ERR_CONFLICT = BaseError(ERR_CODE_CONFLICT, "data is existed or has be updated")


def new_not_found_error(msg: str = "") -> BaseError:
    return BaseError(ERR_CODE_NOT_FOUND, msg or ERR_NOT_FOUND.message)


def new_conflict_error(msg: str = "") -> BaseError:
    return BaseError(ERR_CODE_CONFLICT, msg or ERR_CONFLICT.message)


def new_internal_error(msg: str = "") -> BaseError:
    return BaseError(ERR_CODE_INTERNAL, msg or "internal server error")


def new_friendly_error(msg: str) -> BaseError:
    return BaseError(ERR_CODE_FRIENDLY, msg)


@dataclass(slots=True)
class ValidateErrItem:
    param_name: str
    reason: str
    detail: Any = None


def new_validate_error(msg: str = "", items: list[ValidateErrItem] | None = None) -> BaseError:
    return BaseError(ERR_CODE_VALIDATE, msg or "parameter validate failed", items or [])


def is_err_code(code: int, err: BaseException | None) -> bool:
    cur = err
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, BaseError) and cur.code == code:
            return True
        if isinstance(cur, WrapperValidationError) and cur.code == code:
            return True
        if isinstance(cur, UpstreamCallError) and isinstance(cur.src_err, BaseException):
            cur = cur.src_err
            continue
        cur = cur.__cause__ or cur.__context__
    return False


@dataclass(slots=True)
class SortPair:
    field: str
    is_descending: bool = False


@dataclass(slots=True)
class Sortable:
    order_by: str = ""

    def get_sort_info(self) -> list[SortPair]:
        pairs: list[SortPair] = []
        for raw in self.order_by.split(","):
            raw = raw.strip()
            if not raw:
                continue
            parts = raw.split()
            pairs.append(SortPair(parts[0], len(parts) > 1 and parts[1] == "desc"))
        return pairs
