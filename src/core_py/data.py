"""Shared data errors and pagination/sorting helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

ERR_CODE_UNEXPECTED = 10000
ERR_CODE_INTERNAL = ERR_CODE_UNEXPECTED
ERR_CODE_EXPECTED = 20000
ERR_CODE_VALIDATE = 20001
ERR_CODE_NOT_FOUND = 20002
ERR_CODE_CONFLICT = 20003


class Error(Exception):
    __slots__ = ("code", "message", "cause")

    def __init__(
        self,
        code: int,
        message: str,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.cause = cause
        if cause is not None:
            self.__cause__ = cause

    def __str__(self) -> str:
        if self.message and self.cause is not None:
            return f"{self.message}: {self.cause}"
        if self.cause is not None:
            return str(self.cause)
        if self.message:
            return self.message
        return f"error code {self.code}"


@dataclass(slots=True)
class ExtraDataError(Exception):
    cause: BaseException
    data: Any = None

    def __post_init__(self) -> None:
        self.__cause__ = self.cause

    def __str__(self) -> str:
        return str(self.cause)


class HTTPValidationError(Error):
    __slots__ = ("status_code", "body")

    def __init__(self, status_code: int, body: bytes = b"") -> None:
        super().__init__(ERR_CODE_UNEXPECTED, "http validate failed")
        self.status_code = status_code
        self.body = body

    def __str__(self) -> str:
        if not self.body:
            return f"http validate failed, status: [{self.status_code}]"
        return f"http validate failed, status: [{self.status_code}], body : [{self.body.decode(errors='replace')}]"


@dataclass(slots=True)
class ValidateErrItem:
    param_name: str
    reason: str
    detail: Any = None


class ValidationError(Error):
    __slots__ = ("items",)

    def __init__(
        self,
        message: str = "validation failed",
        cause: BaseException | None = None,
        items: list[ValidateErrItem] | None = None,
    ) -> None:
        super().__init__(ERR_CODE_VALIDATE, message, cause)
        self.items = items or []


def new_validation_error(
    message: str = "",
    items: list[ValidateErrItem] | None = None,
    cause: BaseException | None = None,
) -> ValidationError:
    return ValidationError(message or "validation failed", cause, items)


class ResourceError(Error):
    __slots__ = ("resource",)

    def __init__(
        self,
        code: int,
        message: str = "resource error",
        cause: BaseException | None = None,
        resource: str = "",
    ) -> None:
        super().__init__(code, message, cause)
        self.resource = resource


def new_resource_error(
    code: int,
    message: str = "",
    resource: str = "",
    cause: BaseException | None = None,
) -> ResourceError:
    if not code:
        code = ERR_CODE_EXPECTED
    if not message:
        message = "resource error"
    if resource:
        message = f"{message}: {resource}"
    return ResourceError(code, message, cause, resource)


def new_resource_not_found_error(
    resource: str = "",
    cause: BaseException | None = None,
) -> ResourceError:
    return new_resource_error(ERR_CODE_NOT_FOUND, "resource not found", resource, cause)


def new_resource_conflict_error(
    resource: str = "",
    cause: BaseException | None = None,
) -> ResourceError:
    return new_resource_error(ERR_CODE_CONFLICT, "resource conflict", resource, cause)


class InternalError(Error):
    def __init__(
        self,
        message: str = "internal error",
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(ERR_CODE_UNEXPECTED, message, cause)


def new_internal_failure(
    message: str = "",
    cause: BaseException | None = None,
) -> InternalError:
    return InternalError(message or "internal error", cause)


class RetryableError(Exception):
    __slots__ = ("cause",)

    def __init__(self, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.__cause__ = cause

    def __str__(self) -> str:
        return str(self.cause)


class UpstreamError(Exception):
    __slots__ = ("cause", "target", "operation", "request_id")

    def __init__(
        self,
        target: str = "",
        operation: str = "",
        request_id: str = "",
        cause: BaseException | None = None,
    ) -> None:
        super().__init__("upstream call failed")
        self.cause = cause
        if cause is not None:
            self.__cause__ = cause
        self.target = target
        self.operation = operation
        self.request_id = request_id

    def __str__(self) -> str:
        message = "upstream call failed"
        if self.operation and self.target:
            message = f"{message}: {self.operation} {self.target}"
        elif self.target:
            message = f"{message}: {self.target}"
        elif self.operation:
            message = f"{message}: {self.operation}"
        if self.cause is not None:
            return f"{message}: {self.cause}"
        return message


def new_upstream_error(
    target: str = "",
    operation: str = "",
    request_id: str = "",
    cause: BaseException | None = None,
) -> UpstreamError:
    return UpstreamError(target, operation, request_id, cause)


def new_internal_error(msg: str = "") -> InternalError:
    return InternalError(msg or "internal server error")


def new_validate_error(
    msg: str = "", items: list[ValidateErrItem] | None = None
) -> ValidationError:
    return new_validation_error(msg or "parameter validate failed", items)


def is_err_code(code: int, err: BaseException | None) -> bool:
    return err is not None and code_of(err) == code


def code_of(err: BaseException | None) -> int:
    if err is None:
        return 0

    cur: BaseException | None = err
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, Error) and cur.code:
            return cur.code
        cur = cur.__cause__ or cur.__context__
    return ERR_CODE_UNEXPECTED


def with_error_data(err: BaseException | None, data: Any) -> ExtraDataError | None:
    if err is None:
        return None
    return ExtraDataError(err, data)


def error_data(err: BaseException | None) -> Any:
    cur: BaseException | None = err
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, ExtraDataError):
            return cur.data
        cur = cur.__cause__ or cur.__context__
    return None


def with_retryable_error(err: BaseException | None) -> RetryableError | None:
    if err is None:
        return None
    return RetryableError(err)


def is_retryable_error(err: BaseException | None) -> bool:
    cur: BaseException | None = err
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, RetryableError):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def is_expected(err: BaseException | None) -> bool:
    return code_of(err) >= ERR_CODE_EXPECTED


def is_unexpected(err: BaseException | None) -> bool:
    return err is not None and not is_expected(err)


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
