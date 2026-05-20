"""Logging facade that includes trace and request context."""

from __future__ import annotations

import logging as py_logging
from typing import Protocol

from core_py import context

LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_WARN = "WARN"
LOG_LEVEL_ERROR = "ERROR"
LOG_LEVEL_FATAL = "FATAL"


class Logger(Protocol):
    def ctx_debugf(self, ctx: context.Context | None, msg: str, *args: object) -> None: ...
    def ctx_infof(self, ctx: context.Context | None, msg: str, *args: object) -> None: ...
    def ctx_warnf(self, ctx: context.Context | None, msg: str, *args: object) -> None: ...
    def ctx_errorf(self, ctx: context.Context | None, msg: str, *args: object) -> None: ...
    def ctx_fatalf(self, ctx: context.Context | None, msg: str, *args: object) -> None: ...
    def debugf(self, msg: str, *args: object) -> None: ...
    def infof(self, msg: str, *args: object) -> None: ...
    def warnf(self, msg: str, *args: object) -> None: ...
    def errorf(self, msg: str, *args: object) -> None: ...
    def fatalf(self, msg: str, *args: object) -> None: ...


def _format(msg: str, args: tuple[object, ...]) -> str:
    return msg % args if args else msg


def _trace_msg(ctx: context.Context | None) -> str:
    trace_id, ok_trace = context.ctx_get_trace_id(ctx)
    request_id, ok_req = context.ctx_get_request_id(ctx)
    return f"trace_id: {trace_id if ok_trace else '-'} request_id: {request_id if ok_req else '-'}"


class StdoutLogger:
    def __init__(self, logger: py_logging.Logger | None = None) -> None:
        self._logger = logger or py_logging.getLogger("core_py")
        if not self._logger.handlers:
            handler = py_logging.StreamHandler()
            handler.setFormatter(py_logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self._logger.addHandler(handler)
        self._logger.setLevel(py_logging.INFO)

    def _log(
        self, level: int, level_name: str, ctx: context.Context | None, msg: str, *args: object
    ) -> None:
        self._logger.log(
            level, "%s level: %s msg: %s", _trace_msg(ctx), level_name, _format(msg, args)
        )

    def ctx_debugf(self, ctx: context.Context | None, msg: str, *args: object) -> None:
        self._log(py_logging.DEBUG, LOG_LEVEL_DEBUG, ctx, msg, *args)

    def ctx_infof(self, ctx: context.Context | None, msg: str, *args: object) -> None:
        self._log(py_logging.INFO, LOG_LEVEL_INFO, ctx, msg, *args)

    def ctx_warnf(self, ctx: context.Context | None, msg: str, *args: object) -> None:
        self._log(py_logging.WARNING, LOG_LEVEL_WARN, ctx, msg, *args)

    def ctx_errorf(self, ctx: context.Context | None, msg: str, *args: object) -> None:
        self._log(py_logging.ERROR, LOG_LEVEL_ERROR, ctx, msg, *args)

    def ctx_fatalf(self, ctx: context.Context | None, msg: str, *args: object) -> None:
        self._log(py_logging.CRITICAL, LOG_LEVEL_FATAL, ctx, msg, *args)
        raise SystemExit(1)

    def debugf(self, msg: str, *args: object) -> None:
        self.ctx_debugf(None, msg, *args)

    def infof(self, msg: str, *args: object) -> None:
        self.ctx_infof(None, msg, *args)

    def warnf(self, msg: str, *args: object) -> None:
        self.ctx_warnf(None, msg, *args)

    def errorf(self, msg: str, *args: object) -> None:
        self.ctx_errorf(None, msg, *args)

    def fatalf(self, msg: str, *args: object) -> None:
        self.ctx_fatalf(None, msg, *args)


_default_logger: Logger = StdoutLogger()


def get_logger() -> Logger:
    return _default_logger


def set_logger(logger: Logger) -> None:
    global _default_logger
    _default_logger = logger


def ctx_debugf(ctx: context.Context | None, msg: str, *args: object) -> None:
    _default_logger.ctx_debugf(ctx, msg, *args)


def ctx_infof(ctx: context.Context | None, msg: str, *args: object) -> None:
    _default_logger.ctx_infof(ctx, msg, *args)


def ctx_warnf(ctx: context.Context | None, msg: str, *args: object) -> None:
    _default_logger.ctx_warnf(ctx, msg, *args)


def ctx_errorf(ctx: context.Context | None, msg: str, *args: object) -> None:
    _default_logger.ctx_errorf(ctx, msg, *args)


def ctx_fatalf(ctx: context.Context | None, msg: str, *args: object) -> None:
    _default_logger.ctx_fatalf(ctx, msg, *args)


def debugf(msg: str, *args: object) -> None:
    _default_logger.debugf(msg, *args)


def infof(msg: str, *args: object) -> None:
    _default_logger.infof(msg, *args)


def warnf(msg: str, *args: object) -> None:
    _default_logger.warnf(msg, *args)


def errorf(msg: str, *args: object) -> None:
    _default_logger.errorf(msg, *args)


def fatalf(msg: str, *args: object) -> None:
    _default_logger.fatalf(msg, *args)


# Go-style aliases.
GetLogger = get_logger
SetLogger = set_logger
NewStdoutLogger = StdoutLogger
CtxDebugf = ctx_debugf
CtxInfof = ctx_infof
CtxWarnf = ctx_warnf
CtxErrorf = ctx_errorf
CtxFatalf = ctx_fatalf
Debugf = debugf
Infof = infof
Warnf = warnf
Errorf = errorf
Fatalf = fatalf
