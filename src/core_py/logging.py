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
    def debug(self, msg: str, *args: object) -> None: ...
    def info(self, msg: str, *args: object) -> None: ...
    def warning(self, msg: str, *args: object) -> None: ...
    def error(self, msg: str, *args: object) -> None: ...
    def critical(self, msg: str, *args: object) -> None: ...


def _format(msg: str, args: tuple[object, ...]) -> str:
    return msg % args if args else msg


def _trace_msg() -> str:
    trace_id, ok_trace = context.get_trace_id()
    request_id, ok_req = context.get_request_id()
    return f"trace_id: {trace_id if ok_trace else '-'} request_id: {request_id if ok_req else '-'}"


class StdoutLogger:
    def __init__(self, logger: py_logging.Logger | None = None) -> None:
        self._logger = logger or py_logging.getLogger("core_py")
        if not self._logger.handlers:
            handler = py_logging.StreamHandler()
            handler.setFormatter(py_logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self._logger.addHandler(handler)
        if logger is None:
            self._logger.setLevel(py_logging.INFO)

    def _log(self, level: int, level_name: str, msg: str, *args: object) -> None:
        self._logger.log(level, "%s level: %s msg: %s", _trace_msg(), level_name, _format(msg, args))

    def debug(self, msg: str, *args: object) -> None:
        self._log(py_logging.DEBUG, LOG_LEVEL_DEBUG, msg, *args)

    def info(self, msg: str, *args: object) -> None:
        self._log(py_logging.INFO, LOG_LEVEL_INFO, msg, *args)

    def warning(self, msg: str, *args: object) -> None:
        self._log(py_logging.WARNING, LOG_LEVEL_WARN, msg, *args)

    def error(self, msg: str, *args: object) -> None:
        self._log(py_logging.ERROR, LOG_LEVEL_ERROR, msg, *args)

    def critical(self, msg: str, *args: object) -> None:
        self._log(py_logging.CRITICAL, LOG_LEVEL_FATAL, msg, *args)


_default_logger: Logger = StdoutLogger()


def get_logger() -> Logger:
    return _default_logger


def set_logger(logger: Logger) -> None:
    global _default_logger
    _default_logger = logger

def debug(msg: str, *args: object) -> None:
    _default_logger.debug(msg, *args)


def info(msg: str, *args: object) -> None:
    _default_logger.info(msg, *args)


def warning(msg: str, *args: object) -> None:
    _default_logger.warning(msg, *args)


def error(msg: str, *args: object) -> None:
    _default_logger.error(msg, *args)


def critical(msg: str, *args: object) -> None:
    _default_logger.critical(msg, *args)
