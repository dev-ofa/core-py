"""Logging facade that injects trace and request context via stdlib logging."""

from __future__ import annotations

import logging as py_logging
from typing import Protocol

from core_py import context

LOG_LEVEL_FATAL = "FATAL"


class Logger(Protocol):
    def debug(self, msg: str, *args: object) -> None: ...
    def info(self, msg: str, *args: object) -> None: ...
    def warning(self, msg: str, *args: object) -> None: ...
    def error(self, msg: str, *args: object) -> None: ...
    def critical(self, msg: str, *args: object) -> None: ...


def _trace_value() -> str:
    trace_id, ok = context.get_trace_id()
    return trace_id if ok else "-"


def _request_value() -> str:
    request_id, ok = context.get_request_id()
    return request_id if ok else "-"


class _ContextLoggerAdapter(py_logging.LoggerAdapter):
    def process(self, msg: object, kwargs: dict[str, object]) -> tuple[object, dict[str, object]]:
        extra = kwargs.get("extra")
        merged_extra = dict(extra) if isinstance(extra, dict) else {}
        merged_extra.setdefault("trace_id", _trace_value())
        merged_extra.setdefault("request_id", _request_value())
        kwargs["extra"] = merged_extra
        return msg, kwargs


class _ContextFormatter(py_logging.Formatter):
    def format(self, record: py_logging.LogRecord) -> str:
        if not hasattr(record, "trace_id"):
            record.trace_id = "-"
        if not hasattr(record, "request_id"):
            record.request_id = "-"
        if not hasattr(record, "core_py_level"):
            record.core_py_level = LOG_LEVEL_FATAL if record.levelno == py_logging.CRITICAL else record.levelname
        return super().format(record)


class StdoutLogger:
    def __init__(self, logger: py_logging.Logger | None = None) -> None:
        self._logger = logger or py_logging.getLogger("core_py")
        if not self._logger.handlers:
            handler = py_logging.StreamHandler()
            handler.setFormatter(
                _ContextFormatter(
                    "%(asctime)s %(levelname)s trace_id: %(trace_id)s request_id: %(request_id)s "
                    "level: %(core_py_level)s msg: %(message)s"
                )
            )
            self._logger.addHandler(handler)
        if logger is None:
            self._logger.setLevel(py_logging.INFO)
        self._adapter = _ContextLoggerAdapter(self._logger, {})

    def _log(self, level: int, msg: str, *args: object) -> None:
        self._adapter.log(level, msg, *args)

    def debug(self, msg: str, *args: object) -> None:
        self._log(py_logging.DEBUG, msg, *args)

    def info(self, msg: str, *args: object) -> None:
        self._log(py_logging.INFO, msg, *args)

    def warning(self, msg: str, *args: object) -> None:
        self._log(py_logging.WARNING, msg, *args)

    def error(self, msg: str, *args: object) -> None:
        self._log(py_logging.ERROR, msg, *args)

    def critical(self, msg: str, *args: object) -> None:
        self._log(py_logging.CRITICAL, msg, *args)


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
