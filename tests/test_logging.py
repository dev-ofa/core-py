import logging as py_logging

from core_py import context, logging


class _FakeLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []

    def debug(self, msg: str, *args: object) -> None:
        self.calls.append(("debug", msg, args))

    def info(self, msg: str, *args: object) -> None:
        self.calls.append(("info", msg, args))

    def warning(self, msg: str, *args: object) -> None:
        self.calls.append(("warning", msg, args))

    def error(self, msg: str, *args: object) -> None:
        self.calls.append(("error", msg, args))

    def critical(self, msg: str, *args: object) -> None:
        self.calls.append(("critical", msg, args))


def test_stdout_logger_critical_logs_without_exiting(caplog) -> None:
    logger = py_logging.getLogger("core_py_test_logging")
    logger.handlers.clear()

    with context.use_context():
        context.set_trace_id("trace-log")
        context.set_request_id("req-log")
        with caplog.at_level(py_logging.CRITICAL, logger="core_py_test_logging"):
            logging.StdoutLogger(logger).critical("fatal %s", "boom")

    record = caplog.records[-1]
    assert record.trace_id == "trace-log"
    assert record.request_id == "req-log"
    assert record.levelname == "CRITICAL"
    assert record.getMessage() == "fatal boom"


def test_stdout_logger_preserves_explicit_logger_level(caplog) -> None:
    logger = py_logging.getLogger("core_py_test_logging_debug")
    logger.handlers.clear()
    logger.setLevel(py_logging.DEBUG)

    with caplog.at_level(py_logging.DEBUG, logger="core_py_test_logging_debug"):
        logging.StdoutLogger(logger).debug("debug %s", "message")

    record = caplog.records[-1]
    assert logger.level == py_logging.DEBUG
    assert record.trace_id == "-"
    assert record.request_id == "-"
    assert record.levelname == "DEBUG"
    assert record.getMessage() == "debug message"


def test_stdout_logger_default_formatter_renders_context_fields(capsys) -> None:
    logger = py_logging.getLogger("core_py_test_logging_format")
    logger.handlers.clear()
    logger.setLevel(py_logging.INFO)

    with context.use_context():
        context.set_trace_id("trace-format")
        context.set_request_id("req-format")
        logging.StdoutLogger(logger).info("hello %s", "world")

    captured = capsys.readouterr()
    assert "trace_id: trace-format request_id: req-format" in captured.err
    assert "level: INFO msg: hello world" in captured.err


def test_set_logger_and_module_facade_delegate_to_custom_logger() -> None:
    fake = _FakeLogger()
    previous = logging.get_logger()
    try:
        logging.set_logger(fake)
        assert logging.get_logger() is fake

        logging.debug("debug %s", "value")
        logging.info("info")
        logging.warning("warn")
        logging.error("error")
        logging.critical("fatal")
    finally:
        logging.set_logger(previous)

    assert fake.calls == [
        ("debug", "debug %s", ("value",)),
        ("info", "info", ()),
        ("warning", "warn", ()),
        ("error", "error", ()),
        ("critical", "fatal", ()),
    ]
