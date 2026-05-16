"""
Logging setup for the dimensional data pipeline.

This project file is intentionally named logging.py to match the assignment.
It re-exports Python's standard logging module so third-party imports of
``logging`` continue to work, then adds the project-specific logger factory.
"""

from __future__ import annotations

import importlib.util
import os
import sysconfig


_STDLIB_LOGGING_DIR = os.path.join(sysconfig.get_path("stdlib"), "logging")
_STDLIB_LOGGING_INIT = os.path.join(_STDLIB_LOGGING_DIR, "__init__.py")
_SPEC = importlib.util.spec_from_file_location(
    "_stdlib_logging",
    _STDLIB_LOGGING_INIT,
    submodule_search_locations=[_STDLIB_LOGGING_DIR],
)
_stdlib_logging = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(_stdlib_logging)

for _name in dir(_stdlib_logging):
    if _name not in {"__name__", "__package__", "__spec__", "__loader__", "__file__"}:
        globals()[_name] = getattr(_stdlib_logging, _name)

__path__ = [_STDLIB_LOGGING_DIR]
__all__ = list(getattr(_stdlib_logging, "__all__", [])) + [
    "ExecutionIdFilter",
    "setup_dimensional_logger",
]


class ExecutionIdFilter(Filter):
    """Attach the pipeline execution_id to every log record."""

    def __init__(self, execution_id: str):
        super().__init__()
        self.execution_id = execution_id

    def filter(self, record):
        record.execution_id = self.execution_id
        return True


def setup_dimensional_logger(execution_id: str, log_file_path: str | None = None):
    """Return a logger that writes dimensional flow logs to file and console."""

    if log_file_path is None:
        from pipeline_dimensional_data.config import LOG_FILE_PATH

        log_file_path = LOG_FILE_PATH

    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger = getLogger(f"dimensional_data_flow.{execution_id}")
    logger.setLevel(INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    execution_filter = ExecutionIdFilter(execution_id)
    formatter = Formatter(
        "%(asctime)s | %(levelname)s | execution_id=%(execution_id)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(INFO)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(execution_filter)

    stream_handler = StreamHandler()
    stream_handler.setLevel(INFO)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(execution_filter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
