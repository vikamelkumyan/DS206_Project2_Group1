"""
Logging setup for the dimensional data pipeline.

This project file is intentionally named logging.py to match the assignment.
"""

from __future__ import annotations

import logging
import os

from logging import (
    Filter,
    FileHandler,
    Formatter,
    INFO,
    StreamHandler,
    getLogger,
)

__all__ = [
    "getLogger",
    "Filter",
    "FileHandler",
    "Formatter",
    "INFO",
    "StreamHandler",
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

    project_root = os.path.dirname(os.path.abspath(__file__))
    if log_file_path is None:
        log_file_path = os.path.join(
            project_root,
            "logs",
            "logs_dimensional_data_pipeline.txt",
        )

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
