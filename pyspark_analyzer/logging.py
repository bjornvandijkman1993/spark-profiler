"""Logging configuration for pyspark-analyzer."""

import logging
import os
from typing import Optional


_PACKAGE_NAME = "pyspark_analyzer"
_DEFAULT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_LOG_LEVEL_ENV_VAR = "PYSPARK_ANALYZER_LOG_LEVEL"


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given module name.

    Args:
        name: Module name, typically __name__

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


def set_log_level(level: str) -> None:
    """Set the log level for all pyspark-analyzer loggers.

    Args:
        level: Log level name (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")

    logger = logging.getLogger(_PACKAGE_NAME)
    logger.setLevel(numeric_level)

    for handler in logger.handlers:
        handler.setLevel(numeric_level)


def configure_logging(
    level: Optional[str] = None,
    format_string: Optional[str] = None,
    disable_existing_loggers: bool = False,
) -> None:
    """Configure logging for the pyspark-analyzer package.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               If None, uses environment variable or defaults to WARNING.
        format_string: Custom format string for log messages.
                      If None, uses default format.
        disable_existing_loggers: Whether to disable existing loggers.
    """
    if level is None:
        level = os.environ.get(_LOG_LEVEL_ENV_VAR, "WARNING")

    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")

    if format_string is None:
        format_string = _DEFAULT_FORMAT

    logger = logging.getLogger(_PACKAGE_NAME)
    logger.setLevel(numeric_level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(numeric_level)
        formatter = logging.Formatter(format_string)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if not disable_existing_loggers:
        for existing_logger in logging.Logger.manager.loggerDict.values():
            if isinstance(
                existing_logger, logging.Logger
            ) and existing_logger.name.startswith(_PACKAGE_NAME):
                existing_logger.setLevel(numeric_level)


def disable_logging() -> None:
    """Disable all logging from pyspark-analyzer."""
    logger = logging.getLogger(_PACKAGE_NAME)
    logger.setLevel(logging.CRITICAL + 1)


configure_logging()
