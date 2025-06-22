"""
PySpark DataFrame Profiler

A library for generating comprehensive profiles of PySpark DataFrames with statistics
for all columns including null counts, data type specific metrics, and performance optimizations.
"""

from .api import analyze
from .exceptions import (
    ProfilingError,
    InvalidDataError,
    SamplingError,
    StatisticsError,
    ConfigurationError,
    SparkOperationError,
    DataTypeError,
    ColumnNotFoundError,
)
from .sampling import SamplingConfig
from .logging import configure_logging, set_log_level, disable_logging, get_logger

__version__ = "4.4.0"
__all__ = [
    "analyze",
    # Exceptions
    "ProfilingError",
    "InvalidDataError",
    "SamplingError",
    "StatisticsError",
    "ConfigurationError",
    "SparkOperationError",
    "DataTypeError",
    "ColumnNotFoundError",
    # Sampling and Logging
    "SamplingConfig",
    "configure_logging",
    "set_log_level",
    "disable_logging",
    "get_logger",
]
