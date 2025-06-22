"""
PySpark DataFrame Profiler

A library for generating comprehensive profiles of PySpark DataFrames with statistics
for all columns including null counts, data type specific metrics, and performance optimizations.

Requirements:
    - PySpark >= 3.0.0
    - Python >= 3.8
"""

from .api import analyze
from .exceptions import (
    ColumnNotFoundError,
    ConfigurationError,
    DataTypeError,
    InvalidDataError,
    ProfilingError,
    SamplingError,
    SparkOperationError,
    StatisticsError,
)
from .logging import configure_logging, disable_logging, get_logger, set_log_level
from .progress import ProgressTracker, track_progress
from .sampling import SamplingConfig

__version__ = "5.0.2"
__all__ = [
    "ColumnNotFoundError",
    "ConfigurationError",
    "DataTypeError",
    "InvalidDataError",
    "ProfilingError",
    "ProgressTracker",
    "SamplingConfig",
    "SamplingError",
    "SparkOperationError",
    "StatisticsError",
    "analyze",
    "configure_logging",
    "disable_logging",
    "get_logger",
    "set_log_level",
    "track_progress",
]
