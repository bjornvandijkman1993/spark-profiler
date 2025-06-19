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

__version__ = "3.2.0"
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
]
