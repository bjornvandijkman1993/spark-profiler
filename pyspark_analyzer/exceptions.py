"""
Custom exceptions for the pyspark-analyzer library.

This module defines domain-specific exceptions to provide better error handling
and more meaningful error messages for users.
"""


class ProfilingError(Exception):
    """Base exception for all profiling-related errors."""

    pass


class InvalidDataError(ProfilingError):
    """Raised when input data is invalid or corrupted."""

    pass


class SamplingError(ProfilingError):
    """Raised when sampling operations fail."""

    pass


class StatisticsError(ProfilingError):
    """Raised when statistics computation fails."""

    pass


class ConfigurationError(ProfilingError):
    """Raised when configuration is invalid."""

    pass


class SparkOperationError(ProfilingError):
    """Raised when Spark operations fail."""

    def __init__(self, message: str, original_exception: Exception | None = None):
        super().__init__(message)
        self.original_exception = original_exception


class DataTypeError(InvalidDataError):
    """Raised when data type operations fail."""

    pass


class ColumnNotFoundError(InvalidDataError):
    """Raised when specified columns are not found in the DataFrame."""

    def __init__(self, columns: list, available_columns: list):
        self.missing_columns = columns
        self.available_columns = available_columns
        message = (
            f"Columns not found in DataFrame: {columns}. "
            f"Available columns: {available_columns[:10]}{'...' if len(available_columns) > 10 else ''}"
        )
        super().__init__(message)
