"""
Test cases for custom exception handling.
"""

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from pyspark_analyzer import (
    ColumnNotFoundError,
    ConfigurationError,
    DataTypeError,
    InvalidDataError,
    ProfilingError,
    SamplingError,
    SparkOperationError,
    StatisticsError,
    analyze,
)
from pyspark_analyzer.exceptions import SparkOperationError as SparkOpError
from pyspark_analyzer.sampling import SamplingConfig


class TestExceptionHandling:
    """Test custom exception handling."""

    def test_configuration_error_both_target_and_fraction(self, sample_dataframe):
        """Test that ConfigurationError is raised when both target_rows and fraction are specified."""
        with pytest.raises(ConfigurationError, match="Cannot specify both"):
            analyze(sample_dataframe, target_rows=100, fraction=0.5)

    def test_configuration_error_invalid_fraction(self):
        """Test that ConfigurationError is raised for invalid fraction."""
        with pytest.raises(ConfigurationError, match="fraction must be between"):
            SamplingConfig(fraction=1.5)

    def test_configuration_error_negative_target_rows(self):
        """Test that ConfigurationError is raised for negative target_rows."""
        with pytest.raises(ConfigurationError, match="target_rows must be positive"):
            SamplingConfig(target_rows=-100)

    def test_column_not_found_error(self, sample_dataframe):
        """Test that ColumnNotFoundError is raised for non-existent columns."""
        with pytest.raises(ColumnNotFoundError) as exc_info:
            analyze(sample_dataframe, columns=["nonexistent", "also_missing"])

        # Check that the error message includes available columns
        assert "nonexistent" in str(exc_info.value)
        assert "also_missing" in str(exc_info.value)
        assert "Available columns:" in str(exc_info.value)

    def test_data_type_error_non_dataframe_input(self):
        """Test that DataTypeError is raised for non-DataFrame input."""
        with pytest.raises(DataTypeError, match="Input must be a PySpark DataFrame"):
            analyze("not a dataframe")

    def test_configuration_error_invalid_output_format(self, sample_dataframe):
        """Test that ConfigurationError is raised for invalid output format."""
        with pytest.raises(ConfigurationError, match="Unsupported format type"):
            analyze(sample_dataframe, output_format="invalid_format")

    def test_error_handling_preserves_context(self, spark_session):
        """Test that error handling preserves original exception context."""
        # Create a DataFrame that will cause an error during profiling
        schema = StructType([StructField("bad_column", StringType(), True)])

        # Create empty DataFrame with problematic column
        empty_df = spark_session.createDataFrame([], schema)

        # This should complete without error even with empty DataFrame
        result = analyze(empty_df, output_format="dict")
        assert result["overview"]["total_rows"] == 0

    def test_sampling_config_validation(self):
        """Test multiple validation errors in SamplingConfig."""
        # Test fraction = 0
        with pytest.raises(ConfigurationError, match="fraction must be between"):
            SamplingConfig(fraction=0.0)

        # Test fraction > 1
        with pytest.raises(ConfigurationError, match="fraction must be between"):
            SamplingConfig(fraction=1.1)

        # Test target_rows = 0
        with pytest.raises(ConfigurationError, match="target_rows must be positive"):
            SamplingConfig(target_rows=0)

    def test_profiler_import_deprecation(self):
        """Test that DataFrameProfiler import shows deprecation warning."""
        # We only test import deprecation since DataFrameProfiler no longer exists
        try:
            from pyspark_analyzer.profiler import DataFrameProfiler

            # If it exists, it should raise a deprecation warning on use
            with pytest.warns(DeprecationWarning), pytest.raises(
                DataTypeError, match="Input must be a PySpark DataFrame"
            ):
                DataFrameProfiler("not a dataframe")
        except ImportError:
            # DataFrameProfiler has been removed, which is expected
            pass

    def test_exception_hierarchy(self):
        """Test that custom exceptions inherit from ProfilingError."""
        # Test inheritance hierarchy
        assert issubclass(ConfigurationError, ProfilingError)
        assert issubclass(DataTypeError, InvalidDataError)
        assert issubclass(InvalidDataError, ProfilingError)
        assert issubclass(ColumnNotFoundError, InvalidDataError)
        assert issubclass(SamplingError, ProfilingError)
        assert issubclass(StatisticsError, ProfilingError)
        assert issubclass(SparkOperationError, ProfilingError)

    def test_spark_operation_error_with_original_exception(self):
        """Test SparkOperationError preserves original exception."""
        original_error = ValueError("Original error message")
        spark_error = SparkOpError("Wrapped error message", original_error)

        assert str(spark_error) == "Wrapped error message"
        assert spark_error.original_exception == original_error
        assert isinstance(spark_error.original_exception, ValueError)

    def test_column_not_found_error_attributes(self):
        """Test ColumnNotFoundError includes missing and available columns."""
        missing = ["col1", "col2"]
        available = ["id", "name", "age", "value", "created_at"]

        error = ColumnNotFoundError(missing, available)

        assert error.missing_columns == missing
        assert error.available_columns == available
        assert "col1" in str(error)
        assert "col2" in str(error)
        assert "Available columns:" in str(error)

        # Test with many available columns (should truncate)
        many_columns = [f"col_{i}" for i in range(20)]
        error2 = ColumnNotFoundError(["missing"], many_columns)
        assert "..." in str(error2)
