"""
Test cases for the DataFrame profiler.
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from datetime import datetime

from spark_profiler import DataFrameProfiler
from spark_profiler.statistics import StatisticsComputer
from spark_profiler.utils import get_column_data_types, format_profile_output


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    return SparkSession.builder.appName("TestProfiler").getOrCreate()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )

    data = [
        (1, "Alice", 100.5),
        (2, "Bob", 200.0),
        (3, None, 150.75),
        (4, "Charlie", None),
        (5, "", 175.25),
    ]

    return spark_session.createDataFrame(data, schema)


class TestDataFrameProfiler:
    """Test cases for DataFrameProfiler class."""

    def test_init_with_valid_dataframe(self, sample_dataframe):
        """Test profiler initialization with valid DataFrame."""
        profiler = DataFrameProfiler(sample_dataframe)
        assert profiler.df == sample_dataframe
        assert len(profiler.column_types) == 3

    def test_init_with_invalid_input(self):
        """Test profiler initialization with invalid input."""
        with pytest.raises(TypeError):
            DataFrameProfiler("not_a_dataframe")

    def test_profile_all_columns(self, sample_dataframe):
        """Test profiling all columns."""
        profiler = DataFrameProfiler(sample_dataframe)
        profile = profiler.profile()

        assert "overview" in profile
        assert "columns" in profile
        assert len(profile["columns"]) == 3

        # Check overview
        overview = profile["overview"]
        assert overview["total_rows"] == 5
        assert overview["total_columns"] == 3

        # Check column profiles exist
        assert "id" in profile["columns"]
        assert "name" in profile["columns"]
        assert "value" in profile["columns"]

    def test_profile_specific_columns(self, sample_dataframe):
        """Test profiling specific columns."""
        profiler = DataFrameProfiler(sample_dataframe)
        profile = profiler.profile(columns=["id", "name"])

        assert len(profile["columns"]) == 2
        assert "id" in profile["columns"]
        assert "name" in profile["columns"]
        assert "value" not in profile["columns"]

    def test_profile_invalid_columns(self, sample_dataframe):
        """Test profiling with invalid column names."""
        profiler = DataFrameProfiler(sample_dataframe)

        with pytest.raises(ValueError, match="Columns not found"):
            profiler.profile(columns=["nonexistent_column"])

    def test_numeric_column_stats(self, sample_dataframe):
        """Test statistics for numeric columns."""
        profiler = DataFrameProfiler(sample_dataframe)
        profile = profiler.profile(columns=["id", "value"])

        # Check numeric stats for 'id' column
        id_stats = profile["columns"]["id"]
        assert "min" in id_stats
        assert "max" in id_stats
        assert "mean" in id_stats
        assert "std" in id_stats
        assert "median" in id_stats

        # Check numeric stats for 'value' column (has nulls)
        value_stats = profile["columns"]["value"]
        assert "min" in value_stats
        assert "max" in value_stats
        assert value_stats["null_count"] > 0

    def test_string_column_stats(self, sample_dataframe):
        """Test statistics for string columns."""
        profiler = DataFrameProfiler(sample_dataframe)
        profile = profiler.profile(columns=["name"])

        name_stats = profile["columns"]["name"]
        assert "min_length" in name_stats
        assert "max_length" in name_stats
        assert "avg_length" in name_stats
        assert "empty_count" in name_stats
        assert name_stats["empty_count"] == 1  # One empty string


class TestStatisticsComputer:
    """Test cases for StatisticsComputer class."""

    def test_basic_stats_computation(self, sample_dataframe):
        """Test basic statistics computation."""
        computer = StatisticsComputer(sample_dataframe)
        stats = computer.compute_basic_stats("id")

        assert "total_count" in stats
        assert "non_null_count" in stats
        assert "null_count" in stats
        assert "null_percentage" in stats
        assert "distinct_count" in stats
        assert "distinct_percentage" in stats

        assert stats["total_count"] == 5
        assert stats["non_null_count"] == 5  # id column has no nulls
        assert stats["null_count"] == 0

    def test_numeric_stats_computation(self, sample_dataframe):
        """Test numeric statistics computation."""
        computer = StatisticsComputer(sample_dataframe)
        stats = computer.compute_numeric_stats("value")

        required_keys = ["min", "max", "mean", "std", "median", "q1", "q3"]
        for key in required_keys:
            assert key in stats

    def test_string_stats_computation(self, sample_dataframe):
        """Test string statistics computation."""
        computer = StatisticsComputer(sample_dataframe)
        stats = computer.compute_string_stats("name")

        required_keys = ["min_length", "max_length", "avg_length", "empty_count"]
        for key in required_keys:
            assert key in stats

        assert stats["empty_count"] == 1  # One empty string


class TestUtils:
    """Test cases for utility functions."""

    def test_get_column_data_types(self, sample_dataframe):
        """Test column data type extraction."""
        column_types = get_column_data_types(sample_dataframe)

        assert len(column_types) == 3
        assert "id" in column_types
        assert "name" in column_types
        assert "value" in column_types

        assert str(column_types["id"]) == "IntegerType()"
        assert str(column_types["name"]) == "StringType()"
        assert str(column_types["value"]) == "DoubleType()"

    def test_format_profile_output_dict(self, sample_dataframe):
        """Test dictionary format output."""
        profiler = DataFrameProfiler(sample_dataframe)
        profile = profiler.profile()

        formatted = format_profile_output(profile, format_type="dict")
        assert formatted == profile

    def test_format_profile_output_json(self, sample_dataframe):
        """Test JSON format output."""
        profiler = DataFrameProfiler(sample_dataframe)
        profile = profiler.profile()

        formatted = format_profile_output(profile, format_type="json")
        assert isinstance(formatted, str)
        assert "overview" in formatted
        assert "columns" in formatted

    def test_format_profile_output_summary(self, sample_dataframe):
        """Test summary format output."""
        profiler = DataFrameProfiler(sample_dataframe)
        profile = profiler.profile()

        formatted = format_profile_output(profile, format_type="summary")
        assert isinstance(formatted, str)
        assert "DataFrame Profile Summary" in formatted
        assert "Total Rows:" in formatted
        assert "Column:" in formatted

    def test_format_profile_output_invalid_format(self, sample_dataframe):
        """Test invalid format type."""
        profiler = DataFrameProfiler(sample_dataframe)
        profile = profiler.profile()

        with pytest.raises(ValueError, match="Unsupported format type"):
            format_profile_output(profile, format_type="invalid_format")


if __name__ == "__main__":
    pytest.main([__file__])
