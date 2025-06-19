"""
Test cases for statistics computation module.
"""

import pytest
from datetime import datetime, date
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    DoubleType,
    TimestampType,
)

from pyspark_analyzer.statistics import StatisticsComputer


# LazyRowCount class has been removed from the library


class TestStatisticsComputer:
    """Test cases for StatisticsComputer class."""

    def test_init(self, sample_dataframe):
        """Test initialization of StatisticsComputer."""
        computer = StatisticsComputer(sample_dataframe)
        assert computer.df is sample_dataframe

    def test_get_total_rows(self, spark_session):
        """Test total rows calculation."""
        data = [(1, "a"), (2, "b"), (3, "c")]
        df = spark_session.createDataFrame(data, ["id", "value"])

        computer = StatisticsComputer(df)

        # Test total rows computation
        total = computer._get_total_rows()
        assert total == 3

    def test_compute_basic_stats(self, sample_dataframe):
        """Test basic statistics computation with real data."""
        computer = StatisticsComputer(sample_dataframe)
        stats = computer.compute_basic_stats("name")

        # sample_dataframe has 5 rows, all names are non-null and distinct
        assert stats["total_count"] == 5
        assert stats["non_null_count"] == 5
        assert stats["null_count"] == 0
        assert stats["null_percentage"] == 0.0
        assert stats["distinct_count"] == 5  # All names are unique
        assert stats["distinct_percentage"] == 100.0

    def test_compute_basic_stats_all_nulls(self, spark_session):
        """Test basic statistics when all values are null."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        data = [(1, None), (2, None), (3, None)]
        df = spark_session.createDataFrame(data, schema)

        computer = StatisticsComputer(df)
        stats = computer.compute_basic_stats("value")

        assert stats["total_count"] == 3
        assert stats["non_null_count"] == 0
        assert stats["null_count"] == 3
        assert stats["null_percentage"] == 100.0
        assert stats["distinct_count"] == 0
        assert stats["distinct_percentage"] == 0.0

    def test_compute_basic_stats_no_nulls(self, spark_session):
        """Test basic statistics when there are no null values."""
        data = [(i, f"value_{i}") for i in range(100)]
        df = spark_session.createDataFrame(data, ["id", "value"])

        computer = StatisticsComputer(df)
        stats = computer.compute_basic_stats("value")

        assert stats["total_count"] == 100
        assert stats["non_null_count"] == 100
        assert stats["null_count"] == 0
        assert stats["null_percentage"] == 0.0
        # Allow for approximation error (approx_count_distinct has 5% error)
        assert 95 <= stats["distinct_count"] <= 105  # All unique with tolerance
        assert stats["distinct_percentage"] >= 95.0  # Allow for approximation

    def test_compute_numeric_stats(self, sample_dataframe):
        """Test numeric statistics computation with real data."""
        computer = StatisticsComputer(sample_dataframe)
        stats = computer.compute_numeric_stats("salary")

        # salary column: [50000.0, 60000.0, 70000.0, 55000.0, 65000.0]
        assert stats["min"] == 50000.0
        assert stats["max"] == 70000.0
        assert stats["mean"] == 60000.0
        # For quartiles, we'll just check they exist and are reasonable
        assert "median" in stats
        assert "q1" in stats
        assert "q3" in stats
        assert stats["q1"] <= stats["median"] <= stats["q3"]
        assert "std" in stats
        assert stats["std"] > 0

    def test_compute_numeric_stats_with_nulls(self, spark_session):
        """Test numeric statistics with null values."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", DoubleType(), True),
            ]
        )
        data = [(1, 10.0), (2, 20.0), (3, None), (4, 40.0), (5, None)]
        df = spark_session.createDataFrame(data, schema)

        computer = StatisticsComputer(df)
        stats = computer.compute_numeric_stats("value")

        # Statistics should be computed on non-null values only
        assert stats["min"] == 10.0
        assert stats["max"] == 40.0
        assert stats["mean"] == pytest.approx(23.33, 0.01)  # (10+20+40)/3

    def test_compute_numeric_stats_single_value(self, spark_session):
        """Test numeric statistics when all values are the same."""
        data = [(i, 42.0) for i in range(5)]
        df = spark_session.createDataFrame(data, ["id", "value"])

        computer = StatisticsComputer(df)
        stats = computer.compute_numeric_stats("value")

        assert stats["min"] == 42.0
        assert stats["max"] == 42.0
        assert stats["mean"] == 42.0
        assert stats["median"] == 42.0
        assert stats["std"] == 0.0  # No variation

    def test_compute_string_stats(self, sample_dataframe):
        """Test string statistics computation with real data."""
        computer = StatisticsComputer(sample_dataframe)
        stats = computer.compute_string_stats("name")

        # name column: ["Alice", "Bob", "Charlie", "David", "Eve"]
        # lengths: [5, 3, 7, 5, 3]
        assert stats["min_length"] == 3  # Bob, Eve
        assert stats["max_length"] == 7  # Charlie
        assert stats["avg_length"] == pytest.approx(4.6, 0.1)  # (5+3+7+5+3)/5 = 4.6
        assert stats["empty_count"] == 0  # No empty strings

    def test_compute_string_stats_all_empty(self, spark_session):
        """Test string statistics when all values are empty strings."""
        data = [(1, ""), (2, ""), (3, "")]
        df = spark_session.createDataFrame(data, ["id", "text"])

        computer = StatisticsComputer(df)
        stats = computer.compute_string_stats("text")

        assert stats["min_length"] == 0
        assert stats["max_length"] == 0
        assert stats["avg_length"] == 0.0
        assert stats["empty_count"] == 3

    def test_compute_string_stats_with_nulls(self, spark_session):
        """Test string statistics with null values."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("text", StringType(), True),
            ]
        )
        data = [(1, "hello"), (2, None), (3, "world"), (4, None)]
        df = spark_session.createDataFrame(data, schema)

        computer = StatisticsComputer(df)
        stats = computer.compute_string_stats("text")

        # Length functions should handle nulls
        assert stats["min_length"] == 5  # Both "hello" and "world" have length 5
        assert stats["max_length"] == 5
        assert stats["avg_length"] == 5.0
        assert stats["empty_count"] == 0

    def test_compute_temporal_stats_dates(self, spark_session):
        """Test temporal statistics for date columns."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("date_col", DateType(), True),
            ]
        )
        data = [
            (1, date(2023, 1, 1)),
            (2, date(2023, 6, 15)),
            (3, date(2023, 12, 31)),
            (4, None),
        ]
        df = spark_session.createDataFrame(data, schema)

        computer = StatisticsComputer(df)
        stats = computer.compute_temporal_stats("date_col")

        assert stats["min_date"] == date(2023, 1, 1)
        assert stats["max_date"] == date(2023, 12, 31)
        assert stats["date_range_days"] == 364  # Days between Jan 1 and Dec 31

    def test_compute_temporal_stats_timestamps(self, spark_session):
        """Test temporal statistics for timestamp columns."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("timestamp_col", TimestampType(), True),
            ]
        )
        data = [
            (1, datetime(2023, 1, 1, 0, 0)),
            (2, datetime(2023, 1, 2, 12, 0)),
            (3, datetime(2023, 1, 3, 23, 59)),
            (4, None),
        ]
        df = spark_session.createDataFrame(data, schema)

        computer = StatisticsComputer(df)
        stats = computer.compute_temporal_stats("timestamp_col")

        assert stats["min_date"] == datetime(2023, 1, 1, 0, 0)
        assert stats["max_date"] == datetime(2023, 1, 3, 23, 59)
        assert stats["date_range_days"] == 2  # 3 days span

    def test_compute_temporal_stats_all_nulls(self, spark_session):
        """Test temporal statistics when all values are null."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("date_col", DateType(), True),
            ]
        )
        data = [(1, None), (2, None), (3, None)]
        df = spark_session.createDataFrame(data, schema)

        computer = StatisticsComputer(df)
        stats = computer.compute_temporal_stats("date_col")

        assert stats["min_date"] is None
        assert stats["max_date"] is None
        assert stats["date_range_days"] is None

    def test_compute_temporal_stats_same_date(self, spark_session):
        """Test temporal statistics when all dates are the same."""
        data = [
            (1, date(2023, 6, 15)),
            (2, date(2023, 6, 15)),
            (3, date(2023, 6, 15)),
        ]
        df = spark_session.createDataFrame(data, ["id", "date_col"])

        computer = StatisticsComputer(df)
        stats = computer.compute_temporal_stats("date_col")

        assert stats["min_date"] == date(2023, 6, 15)
        assert stats["max_date"] == date(2023, 6, 15)
        assert stats["date_range_days"] == 0  # Same date

    def test_empty_dataframe(self, spark_session):
        """Test statistics computation on empty DataFrame."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        df = spark_session.createDataFrame([], schema)

        computer = StatisticsComputer(df)

        # Basic stats should handle empty DataFrame
        stats = computer.compute_basic_stats("value")
        assert stats["total_count"] == 0
        assert stats["non_null_count"] == 0
        assert stats["null_count"] == 0
        assert stats["null_percentage"] == 0.0
        assert stats["distinct_count"] == 0
        assert stats["distinct_percentage"] == 0.0
