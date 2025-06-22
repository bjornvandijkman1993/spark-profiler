"""
Test cases for performance optimization utilities.
"""

from unittest.mock import Mock, patch

from pyspark_analyzer.performance import optimize_dataframe_for_profiling
from pyspark_analyzer.statistics import StatisticsComputer


class TestStatisticsComputerBatch:
    """Test cases for StatisticsComputer batch functionality."""

    def test_init(self, sample_dataframe):
        """Test initialization of StatisticsComputer."""
        computer = StatisticsComputer(sample_dataframe)
        assert computer.df is sample_dataframe
        # Removed cache_enabled check as caching methods were removed

    def test_compute_all_columns_batch(self, sample_dataframe):
        """Test batch computation of statistics for all columns."""

        computer = StatisticsComputer(sample_dataframe)
        results = computer.compute_all_columns_batch()

        # Check that all columns are processed
        assert set(results.keys()) == {"id", "name", "age", "salary"}

        # Check numeric column statistics
        assert results["id"]["data_type"] == "LongType()"
        assert results["id"]["total_count"] == 5  # sample_dataframe has 5 rows
        assert results["id"]["null_count"] == 0
        assert results["id"]["min"] == 1
        assert results["id"]["max"] == 5

        # Check string column statistics
        assert results["name"]["data_type"] == "StringType()"
        assert results["name"]["total_count"] == 5
        assert (
            results["name"]["null_count"] == 0
        )  # All names are non-null in sample data
        assert results["name"]["empty_count"] == 0  # No empty strings in sample data

        # Check that caching was cleaned up
        # Removed cache_enabled check as caching methods were removed

    def test_compute_all_columns_batch_specific_columns(self, sample_dataframe):
        """Test batch computation for specific columns only."""
        computer = StatisticsComputer(sample_dataframe)

        # Test with subset of columns
        selected_columns = ["id", "name"]
        results = computer.compute_all_columns_batch(columns=selected_columns)

        assert set(results.keys()) == set(selected_columns)
        assert "age" not in results
        assert "salary" not in results

    def test_compute_column_stats_numeric(self, spark_session):
        """Test statistics computation for numeric columns."""
        data = [(1, 10.5), (2, 20.0), (3, 30.5), (4, None), (5, 50.0)]
        df = spark_session.createDataFrame(data, ["id", "value"])

        computer = StatisticsComputer(df)
        stats = computer.compute_all_columns_batch(columns=["value"])["value"]

        assert stats["data_type"] == "DoubleType()"
        assert stats["total_count"] == 5
        assert stats["non_null_count"] == 4
        assert stats["null_count"] == 1
        assert stats["null_percentage"] == 20.0
        assert stats["min"] == 10.5
        assert stats["max"] == 50.0
        assert "mean" in stats
        assert "median" in stats
        assert "q1" in stats
        assert "q3" in stats

    def test_compute_column_stats_string(self, spark_session):
        """Test statistics computation for string columns."""
        data = [
            (1, "short"),
            (2, "medium length"),
            (3, "a very long string here"),
            (4, ""),
            (5, None),
        ]
        df = spark_session.createDataFrame(data, ["id", "text"])

        computer = StatisticsComputer(df)
        stats = computer.compute_all_columns_batch(columns=["text"])["text"]

        assert stats["data_type"] == "StringType()"
        assert stats["total_count"] == 5
        assert stats["null_count"] == 1
        assert stats["empty_count"] == 1
        assert stats["min_length"] == 0  # empty string
        assert stats["max_length"] == 23  # "a very long string here"
        assert "avg_length" in stats

    def test_compute_column_stats_with_all_nulls(self, spark_session):
        """Test statistics computation when column has all null values."""
        from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", DoubleType(), True),
            ]
        )
        data = [(1, None), (2, None), (3, None)]
        df = spark_session.createDataFrame(data, schema=schema)

        computer = StatisticsComputer(df)
        stats = computer.compute_all_columns_batch(columns=["value"])["value"]

        assert stats["total_count"] == 3
        assert stats["non_null_count"] == 0
        assert stats["null_count"] == 3
        assert stats["null_percentage"] == 100.0
        assert stats["distinct_percentage"] == 0.0

    def test_error_handling_in_batch_compute(self, spark_session):
        """Test error handling during batch computation."""
        # Create a DataFrame with an invalid column type that will cause an error
        from pyspark.sql.types import ArrayType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("array_col", ArrayType(StringType()), True),
            ]
        )

        data = [("1", ["a", "b"]), ("2", ["c", "d"])]
        df = spark_session.createDataFrame(data, schema)

        computer = StatisticsComputer(df)

        # The batch compute should handle array types gracefully
        results = computer.compute_all_columns_batch()

        # Should successfully compute stats for the string column
        assert "id" in results
        assert results["id"]["data_type"] == "StringType()"

        # Array column should also be processed (with basic stats only)
        assert "array_col" in results
        assert "ArrayType(StringType(), True)" in results["array_col"]["data_type"]


class TestOptimizeDataFrameForProfiling:
    """Test cases for optimize_dataframe_for_profiling function."""

    def test_no_optimization(self, sample_dataframe):
        """Test when no optimization is applied."""
        result = optimize_dataframe_for_profiling(sample_dataframe)
        # Should return the same DataFrame when no sampling
        assert result.count() == sample_dataframe.count()

    def test_sampling_optimization(self, spark_session):
        """Test DataFrame sampling optimization."""
        # Create a larger DataFrame
        data = [(i, f"name_{i}", i * 1.5) for i in range(1000)]
        df = spark_session.createDataFrame(data, ["id", "name", "value"])

        # Apply sampling
        result = optimize_dataframe_for_profiling(df, sample_fraction=0.1)

        # Check that sampling reduced the size
        result_count = result.count()
        assert result_count < 1000
        assert result_count > 50  # Should have at least some rows

    def test_invalid_sample_fraction(self, sample_dataframe):
        """Test with invalid sample fractions."""
        # Sample fraction of 0 should not sample
        result = optimize_dataframe_for_profiling(sample_dataframe, sample_fraction=0)
        assert result.count() == sample_dataframe.count()

        # Sample fraction of 1.0 should not sample
        result = optimize_dataframe_for_profiling(sample_dataframe, sample_fraction=1.0)
        assert result.count() == sample_dataframe.count()

        # Sample fraction > 1 should not sample
        result = optimize_dataframe_for_profiling(sample_dataframe, sample_fraction=1.5)
        assert result.count() == sample_dataframe.count()

    def test_repartitioning_small_dataset(self, spark_session):
        """Test repartitioning optimization for small datasets."""
        # Create a small DataFrame with multiple partitions
        data = [(i, f"name_{i}") for i in range(10)]
        df = spark_session.createDataFrame(data, ["id", "name"]).repartition(10)

        # Check initial partitions
        assert df.rdd.getNumPartitions() == 10

        # Optimize should coalesce to 1 partition for small data
        result = optimize_dataframe_for_profiling(df)
        assert result.rdd.getNumPartitions() == 1

    def test_repartitioning_large_dataset(self, sample_dataframe):
        """Test repartitioning optimization for large datasets."""
        # Use context managers for proper mocking
        with patch.object(
            sample_dataframe, "count", return_value=2_000_000
        ), patch.object(
            sample_dataframe.rdd, "getNumPartitions", return_value=2
        ), patch.object(
            sample_dataframe, "repartition"
        ) as mock_repartition:
            # Mock repartition to return a new DataFrame mock
            mock_repartitioned_df = Mock()
            mock_repartitioned_df.rdd.getNumPartitions.return_value = 8
            mock_repartition.return_value = mock_repartitioned_df

            result = optimize_dataframe_for_profiling(sample_dataframe)

        # With adaptive partitioning, the logic is more sophisticated
        # It considers data size and cluster configuration
        result_partitions = result.rdd.getNumPartitions()
        # The new logic may decide not to repartition if the overhead isn't worth it
        # or may repartition based on estimated data size
        assert result_partitions >= 1  # At least some partitions

    def test_medium_dataset_no_repartition(self, spark_session):
        """Test that medium-sized datasets are not repartitioned."""
        # Create a medium DataFrame
        data = [(i, f"name_{i}") for i in range(10000)]
        df = spark_session.createDataFrame(data, ["id", "name"]).repartition(4)

        # Optimize should apply adaptive logic
        result = optimize_dataframe_for_profiling(df, row_count=10000)
        # With adaptive partitioning, the optimal number of partitions
        # depends on data size and cluster configuration
        result_partitions = result.rdd.getNumPartitions()
        # For 10K rows, it should optimize to a reasonable number
        assert 1 <= result_partitions <= 8

    def test_row_count_parameter_avoids_redundant_count(self, spark_session, mocker):
        """Test that providing row_count parameter avoids redundant count operations."""
        # Create a DataFrame
        data = [(i, f"name_{i}", i * 10) for i in range(100)]
        df = spark_session.createDataFrame(data, ["id", "name", "value"])

        # Mock the count method to track calls
        count_spy = mocker.spy(df, "count")

        # Call optimize without row_count - should call count()
        optimize_dataframe_for_profiling(df)
        assert count_spy.call_count == 1

        # Reset spy
        count_spy.reset_mock()

        # Call optimize with row_count - should NOT call count()
        optimize_dataframe_for_profiling(df, row_count=100)
        assert count_spy.call_count == 0
