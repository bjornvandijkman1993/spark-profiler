"""
Test cases for statistics computation module.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
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

    def test_compute_all_columns_batch(self, sample_dataframe):
        """Test the batch computation method that exists in the refactored code."""
        computer = StatisticsComputer(sample_dataframe)

        # Test computing statistics for all columns
        stats = computer.compute_all_columns_batch()

        # Should have statistics for all columns
        assert "id" in stats
        assert "name" in stats
        assert "age" in stats
        assert "salary" in stats

        # Check basic structure of results
        for col_name, col_stats in stats.items():
            assert "total_count" in col_stats
            assert "non_null_count" in col_stats
            assert "null_count" in col_stats
            assert "data_type" in col_stats

    def test_compute_all_columns_batch_specific_columns(self, sample_dataframe):
        """Test batch computation for specific columns."""
        computer = StatisticsComputer(sample_dataframe)

        # Test computing statistics for specific columns
        stats = computer.compute_all_columns_batch(columns=["name", "age"])

        # Should only have statistics for requested columns
        assert "name" in stats
        assert "age" in stats
        assert "salary" not in stats
        assert "hire_date" not in stats

    def test_compute_all_columns_batch_empty_dataframe(self, spark_session):
        """Test batch computation on empty DataFrame."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        df = spark_session.createDataFrame([], schema)

        computer = StatisticsComputer(df)

        # Should handle empty DataFrame gracefully
        stats = computer.compute_all_columns_batch()

        # Should have statistics for all columns even if empty
        assert "id" in stats
        assert "value" in stats

        # Check counts are zero
        assert stats["id"]["total_count"] == 0
        assert stats["value"]["total_count"] == 0
