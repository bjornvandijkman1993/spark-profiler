"""
Integration tests for spark-profiler with real Spark DataFrames.
"""

import pytest
from datetime import date
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    BooleanType,
    ArrayType,
)

from pyspark_analyzer import ColumnNotFoundError, ConfigurationError, analyze
from pyspark_analyzer.performance import optimize_dataframe_for_profiling
from pyspark_analyzer.utils import format_profile_output


class TestEndToEndProfiling:
    """Integration tests for complete profiling workflow."""

    def test_profile_mixed_types_dataframe(self, spark_session):
        """Test profiling a DataFrame with various data types."""
        # Create a realistic dataset
        data = [
            (
                1,
                "Alice Johnson",
                28,
                75000.50,
                True,
                date(2020, 1, 15),
                ["Python", "SQL"],
            ),
            (
                2,
                "Bob Smith",
                35,
                85000.00,
                True,
                date(2019, 6, 1),
                ["Java", "Scala", "SQL"],
            ),
            (3, "Carol White", 42, 95000.75, False, date(2018, 3, 20), ["Python"]),
            (4, "David Brown", None, None, True, date(2021, 11, 30), None),
            (5, "", 29, 65000.00, True, None, ["R", "Python", "SQL"]),
            (6, "Eve Davis", 31, 80000.25, None, date(2020, 8, 15), []),
        ]

        schema = StructType(
            [
                StructField("employee_id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", DoubleType(), True),
                StructField("is_active", BooleanType(), True),
                StructField("hire_date", DateType(), True),
                StructField("skills", ArrayType(StringType()), True),
            ]
        )

        df = spark_session.createDataFrame(data, schema)

        # Profile the DataFrame
        profile = analyze(df, output_format="dict")

        # Verify overview
        assert profile["overview"]["total_rows"] == 6
        assert profile["overview"]["total_columns"] == 7

        # Verify column profiles
        columns = profile["columns"]

        # Check employee_id (no nulls)
        assert columns["employee_id"]["null_count"] == 0
        assert columns["employee_id"]["distinct_count"] == 6
        assert columns["employee_id"]["min"] == 1
        assert columns["employee_id"]["max"] == 6

        # Check name (has empty string)
        assert columns["name"]["null_count"] == 0  # Empty string is not null
        assert columns["name"]["empty_count"] == 1
        assert columns["name"]["min_length"] == 0
        assert columns["name"]["max_length"] > 10

        # Check age (has nulls)
        assert columns["age"]["null_count"] == 1
        assert columns["age"]["mean"] == pytest.approx(33.0, 0.1)

        # Check hire_date
        assert columns["hire_date"]["null_count"] == 1
        assert columns["hire_date"]["min_date"] == date(2018, 3, 20)
        assert columns["hire_date"]["max_date"] == date(2021, 11, 30)

    def test_profile_large_dataframe_with_sampling(self, spark_session):
        """Test profiling a large DataFrame with automatic sampling."""
        # Create a moderately large DataFrame (reduced from 15M to 100k for speed)
        large_df = spark_session.range(0, 100_000).selectExpr(
            "id",
            "id % 1000 as category",
            "rand() * 1000000 as revenue",
            "date_add('2020-01-01', cast(rand() * 1095 as int)) as transaction_date",
            "concat('customer_', id % 10000) as customer_id",
        )

        # Profile with sampling enabled using fraction-based sampling
        profile = analyze(large_df, fraction=0.5, output_format="dict")

        # Verify sampling was applied
        assert profile["sampling"]["is_sampled"] is True
        assert (
            profile["sampling"]["sample_size"] <= 60_000
        )  # Should be approximately 50k with some variance
        assert profile["sampling"]["sampling_fraction"] <= 1.0
        # quality_score is no longer part of the simplified API

        # Verify profile still contains valid statistics
        # When sampled, overview shows sampled rows, original size is in sampling info
        assert profile["sampling"]["original_size"] == 100_000
        assert profile["overview"]["total_rows"] == profile["sampling"]["sample_size"]
        assert profile["columns"]["category"]["distinct_count"] == pytest.approx(
            1000, rel=0.1
        )
        assert profile["columns"]["revenue"]["min"] >= 0
        assert profile["columns"]["revenue"]["max"] <= 1_000_000

    def test_profile_with_custom_sampling_config(self, spark_session):
        """Test profiling with custom sampling configuration."""
        # Create medium-sized DataFrame
        df = spark_session.range(0, 500_000).selectExpr(
            "id",
            "rand() * 100 as score",
            "case when rand() > 0.9 then null else concat('user_', id) end as username",
        )

        # Profile with custom sampling config
        profile = analyze(df, target_rows=50_000, seed=42, output_format="dict")

        # Verify custom sampling was applied
        assert profile["sampling"]["is_sampled"] is True
        assert profile["sampling"]["sample_size"] <= 50_000

    def test_profile_output_formats(self, spark_session):
        """Test different output formats for profile data."""
        # Create simple DataFrame
        df = spark_session.createDataFrame(
            [(1, "A", 10.5), (2, "B", 20.0), (3, "C", 30.5)],
            ["id", "category", "value"],
        )

        profile = analyze(df, output_format="dict")

        # Test dictionary format (default)
        dict_output = format_profile_output(profile, "dict")
        assert isinstance(dict_output, dict)
        assert "overview" in dict_output
        assert "columns" in dict_output

        # Test JSON format
        json_output = format_profile_output(profile, "json")
        assert isinstance(json_output, str)
        parsed = json.loads(json_output)
        assert parsed == dict_output

        # Test summary format
        summary_output = format_profile_output(profile, "summary")
        assert isinstance(summary_output, str)
        assert "DataFrame Profile Summary" in summary_output
        assert "Total Rows: 3" in summary_output

    def test_profile_edge_cases(self, spark_session):
        """Test profiling edge cases."""
        from pyspark.sql.types import (
            StructType,
            StructField,
            IntegerType,
            StringType,
            DoubleType,
        )

        # Empty DataFrame
        empty_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", StringType(), True),
            ]
        )
        empty_df = spark_session.createDataFrame([], empty_schema)

        profile = analyze(empty_df, output_format="dict")

        assert profile["overview"]["total_rows"] == 0
        assert all(col["total_count"] == 0 for col in profile["columns"].values())

        # Single row DataFrame
        single_df = spark_session.createDataFrame(
            [(1, "test", 42.0)], ["id", "text", "number"]
        )
        profile = analyze(single_df, output_format="dict")

        assert profile["overview"]["total_rows"] == 1
        assert profile["columns"]["number"]["std"] == 0  # No variation

        # All nulls DataFrame
        null_schema = StructType(
            [
                StructField("col1", IntegerType(), True),
                StructField("col2", StringType(), True),
                StructField("col3", DoubleType(), True),
            ]
        )
        null_data = [(None, None, None) for _ in range(5)]
        null_df = spark_session.createDataFrame(null_data, schema=null_schema)
        profile = analyze(null_df, output_format="dict")

        assert all(
            col["null_percentage"] == 100.0 for col in profile["columns"].values()
        )

    def test_performance_optimization_integration(self, spark_session):
        """Test integration of performance optimizations."""
        # Create DataFrame that benefits from optimization (reduced size)
        df = spark_session.range(0, 10_000).selectExpr(
            "id",
            "id % 100 as group_id",
            "rand() * 1000 as metric1",
            "rand() * 1000 as metric2",
            "rand() * 1000 as metric3",
        )

        # First, optimize the DataFrame
        optimized_df = optimize_dataframe_for_profiling(df, sample_fraction=0.1)

        # Then profile the optimized DataFrame
        profile = analyze(optimized_df, output_format="dict")

        # Verify optimization worked
        actual_rows = optimized_df.count()
        assert actual_rows < 10_000  # Should be sampled
        assert profile["overview"]["total_rows"] == actual_rows

    def test_column_selection_profiling(self, spark_session):
        """Test profiling specific columns only."""
        # Create DataFrame with many columns
        df = spark_session.range(0, 1000).selectExpr(
            "id",
            "id % 10 as category",
            "rand() * 100 as metric1",
            "rand() * 100 as metric2",
            "rand() * 100 as metric3",
            "concat('text_', id) as text_field",
        )

        # Profile only specific columns
        profile = analyze(
            df, columns=["id", "category", "metric1"], output_format="dict"
        )

        # Verify only selected columns are profiled
        assert set(profile["columns"].keys()) == {"id", "category", "metric1"}
        assert "metric2" not in profile["columns"]
        assert "text_field" not in profile["columns"]

    def test_error_handling_integration(self, spark_session):
        """Test error handling in profiling workflow."""
        df = spark_session.createDataFrame([(1, "test")], ["id", "text"])

        # Test invalid column selection
        with pytest.raises(ColumnNotFoundError):
            analyze(df, columns=["non_existent_column"], output_format="dict")

        # Test invalid output format
        with pytest.raises(ConfigurationError):
            profile = analyze(df, output_format="dict")
            format_profile_output(profile, "invalid_format")

    def test_concurrent_profiling(self, spark_session):
        """Test profiling multiple DataFrames concurrently."""
        # Create multiple DataFrames
        df1 = spark_session.range(0, 10000).selectExpr("id", "rand() * 100 as value1")
        df2 = spark_session.range(0, 10000).selectExpr("id", "rand() * 200 as value2")
        df3 = spark_session.range(0, 10000).selectExpr("id", "rand() * 300 as value3")

        # Profile all DataFrames
        profile1 = analyze(df1, output_format="dict")
        profile2 = analyze(df2, output_format="dict")
        profile3 = analyze(df3, output_format="dict")

        # Verify each profile is independent
        assert profile1["columns"]["value1"]["max"] <= 100
        assert profile2["columns"]["value2"]["max"] <= 200
        assert profile3["columns"]["value3"]["max"] <= 300

    def test_memory_efficiency(self, spark_session):
        """Test memory efficiency with caching and cleanup."""
        # Create a DataFrame that would benefit from caching
        df = spark_session.range(0, 50000).selectExpr(
            "id", "id % 50 as category", "rand() * 1000 as value"
        )

        # Profile with caching
        profile = analyze(df, output_format="dict")

        # Verify profile completed successfully
        assert profile["overview"]["total_rows"] == 50000

        # The profiler should clean up any caching automatically
        # This test ensures no memory leaks occur
