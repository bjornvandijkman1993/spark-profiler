"""
Tests for the simplified analyze() API.
"""

import pytest
import pandas as pd
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from datetime import datetime

from pyspark_analyzer import analyze


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    data = [
        (1, "Alice", 25, 50000.0, datetime(2021, 1, 1)),
        (2, "Bob", 30, 60000.0, datetime(2021, 2, 1)),
        (3, None, 35, 70000.0, datetime(2021, 3, 1)),
        (4, "David", None, 80000.0, datetime(2021, 4, 1)),
        (5, "Eve", 40, None, None),
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("join_date", TimestampType(), True),
        ]
    )

    return spark_session.createDataFrame(data, schema)


class TestAnalyzeAPI:
    """Test the analyze() function API."""

    def test_basic_usage(self, sample_dataframe):
        """Test basic usage of analyze() function."""
        result = analyze(sample_dataframe)

        # Should return pandas DataFrame by default
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5  # 5 columns
        assert "id" in result["column_name"].values
        assert "age" in result["column_name"].values

    def test_output_formats(self, sample_dataframe):
        """Test different output formats."""
        # Dictionary format
        dict_result = analyze(sample_dataframe, output_format="dict")
        assert isinstance(dict_result, dict)
        assert "overview" in dict_result
        assert "columns" in dict_result
        assert "sampling" in dict_result

        # JSON format
        json_result = analyze(sample_dataframe, output_format="json")
        assert isinstance(json_result, str)
        parsed = json.loads(json_result)
        assert "overview" in parsed

        # Summary format
        summary_result = analyze(sample_dataframe, output_format="summary")
        assert isinstance(summary_result, str)
        assert "DataFrame Profile Summary" in summary_result

        # Pandas format (explicit)
        pandas_result = analyze(sample_dataframe, output_format="pandas")
        assert isinstance(pandas_result, pd.DataFrame)

    def test_column_selection(self, sample_dataframe):
        """Test profiling specific columns."""
        result = analyze(sample_dataframe, columns=["age", "salary"])
        assert len(result) == 2
        assert "age" in result["column_name"].values
        assert "salary" in result["column_name"].values
        assert "name" not in result["column_name"].values

    def test_sampling_options(self, sample_dataframe):
        """Test various sampling options."""
        # Disable sampling
        result = analyze(sample_dataframe, sampling=False, output_format="dict")
        assert result["sampling"]["is_sampled"] is False

        # Enable sampling (though won't actually sample for small dataset)
        result = analyze(sample_dataframe, sampling=True, output_format="dict")
        assert "is_sampled" in result["sampling"]

        # Target rows
        result = analyze(sample_dataframe, target_rows=3, output_format="dict")
        assert result["sampling"]["is_sampled"] is True
        assert result["sampling"]["sample_size"] <= 3

        # Fraction
        result = analyze(sample_dataframe, fraction=0.5, output_format="dict")
        assert result["sampling"]["is_sampled"] is True

    def test_advanced_statistics_control(self, sample_dataframe):
        """Test control over advanced statistics."""
        # With advanced statistics
        result_with = analyze(
            sample_dataframe, include_advanced=True, output_format="dict"
        )
        age_stats = result_with["columns"]["age"]
        assert "skewness" in age_stats
        assert "kurtosis" in age_stats

        # Without advanced statistics
        result_without = analyze(
            sample_dataframe, include_advanced=False, output_format="dict"
        )
        age_stats = result_without["columns"]["age"]
        assert "skewness" not in age_stats
        assert "kurtosis" not in age_stats

    def test_quality_metrics_control(self, sample_dataframe):
        """Test control over quality metrics."""
        # With quality metrics
        result_with = analyze(
            sample_dataframe, include_quality=True, output_format="dict"
        )
        assert "quality" in result_with["columns"]["age"]

        # Without quality metrics
        result_without = analyze(
            sample_dataframe, include_quality=False, output_format="dict"
        )
        assert "quality" not in result_without["columns"]["age"]

    def test_optimization_flag(self, sample_dataframe):
        """Test optimization for large datasets flag."""
        # Just verify it runs without error
        result = analyze(sample_dataframe, output_format="dict")
        assert "overview" in result

    def test_invalid_parameters(self, sample_dataframe):
        """Test error handling for invalid parameters."""
        from pyspark_analyzer import ConfigurationError, ColumnNotFoundError

        # Both target_rows and fraction
        with pytest.raises(ConfigurationError, match="Cannot specify both"):
            analyze(sample_dataframe, target_rows=100, fraction=0.5)

        # Invalid columns
        with pytest.raises(ColumnNotFoundError) as exc_info:
            analyze(sample_dataframe, columns=["nonexistent"])
        assert "nonexistent" in str(exc_info.value)

        # Invalid output format
        with pytest.raises(ConfigurationError):
            analyze(sample_dataframe, output_format="invalid")

    def test_reproducible_sampling(self, sample_dataframe):
        """Test that sampling with seed is reproducible."""
        result1 = analyze(sample_dataframe, fraction=0.5, seed=42, output_format="dict")

        result2 = analyze(sample_dataframe, fraction=0.5, seed=42, output_format="dict")

        # Should get same sample size
        assert result1["sampling"]["sample_size"] == result2["sampling"]["sample_size"]

    def test_empty_dataframe(self, spark_session):
        """Test handling of empty DataFrame."""
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("value", DoubleType(), True),
            ]
        )
        empty_df = spark_session.createDataFrame([], schema)

        result = analyze(empty_df, output_format="dict")
        assert result["overview"]["total_rows"] == 0
        assert result["columns"]["id"]["total_count"] == 0

    def test_null_handling(self, sample_dataframe):
        """Test that nulls are properly handled."""
        result = analyze(sample_dataframe, output_format="dict")

        # Check null counts
        assert result["columns"]["name"]["null_count"] == 1
        assert result["columns"]["age"]["null_count"] == 1
        assert result["columns"]["salary"]["null_count"] == 1
        assert result["columns"]["join_date"]["null_count"] == 1

    def test_pandas_output_structure(self, sample_dataframe):
        """Test the structure of pandas output."""
        result = analyze(sample_dataframe)

        # Check expected columns exist
        expected_columns = [
            "data_type",
            "total_count",
            "non_null_count",
            "null_count",
            "null_percentage",
            "distinct_count",
        ]
        for col in expected_columns:
            assert col in result.columns

        # Check numeric columns have additional stats
        assert "mean" in result.columns
        assert "min" in result.columns
        assert "max" in result.columns

    def test_integration_with_real_data(self, spark_session):
        """Test with a more realistic dataset."""
        # Create a larger dataset
        data = []
        for i in range(1000):
            data.append(
                (
                    i,
                    f"User_{i % 100}",
                    20 + (i % 50),
                    30000.0 + (i * 100),
                    datetime(2020, 1 + (i % 12), 1 + (i % 28)),
                )
            )

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("username", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", DoubleType(), True),
                StructField("created_at", TimestampType(), True),
            ]
        )

        df = spark_session.createDataFrame(data, schema)

        # Profile with different options
        result = analyze(
            df,
            target_rows=500,
            include_advanced=True,
            include_quality=True,
            output_format="dict",
        )

        # Verify sampling occurred
        assert result["sampling"]["is_sampled"] is True
        # Allow for some variation in sampling
        assert 400 <= result["sampling"]["sample_size"] <= 600

        # Verify all columns profiled
        assert len(result["columns"]) == 5

        # Verify advanced stats present
        assert "skewness" in result["columns"]["age"]
        assert "outliers" in result["columns"]["salary"]

        # Verify quality metrics present
        assert "quality" in result["columns"]["username"]
