"""
Unit tests for pandas DataFrame output functionality.
"""

from datetime import UTC, datetime

import pandas as pd
import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pyspark_analyzer import analyze
from pyspark_analyzer.utils import format_profile_output


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("score", DoubleType(), True),
            StructField("created_at", TimestampType(), True),
        ]
    )

    data = [
        (1, "Alice", 85.5, datetime(2023, 1, 1, tzinfo=UTC)),
        (2, "Bob", None, datetime(2023, 1, 2, tzinfo=UTC)),
        (3, None, 92.0, datetime(2023, 1, 3, tzinfo=UTC)),
        (4, "Charlie", 78.5, None),
        (5, "David", 88.0, datetime(2023, 1, 5, tzinfo=UTC)),
    ]

    return spark_session.createDataFrame(data, schema)


class TestPandasOutput:
    """Test cases for pandas DataFrame output."""

    def test_default_output_is_pandas(self, sample_dataframe):
        """Test that default output format is pandas DataFrame."""
        result = analyze(sample_dataframe)

        assert isinstance(result, pd.DataFrame)
        assert not result.empty

    def test_pandas_output_structure(self, sample_dataframe):
        """Test the structure of pandas DataFrame output."""
        df = analyze(sample_dataframe, output_format="pandas")

        # Check DataFrame shape
        assert len(df) == 4  # 4 columns in sample data

        # Check required columns exist
        required_columns = [
            "column_name",
            "data_type",
            "total_count",
            "non_null_count",
            "null_count",
            "null_percentage",
            "distinct_count",
            "distinct_percentage",
        ]
        for col in required_columns:
            assert col in df.columns

        # Check column names
        assert list(df["column_name"]) == ["id", "name", "score", "created_at"]

    def test_pandas_output_metadata(self, sample_dataframe):
        """Test that metadata is stored in DataFrame.attrs."""
        df = analyze(sample_dataframe, output_format="pandas")

        # Check metadata exists
        assert "overview" in df.attrs
        assert "sampling" in df.attrs
        assert "profiling_timestamp" in df.attrs

        # Check overview metadata
        assert df.attrs["overview"]["total_rows"] == 5
        assert df.attrs["overview"]["total_columns"] == 4

        # Check sampling metadata
        assert df.attrs["sampling"]["is_sampled"] is False

        # Check timestamp
        assert isinstance(df.attrs["profiling_timestamp"], pd.Timestamp)

    def test_numeric_column_statistics(self, sample_dataframe):
        """Test that numeric columns have appropriate statistics."""
        df = analyze(sample_dataframe, output_format="pandas")

        # Find numeric columns
        numeric_rows = df[df["column_name"].isin(["id", "score"])]

        for _, row in numeric_rows.iterrows():
            assert "min" in row.index
            assert "max" in row.index
            assert "mean" in row.index
            assert "std" in row.index
            assert "median" in row.index

    def test_string_column_statistics(self, sample_dataframe):
        """Test that string columns have appropriate statistics."""
        df = analyze(sample_dataframe, output_format="pandas")

        # Find string column
        string_row = df[df["column_name"] == "name"].iloc[0]

        assert "min_length" in string_row.index
        assert "max_length" in string_row.index
        assert "avg_length" in string_row.index

    def test_temporal_column_statistics(self, sample_dataframe):
        """Test that temporal columns have appropriate statistics."""
        df = analyze(sample_dataframe, output_format="pandas")

        # Find temporal column
        temporal_row = df[df["column_name"] == "created_at"].iloc[0]

        assert "min_date" in temporal_row.index
        assert "max_date" in temporal_row.index
        assert "date_range_days" in temporal_row.index

    def test_format_profile_output_function(self, sample_dataframe):
        """Test the format_profile_output utility function."""
        profile_dict = analyze(sample_dataframe, output_format="dict")

        # Test pandas format
        df = format_profile_output(profile_dict, "pandas")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4

        # Test other formats still work
        assert isinstance(format_profile_output(profile_dict, "dict"), dict)
        assert isinstance(format_profile_output(profile_dict, "json"), str)
        assert isinstance(format_profile_output(profile_dict, "summary"), str)

    def test_convenience_methods(self, sample_dataframe, tmp_path):
        """Test saving profile outputs using pandas methods directly."""
        profile_df = analyze(sample_dataframe, output_format="pandas")

        # Test to_csv
        csv_path = tmp_path / "profile.csv"
        profile_df.to_csv(str(csv_path), index=False)
        assert csv_path.exists()

        # Read back and verify
        df_read = pd.read_csv(csv_path)
        assert len(df_read) == 4
        assert "column_name" in df_read.columns

        # Test to_parquet (skip if pyarrow not installed)
        parquet_path = tmp_path / "profile.parquet"
        try:
            profile_df.to_parquet(str(parquet_path))
            assert parquet_path.exists()
        except ImportError:
            # Skip parquet test if pyarrow not installed
            pass

        # Test different output formats
        dict_output = analyze(sample_dataframe, output_format="dict")
        assert isinstance(dict_output, dict)

        pandas_output = analyze(sample_dataframe, output_format="pandas")
        assert isinstance(pandas_output, pd.DataFrame)

    def test_column_order(self, sample_dataframe):
        """Test that columns are in the expected order."""
        df = analyze(sample_dataframe, output_format="pandas")

        # Check that column_name is first
        assert df.columns[0] == "column_name"

        # Check that data_type is second
        assert df.columns[1] == "data_type"

        # Check basic stats come before type-specific stats
        basic_cols = ["total_count", "non_null_count", "null_count", "null_percentage"]
        for col in basic_cols:
            assert col in df.columns[:8]  # Should be in first 8 columns

    def test_empty_dataframe(self, spark_session):
        """Test handling of empty DataFrame."""
        empty_df = spark_session.createDataFrame([], StructType([]))

        result = analyze(empty_df, output_format="pandas")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

        # Metadata should still exist
        assert "overview" in result.attrs
        assert result.attrs["overview"]["total_columns"] == 0

    def test_with_sampling(self, sample_dataframe):
        """Test pandas output with sampling enabled."""
        df = analyze(sample_dataframe, fraction=0.5, output_format="pandas")

        # Check that sampling info is in metadata
        assert df.attrs["sampling"]["is_sampled"] is True
        assert df.attrs["sampling"]["sampling_fraction"] == 0.5
        # quality_score is no longer part of the simplified API

    def test_null_handling(self, sample_dataframe):
        """Test that null values are handled correctly."""
        df = analyze(sample_dataframe, output_format="pandas")

        # Check null counts
        name_row = df[df["column_name"] == "name"].iloc[0]
        assert name_row["null_count"] == 1
        assert name_row["non_null_count"] == 4

        score_row = df[df["column_name"] == "score"].iloc[0]
        assert score_row["null_count"] == 1
        assert score_row["non_null_count"] == 4

    def test_explicit_format_parameter(self, sample_dataframe):
        """Test that explicit format parameter works correctly."""
        # Test each format explicitly
        pandas_result = analyze(sample_dataframe, output_format="pandas")
        assert isinstance(pandas_result, pd.DataFrame)

        dict_result = analyze(sample_dataframe, output_format="dict")
        assert isinstance(dict_result, dict)

        json_result = analyze(sample_dataframe, output_format="json")
        assert isinstance(json_result, str)

        summary_result = analyze(sample_dataframe, output_format="summary")
        assert isinstance(summary_result, str)
        assert "DataFrame Profile Summary" in summary_result
