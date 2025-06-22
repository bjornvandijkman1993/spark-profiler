"""
Tests for advanced statistics functionality.
"""

from datetime import date

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pyspark_analyzer import analyze


class TestProfilerIntegration:
    """Test integration of advanced statistics with analyze function."""

    def test_profile_with_advanced_stats(self, spark_session):
        """Test profiling with advanced statistics enabled."""
        data = [
            (1, "apple", 10.5, date(2023, 1, 1)),
            (2, "banana", 20.0, date(2023, 1, 2)),
            (3, "apple", 15.3, date(2023, 1, 3)),
            (4, None, 25.0, date(2023, 1, 4)),
            (5, "cherry", None, None),
        ]
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("fruit", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("date", DateType(), True),
            ]
        )
        df = spark_session.createDataFrame(data, schema)

        profile = analyze(df, output_format="dict", include_advanced=True)

        # Check numeric column has advanced stats
        price_stats = profile["columns"]["price"]
        assert "skewness" in price_stats
        assert "kurtosis" in price_stats
        assert "outliers" in price_stats
        assert "quality" in price_stats

        # Check string column has advanced stats
        fruit_stats = profile["columns"]["fruit"]
        assert "patterns" in fruit_stats
        assert "top_values" in fruit_stats
        assert "quality" in fruit_stats

    def test_quick_profile(self, spark_session):
        """Test quick profile without advanced stats."""
        data = [(i, f"value_{i}", float(i * 10)) for i in range(100)]
        df = spark_session.createDataFrame(data, ["id", "text", "value"])

        profile = analyze(
            df,
            output_format="dict",
            include_advanced=False,
            include_quality=False,
        )

        # Should have basic stats but not advanced ones
        value_stats = profile["columns"]["value"]
        assert "min" in value_stats
        assert "max" in value_stats
        assert "mean" in value_stats
        assert "skewness" not in value_stats  # Advanced stat
        assert "quality" not in value_stats  # Quality stat

    def test_quality_report(self, spark_session):
        """Test quality report generation."""
        data = [
            (1, "normal", 10.0),
            (2, None, 20.0),
            (3, "", None),
            (4, "test", float("inf")),
        ]
        df = spark_session.createDataFrame(data, ["id", "text", "value"])

        profile = analyze(
            df,
            output_format="dict",
            include_advanced=False,
            include_quality=True,
        )

        # Extract quality metrics into a summary (replicate quality_report functionality)
        import pandas as pd

        quality_data = []
        for col_name, col_stats in profile["columns"].items():
            if "quality" in col_stats:
                quality_info = {
                    "column": col_name,
                    "data_type": col_stats["data_type"],
                    **col_stats["quality"],
                }
                quality_data.append(quality_info)

        quality_df = pd.DataFrame(quality_data)

        # Should return a pandas DataFrame
        assert hasattr(quality_df, "columns")
        assert "column" in quality_df.columns
        assert "quality_score" in quality_df.columns
        assert "completeness" in quality_df.columns
