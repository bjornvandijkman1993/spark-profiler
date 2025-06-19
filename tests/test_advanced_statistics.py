"""
Tests for advanced statistics functionality.
"""

import pytest
from datetime import date
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    DateType,
)
import numpy as np

from pyspark_analyzer import analyze
from pyspark_analyzer.statistics import StatisticsComputer


class TestAdvancedNumericStatistics:
    """Test advanced numeric statistics computation."""

    def test_skewness_kurtosis(self, spark_session):
        """Test skewness and kurtosis computation."""
        # Create data with known distribution properties
        # Normal distribution should have skewness ~0 and kurtosis ~0
        np.random.seed(42)
        normal_data = [
            (i, float(x)) for i, x in enumerate(np.random.normal(0, 1, 1000))
        ]
        df = spark_session.createDataFrame(normal_data, ["id", "value"])

        computer = StatisticsComputer(df)
        stats = computer.compute_numeric_stats("value", advanced=True)

        # Check that skewness is close to 0 for normal distribution
        assert "skewness" in stats
        assert (
            abs(stats["skewness"]) < 0.5
        )  # Normal distribution should have low skewness

        # Check kurtosis
        assert "kurtosis" in stats
        assert (
            abs(stats["kurtosis"]) < 2.0
        )  # Normal distribution kurtosis should be close to 0

        # Check other advanced stats
        assert "variance" in stats
        assert "sum" in stats
        assert "cv" in stats  # Coefficient of variation
        assert "p5" in stats
        assert "p95" in stats
        assert "range" in stats
        assert "iqr" in stats

    def test_zero_negative_counts(self, spark_session):
        """Test zero and negative value counts."""
        data = [(1, -5.0), (2, 0.0), (3, 10.0), (4, -3.0), (5, 0.0), (6, 15.0)]
        df = spark_session.createDataFrame(data, ["id", "value"])

        computer = StatisticsComputer(df)
        stats = computer.compute_numeric_stats("value", advanced=True)

        assert stats["zero_count"] == 2
        assert stats["negative_count"] == 2

    def test_outlier_detection_iqr(self, spark_session):
        """Test IQR-based outlier detection."""
        # Create data with obvious outliers
        data = [(i, float(i)) for i in range(1, 11)]  # 1 to 10
        data.extend([(11, 100.0), (12, -50.0)])  # Outliers
        df = spark_session.createDataFrame(data, ["id", "value"])

        computer = StatisticsComputer(df)
        outlier_stats = computer.compute_outlier_stats("value", method="iqr")

        assert outlier_stats["method"] == "iqr"
        assert outlier_stats["outlier_count"] >= 2  # At least the two extreme values
        assert outlier_stats["lower_bound"] < 1
        assert outlier_stats["upper_bound"] > 10
        assert "outlier_percentage" in outlier_stats

    def test_outlier_detection_zscore(self, spark_session):
        """Test z-score based outlier detection."""
        # Create normal data with extreme outliers
        np.random.seed(42)
        data = [(i, float(x)) for i, x in enumerate(np.random.normal(0, 1, 100))]
        data.extend([(100, 10.0), (101, -10.0)])  # Extreme outliers (>3 std from mean)
        df = spark_session.createDataFrame(data, ["id", "value"])

        computer = StatisticsComputer(df)
        outlier_stats = computer.compute_outlier_stats("value", method="zscore")

        assert outlier_stats["method"] == "zscore"
        assert outlier_stats["outlier_count"] >= 2  # At least our inserted outliers
        assert outlier_stats["threshold"] == 3.0


class TestAdvancedStringStatistics:
    """Test advanced string statistics computation."""

    def test_pattern_detection(self, spark_session):
        """Test pattern detection in strings."""
        data = [
            (1, "user@example.com"),
            (2, "admin@test.org"),
            (3, "https://example.com"),
            (4, "http://test.org"),
            (5, "+1-234-567-8900"),
            (6, "555-123-4567"),
            (7, "12345"),
            (8, "UPPERCASE"),
            (9, "lowercase"),
            (10, "Mixed Case"),
        ]
        df = spark_session.createDataFrame(data, ["id", "text"])

        computer = StatisticsComputer(df)
        stats = computer.compute_string_stats("text", pattern_detection=True)

        assert "patterns" in stats
        patterns = stats["patterns"]
        assert patterns["email_count"] == 2
        assert patterns["url_count"] == 2
        assert patterns["phone_like_count"] >= 2
        assert patterns["numeric_string_count"] == 1
        # Note: uppercase_count includes strings with no lowercase letters (numbers, etc.)
        assert patterns["uppercase_count"] >= 1  # At least "UPPERCASE"
        assert patterns["lowercase_count"] >= 1  # At least "lowercase"

    def test_top_values(self, spark_session):
        """Test top frequent values computation."""
        data = [
            (1, "apple"),
            (2, "banana"),
            (3, "apple"),
            (4, "cherry"),
            (5, "banana"),
            (6, "apple"),
            (7, "banana"),
            (8, "date"),
        ]
        df = spark_session.createDataFrame(data, ["id", "fruit"])

        computer = StatisticsComputer(df)
        stats = computer.compute_string_stats("fruit", top_n=3)

        assert "top_values" in stats
        top_values = stats["top_values"]
        assert len(top_values) == 3

        # Check that apple and banana are in top 2
        top_2_fruits = {item["value"] for item in top_values[:2]}
        assert "apple" in top_2_fruits
        assert "banana" in top_2_fruits

        # Check counts
        value_counts = {item["value"]: item["count"] for item in top_values}
        assert value_counts["apple"] == 3
        assert value_counts["banana"] == 3

    def test_whitespace_detection(self, spark_session):
        """Test whitespace detection."""
        data = [
            (1, "normal"),
            (2, " leading"),
            (3, "trailing "),
            (4, " both "),
            (5, "no_space"),
        ]
        df = spark_session.createDataFrame(data, ["id", "text"])

        computer = StatisticsComputer(df)
        stats = computer.compute_string_stats("text")

        assert stats["has_whitespace_count"] == 3  # Items 2, 3, 4


class TestDataQualityStatistics:
    """Test data quality metrics computation."""

    def test_numeric_quality(self, spark_session):
        """Test quality metrics for numeric columns."""
        # Create data with various quality issues
        data = [
            (1, 10.0),
            (2, 20.0),
            (3, None),  # Missing
            (4, float("inf")),  # Infinity
            (5, float("nan")),  # NaN
            (6, 1000.0),  # Potential outlier
            (7, 30.0),
            (8, None),  # Missing
        ]
        df = spark_session.createDataFrame(data, ["id", "value"])

        computer = StatisticsComputer(df)
        quality = computer.compute_data_quality_stats("value", column_type="numeric")

        assert quality["completeness"] == 0.75  # 6/8 non-null
        assert quality["null_count"] == 2
        assert quality["nan_count"] == 1
        assert quality["infinity_count"] == 1
        assert "outlier_percentage" in quality
        assert "quality_score" in quality
        assert 0 <= quality["quality_score"] <= 1

    def test_string_quality(self, spark_session):
        """Test quality metrics for string columns."""
        data = [
            (1, "normal"),
            (2, ""),  # Empty
            (3, " "),  # Blank
            (4, None),  # Null
            (5, "café"),  # Non-ASCII
            (6, "a"),  # Single char
            (7, "normal2"),
        ]
        df = spark_session.createDataFrame(data, ["id", "text"])

        computer = StatisticsComputer(df)
        quality = computer.compute_data_quality_stats("text", column_type="string")

        assert quality["completeness"] == pytest.approx(
            6 / 7, rel=1e-6
        )  # 6 non-null out of 7
        assert quality["blank_count"] == 2  # Empty string and space
        assert quality["non_ascii_count"] == 1
        assert quality["single_char_count"] == 2  # "a" and " "
        assert "quality_score" in quality

    def test_id_column_quality(self, spark_session):
        """Test quality metrics for ID columns."""
        # ID column with duplicates
        data = [(1, "ID001"), (2, "ID002"), (3, "ID001"), (4, "ID003"), (5, None)]
        df = spark_session.createDataFrame(data, ["row", "user_id"])

        computer = StatisticsComputer(df)
        quality = computer.compute_data_quality_stats("user_id")

        # Quality score should be penalized for low uniqueness in ID column
        assert quality["uniqueness"] < 1.0  # Has duplicates
        assert quality["quality_score"] < quality["completeness"]  # Penalized


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
        assert len(quality_df) == 3  # 3 columns
