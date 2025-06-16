"""
Statistics computation functions for DataFrame profiling.
"""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    when,
    min as spark_min,
    max as spark_max,
    mean,
    stddev,
    expr,
    length,
    approx_count_distinct,
    skewness,
    kurtosis,
    variance,
    sum as spark_sum,
    trim,
    upper,
    lower,
    desc,
    abs as spark_abs,
)
from .utils import escape_column_name


class StatisticsComputer:
    """Handles computation of various statistics for DataFrame columns."""

    def __init__(self, dataframe: DataFrame, total_rows: Optional[int] = None):
        """
        Initialize with a PySpark DataFrame.

        Args:
            dataframe: PySpark DataFrame to compute statistics for
            total_rows: Cached row count to avoid recomputation
        """
        self.df = dataframe
        self.total_rows: Optional[int] = (
            total_rows  # Use provided count or lazy evaluation
        )

    def _get_total_rows(self) -> int:
        """Get total row count (cached for performance)."""
        if self.total_rows is None:
            self.total_rows = self.df.count()
        assert self.total_rows is not None  # Type narrowing for mypy
        return self.total_rows

    def compute_basic_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Compute basic statistics for any column type.

        Args:
            column_name: Name of the column

        Returns:
            Dictionary with basic statistics
        """
        total_rows = self._get_total_rows()

        # Single aggregation for efficiency - optimized for large datasets
        escaped_name = escape_column_name(column_name)
        result = self.df.agg(
            count(col(escaped_name)).alias("non_null_count"),
            count(when(col(escaped_name).isNull(), 1)).alias("null_count"),
            approx_count_distinct(col(escaped_name), rsd=0.05).alias(
                "distinct_count"
            ),  # 5% relative error for speed
        ).collect()[0]

        non_null_count = result["non_null_count"]
        null_count = result["null_count"]
        distinct_count = result["distinct_count"]

        return {
            "total_count": total_rows,
            "non_null_count": non_null_count,
            "null_count": null_count,
            "null_percentage": (
                (null_count / total_rows * 100) if total_rows > 0 else 0.0
            ),
            "distinct_count": distinct_count,
            "distinct_percentage": (
                (distinct_count / non_null_count * 100) if non_null_count > 0 else 0.0
            ),
        }

    def compute_numeric_stats(
        self, column_name: str, advanced: bool = True
    ) -> Dict[str, Any]:
        """
        Compute statistics specific to numeric columns.

        Args:
            column_name: Name of the numeric column
            advanced: Whether to compute advanced statistics (default: True)

        Returns:
            Dictionary with numeric statistics
        """
        # Build aggregation list dynamically for performance
        agg_list = [
            spark_min(col(column_name)).alias("min_value"),
            spark_max(col(column_name)).alias("max_value"),
            mean(col(column_name)).alias("mean_value"),
            stddev(col(column_name)).alias("std_value"),
            expr(f"percentile_approx({column_name}, 0.5)").alias("median_value"),
            expr(f"percentile_approx({column_name}, 0.25)").alias("q1_value"),
            expr(f"percentile_approx({column_name}, 0.75)").alias("q3_value"),
        ]

        if advanced:
            # Add advanced statistics in the same aggregation for efficiency
            agg_list.extend(
                [
                    skewness(col(column_name)).alias("skewness_value"),
                    kurtosis(col(column_name)).alias("kurtosis_value"),
                    variance(col(column_name)).alias("variance_value"),
                    spark_sum(col(column_name)).alias("sum_value"),
                    count(when(col(column_name) == 0, 1)).alias("zero_count"),
                    count(when(col(column_name) < 0, 1)).alias("negative_count"),
                    expr(f"percentile_approx({column_name}, 0.05)").alias("p5_value"),
                    expr(f"percentile_approx({column_name}, 0.95)").alias("p95_value"),
                ]
            )

        # Single aggregation for all numeric stats
        result = self.df.agg(*agg_list).collect()[0]

        stats = {
            "min": result["min_value"],
            "max": result["max_value"],
            "mean": result["mean_value"],
            "std": result["std_value"] if result["std_value"] is not None else 0.0,
            "median": result["median_value"],
            "q1": result["q1_value"],
            "q3": result["q3_value"],
        }

        # Calculate derived statistics
        if result["min_value"] is not None and result["max_value"] is not None:
            stats["range"] = result["max_value"] - result["min_value"]

        if result["q1_value"] is not None and result["q3_value"] is not None:
            stats["iqr"] = result["q3_value"] - result["q1_value"]

        if advanced:
            stats.update(
                {
                    "skewness": result["skewness_value"],
                    "kurtosis": result["kurtosis_value"],
                    "variance": result["variance_value"],
                    "sum": result["sum_value"],
                    "zero_count": result["zero_count"],
                    "negative_count": result["negative_count"],
                    "p5": result["p5_value"],
                    "p95": result["p95_value"],
                }
            )

            # Coefficient of variation (only if mean is not zero)
            if (
                result["mean_value"]
                and result["mean_value"] != 0
                and result["std_value"]
            ):
                stats["cv"] = abs(result["std_value"] / result["mean_value"])

        return stats

    def compute_string_stats(
        self, column_name: str, top_n: int = 10, pattern_detection: bool = True
    ) -> Dict[str, Any]:
        """
        Compute statistics specific to string columns.

        Args:
            column_name: Name of the string column
            top_n: Number of top frequent values to return (default: 10)
            pattern_detection: Whether to detect patterns (default: True)

        Returns:
            Dictionary with string statistics
        """
        # Build aggregation list
        agg_list = [
            spark_min(length(col(column_name))).alias("min_length"),
            spark_max(length(col(column_name))).alias("max_length"),
            mean(length(col(column_name))).alias("avg_length"),
            count(when(col(column_name) == "", 1)).alias("empty_count"),
            count(when(trim(col(column_name)) != col(column_name), 1)).alias(
                "has_whitespace_count"
            ),
        ]

        if pattern_detection:
            # Common pattern detection (email, URL, phone, numeric)
            agg_list.extend(
                [
                    count(
                        when(
                            col(column_name).rlike(
                                r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                            ),
                            1,
                        )
                    ).alias("email_count"),
                    count(when(col(column_name).rlike(r"^https?://"), 1)).alias(
                        "url_count"
                    ),
                    count(
                        when(col(column_name).rlike(r"^\+?[0-9\s\-\(\)]+$"), 1)
                    ).alias("phone_like_count"),
                    count(when(col(column_name).rlike(r"^[0-9]+$"), 1)).alias(
                        "numeric_string_count"
                    ),
                    count(
                        when(
                            (col(column_name).isNotNull())
                            & (col(column_name) == upper(col(column_name))),
                            1,
                        )
                    ).alias("uppercase_count"),
                    count(
                        when(
                            (col(column_name).isNotNull())
                            & (col(column_name) == lower(col(column_name))),
                            1,
                        )
                    ).alias("lowercase_count"),
                ]
            )

        # Single aggregation for efficiency
        result = self.df.agg(*agg_list).collect()[0]

        stats = {
            "min_length": result["min_length"],
            "max_length": result["max_length"],
            "avg_length": result["avg_length"],
            "empty_count": result["empty_count"],
            "has_whitespace_count": result["has_whitespace_count"],
        }

        if pattern_detection:
            stats["patterns"] = {
                "email_count": result["email_count"],
                "url_count": result["url_count"],
                "phone_like_count": result["phone_like_count"],
                "numeric_string_count": result["numeric_string_count"],
                "uppercase_count": result["uppercase_count"],
                "lowercase_count": result["lowercase_count"],
            }

        # Get top N frequent values efficiently
        if top_n > 0:
            # Use groupBy with count and limit for performance
            top_values = (
                self.df.filter(col(column_name).isNotNull())
                .groupBy(column_name)
                .count()
                .orderBy(desc("count"))
                .limit(top_n)
                .collect()
            )

            stats["top_values"] = [
                {"value": row[column_name], "count": row["count"]} for row in top_values
            ]

        return stats

    def compute_temporal_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Compute statistics specific to temporal columns (date/timestamp).

        Args:
            column_name: Name of the temporal column

        Returns:
            Dictionary with temporal statistics
        """
        result = self.df.agg(
            spark_min(col(column_name)).alias("min_date"),
            spark_max(col(column_name)).alias("max_date"),
        ).collect()[0]

        min_date = result["min_date"]
        max_date = result["max_date"]

        # Calculate date range in days if both dates are present
        date_range_days = None
        if min_date and max_date:
            try:
                date_range_days = (max_date - min_date).days
            except (AttributeError, TypeError):
                # Handle different datetime types
                pass

        return {
            "min_date": min_date,
            "max_date": max_date,
            "date_range_days": date_range_days,
        }

    def compute_outlier_stats(
        self, column_name: str, method: str = "iqr"
    ) -> Dict[str, Any]:
        """
        Compute outlier detection statistics for numeric columns.

        Args:
            column_name: Name of the numeric column
            method: Method for outlier detection ('iqr' or 'zscore')

        Returns:
            Dictionary with outlier statistics
        """
        if method == "iqr":
            # IQR method: outliers are values outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
            result = self.df.agg(
                expr(f"percentile_approx({column_name}, 0.25)").alias("q1"),
                expr(f"percentile_approx({column_name}, 0.75)").alias("q3"),
            ).collect()[0]

            q1 = result["q1"]
            q3 = result["q3"]

            if q1 is not None and q3 is not None:
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                # Count outliers in a single pass
                outlier_result = self.df.agg(
                    count(when(col(column_name) < lower_bound, 1)).alias(
                        "lower_outliers"
                    ),
                    count(when(col(column_name) > upper_bound, 1)).alias(
                        "upper_outliers"
                    ),
                    count(
                        when(
                            (col(column_name) < lower_bound)
                            | (col(column_name) > upper_bound),
                            1,
                        )
                    ).alias("total_outliers"),
                ).collect()[0]

                total_rows = self._get_total_rows()
                outlier_count = outlier_result["total_outliers"]

                return {
                    "method": "iqr",
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "outlier_count": outlier_count,
                    "outlier_percentage": (
                        (outlier_count / total_rows * 100) if total_rows > 0 else 0.0
                    ),
                    "lower_outlier_count": outlier_result["lower_outliers"],
                    "upper_outlier_count": outlier_result["upper_outliers"],
                }

        elif method == "zscore":
            # Z-score method: outliers are values with |z-score| > 3
            stats_result = self.df.agg(
                mean(col(column_name)).alias("mean_val"),
                stddev(col(column_name)).alias("std_val"),
            ).collect()[0]

            mean_val = stats_result["mean_val"]
            std_val = stats_result["std_val"]

            if mean_val is not None and std_val is not None and std_val > 0:
                # Count values with |z-score| > 3
                outlier_count = self.df.filter(
                    spark_abs((col(column_name) - mean_val) / std_val) > 3
                ).count()

                total_rows = self._get_total_rows()

                return {
                    "method": "zscore",
                    "threshold": 3.0,
                    "outlier_count": outlier_count,
                    "outlier_percentage": (
                        (outlier_count / total_rows * 100) if total_rows > 0 else 0.0
                    ),
                    "mean": mean_val,
                    "std": std_val,
                }

        return {"method": method, "outlier_count": 0, "outlier_percentage": 0.0}

    def compute_data_quality_stats(
        self, column_name: str, column_type: str = "auto"
    ) -> Dict[str, Any]:
        """
        Compute data quality metrics for a column.

        Args:
            column_name: Name of the column
            column_type: Type of column ('numeric', 'string', 'temporal', 'auto')

        Returns:
            Dictionary with data quality metrics
        """
        # Get basic stats first (reuse existing computation)
        basic_stats = self.compute_basic_stats(column_name)

        # Initialize quality metrics
        quality_metrics = {
            "completeness": 1.0 - (basic_stats["null_percentage"] / 100.0),
            "uniqueness": (
                basic_stats["distinct_percentage"] / 100.0
                if basic_stats["non_null_count"] > 0
                else 0.0
            ),
            "null_count": basic_stats["null_count"],
        }

        # Auto-detect column type if needed
        if column_type == "auto":
            # Simple type detection based on data
            sample_result = (
                self.df.select(col(column_name))
                .filter(col(column_name).isNotNull())
                .limit(100)
                .collect()
            )
            if sample_result:
                sample_val = sample_result[0][column_name]
                if isinstance(sample_val, (int, float)):
                    column_type = "numeric"
                elif isinstance(sample_val, str):
                    column_type = "string"
                else:
                    column_type = "other"

        # Type-specific quality checks
        if column_type == "numeric":
            # Check for numeric quality issues
            quality_result = self.df.agg(
                count(when(col(column_name).isNaN(), 1)).alias("nan_count"),
                count(when(col(column_name) == float("inf"), 1)).alias("inf_count"),
                count(when(col(column_name) == float("-inf"), 1)).alias(
                    "neg_inf_count"
                ),
            ).collect()[0]

            quality_metrics.update(
                {
                    "nan_count": quality_result["nan_count"],
                    "infinity_count": quality_result["inf_count"]
                    + quality_result["neg_inf_count"],
                }
            )

            # Get outlier info
            outlier_stats = self.compute_outlier_stats(column_name, method="iqr")
            quality_metrics["outlier_percentage"] = outlier_stats["outlier_percentage"]

        elif column_type == "string":
            # Check for string quality issues
            quality_result = self.df.agg(
                count(when(trim(col(column_name)) == "", 1)).alias("blank_count"),
                count(when(col(column_name).rlike(r"[^\x00-\x7F]"), 1)).alias(
                    "non_ascii_count"
                ),
                count(when(length(col(column_name)) == 1, 1)).alias(
                    "single_char_count"
                ),
            ).collect()[0]

            quality_metrics.update(
                {
                    "blank_count": quality_result["blank_count"],
                    "non_ascii_count": quality_result["non_ascii_count"],
                    "single_char_count": quality_result["single_char_count"],
                }
            )
        # For other types (arrays, structs, etc.), we only have basic quality metrics

        # Calculate overall quality score (0-1)
        quality_score = quality_metrics["completeness"]

        # Adjust score based on other factors
        if column_type == "numeric" and "outlier_percentage" in quality_metrics:
            # Penalize for outliers (max 10% penalty)
            outlier_penalty = min(
                quality_metrics["outlier_percentage"] / 100.0 * 0.1, 0.1
            )
            quality_score *= 1 - outlier_penalty

        # Penalize for low uniqueness in ID-like columns
        if "id" in column_name.lower() and quality_metrics["uniqueness"] < 0.95:
            quality_score *= quality_metrics["uniqueness"]

        quality_metrics["quality_score"] = round(quality_score, 3)
        quality_metrics["column_type"] = column_type

        return quality_metrics
