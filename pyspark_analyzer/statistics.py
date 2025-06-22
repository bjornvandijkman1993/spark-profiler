"""
Statistics computation functions for DataFrame profiling.
"""

from typing import Dict, Any, List, Optional
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
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError, Py4JJavaError
from pyspark import __version__ as pyspark_version

from .utils import escape_column_name
from .exceptions import StatisticsError, SparkOperationError
from .logging import get_logger

# Check if median function is available (PySpark 3.4.0+)
try:
    from pyspark.sql.functions import median

    HAS_MEDIAN = True
except ImportError:
    HAS_MEDIAN = False

logger = get_logger(__name__)

# Log which median calculation method will be used
if HAS_MEDIAN:
    logger.debug(f"Using native median function (PySpark {pyspark_version})")
else:
    logger.debug(
        f"Using percentile_approx for median calculation (PySpark {pyspark_version})"
    )


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
        # Store total rows if provided to avoid recomputation
        self._total_rows = total_rows
        self.cache_enabled = False
        logger.debug(
            f"StatisticsComputer initialized with {'cached' if total_rows else 'lazy'} row count"
        )

    def _get_total_rows(self) -> int:
        """Get total row count, computing if not cached."""
        if self._total_rows is None:
            logger.debug("Computing DataFrame row count")
            self._total_rows = self.df.count()
            logger.debug(f"Row count computed: {self._total_rows:,}")
        return self._total_rows

    def compute_basic_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Compute basic statistics for any column type using optimized lazy evaluation.

        Args:
            column_name: Name of the column

        Returns:
            Dictionary with basic statistics
        """
        logger.debug(f"Computing basic statistics for column: {column_name}")
        try:
            # Use lazy evaluation - total_rows will only be computed if needed
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
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(
                f"Failed to compute basic stats for column '{column_name}': {str(e)}"
            )
            raise SparkOperationError(
                f"Failed to compute basic statistics for column '{column_name}': {str(e)}",
                e,
            )
        except Exception as e:
            logger.error(
                f"Unexpected error computing basic stats for column '{column_name}': {str(e)}"
            )
            raise StatisticsError(
                f"Failed to compute basic statistics for column '{column_name}': {str(e)}"
            )

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
        logger.debug(
            f"Computing numeric statistics for column: {column_name}, advanced={advanced}"
        )
        # Build aggregation list dynamically for performance
        agg_list = [
            spark_min(col(column_name)).alias("min_value"),
            spark_max(col(column_name)).alias("max_value"),
            mean(col(column_name)).alias("mean_value"),
            stddev(col(column_name)).alias("std_value"),
        ]

        # Use median function if available (PySpark 3.4.0+), otherwise use percentile_approx
        if HAS_MEDIAN:
            agg_list.append(median(col(column_name)).alias("median_value"))
        else:
            agg_list.append(
                expr(f"percentile_approx({column_name}, 0.5)").alias("median_value")
            )

        agg_list.extend(
            [
                expr(f"percentile_approx({column_name}, 0.25)").alias("q1_value"),
                expr(f"percentile_approx({column_name}, 0.75)").alias("q3_value"),
            ]
        )

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
        logger.debug(f"Executing numeric aggregation with {len(agg_list)} expressions")
        try:
            result = self.df.agg(*agg_list).collect()[0]
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(
                f"Failed to compute numeric stats for column '{column_name}': {str(e)}"
            )
            raise SparkOperationError(
                f"Failed to compute numeric statistics for column '{column_name}': {str(e)}",
                e,
            )
        except Exception as e:
            logger.error(
                f"Unexpected error computing numeric stats for column '{column_name}': {str(e)}"
            )
            raise StatisticsError(
                f"Failed to compute numeric statistics for column '{column_name}': {str(e)}"
            )

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
                try:
                    stats["cv"] = abs(result["std_value"] / result["mean_value"])
                except (ZeroDivisionError, ArithmeticError) as e:
                    logger.warning(
                        f"Could not compute coefficient of variation for column '{column_name}': {str(e)}"
                    )
                    stats["cv"] = None

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
        try:
            result = self.df.agg(*agg_list).collect()[0]
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(
                f"Failed to compute string stats for column '{column_name}': {str(e)}"
            )
            raise SparkOperationError(
                f"Failed to compute string statistics for column '{column_name}': {str(e)}",
                e,
            )
        except Exception as e:
            logger.error(
                f"Unexpected error computing string stats for column '{column_name}': {str(e)}"
            )
            raise StatisticsError(
                f"Failed to compute string statistics for column '{column_name}': {str(e)}"
            )

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
        try:
            result = self.df.agg(
                spark_min(col(column_name)).alias("min_date"),
                spark_max(col(column_name)).alias("max_date"),
            ).collect()[0]
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(
                f"Failed to compute temporal stats for column '{column_name}': {str(e)}"
            )
            raise SparkOperationError(
                f"Failed to compute temporal statistics for column '{column_name}': {str(e)}",
                e,
            )
        except Exception as e:
            logger.error(
                f"Unexpected error computing temporal stats for column '{column_name}': {str(e)}"
            )
            raise StatisticsError(
                f"Failed to compute temporal statistics for column '{column_name}': {str(e)}"
            )

        min_date = result["min_date"]
        max_date = result["max_date"]

        # Calculate date range in days if both dates are present
        date_range_days = None
        if min_date and max_date:
            try:
                date_range_days = (max_date - min_date).days
            except (AttributeError, TypeError) as e:
                # Handle different datetime types
                logger.warning(
                    f"Could not calculate date range for column '{column_name}': {str(e)}"
                )
                date_range_days = None

        return {
            "min_date": min_date,
            "max_date": max_date,
            "date_range_days": date_range_days,
        }

    def compute_outlier_stats(
        self, column_name: str, method: str = "iqr"
    ) -> Dict[str, Any]:
        """
        Compute outlier detection statistics for numeric columns using lazy evaluation.

        Args:
            column_name: Name of the numeric column
            method: Method for outlier detection ('iqr' or 'zscore')

        Returns:
            Dictionary with outlier statistics
        """
        logger.debug(
            f"Computing outlier statistics for column: {column_name}, method={method}"
        )
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

                # Compute outlier counts in a single aggregation
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
                # Compute z-score outliers
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

    def enable_caching(self) -> None:
        """
        Enable DataFrame caching for multiple statistics computations.

        Use this when profiling multiple columns on the same dataset
        to avoid recomputing the DataFrame multiple times.
        """
        if not self.cache_enabled:
            self.df.cache()
            self.cache_enabled = True

    def disable_caching(self) -> None:
        """Disable DataFrame caching and unpersist cached data."""
        if self.cache_enabled:
            self.df.unpersist()
            self.cache_enabled = False

    def compute_all_columns_batch(
        self,
        columns: Optional[List[str]] = None,
        include_advanced: bool = True,
        include_quality: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compute statistics for multiple columns in batch operations.

        This method optimizes performance by:
        1. Combining multiple aggregations into single operations
        2. Using approximate functions where possible
        3. Minimizing data shuffling

        Args:
            columns: List of columns to profile. If None, profiles all columns.
            include_advanced: Include advanced statistics (skewness, kurtosis, outliers, etc.)
            include_quality: Include data quality metrics

        Returns:
            Dictionary mapping column names to their statistics
        """
        if columns is None:
            columns = self.df.columns

        logger.info(f"Starting batch computation for {len(columns)} columns")

        # Enable caching for multiple operations
        self.enable_caching()

        try:
            # Get data types for all columns
            column_types = {
                field.name: field.dataType for field in self.df.schema.fields
            }

            # Build all aggregation expressions at once
            all_agg_exprs = []
            columns_to_process = []

            for column in columns:
                if column in column_types:
                    columns_to_process.append(column)
                    agg_exprs = self._build_column_agg_exprs(
                        column, column_types[column], include_advanced
                    )
                    all_agg_exprs.extend(agg_exprs)

            # Execute single aggregation for all columns
            if all_agg_exprs:
                logger.debug(
                    f"Executing batch aggregation with {len(all_agg_exprs)} expressions"
                )
                result_row = self.df.agg(*all_agg_exprs).collect()[0]

                # Get total rows if not cached
                total_rows = self._get_total_rows()

                # Extract results for each column
                results = {}
                for column in columns_to_process:
                    results[column] = self._extract_column_stats(
                        column,
                        column_types[column],
                        result_row,
                        total_rows,
                        include_advanced,
                        include_quality,
                    )

                logger.info(f"Batch computation completed for {len(results)} columns")
                return results
            else:
                return {}
        finally:
            # Always clean up caching
            self.disable_caching()

    def _build_column_agg_exprs(
        self, column_name: str, column_type: Any, include_advanced: bool = True
    ) -> List[Any]:
        """
        Build aggregation expressions for a single column.

        Args:
            column_name: Name of the column
            column_type: PySpark data type of the column
            include_advanced: Whether to include advanced statistics

        Returns:
            List of aggregation expressions for this column
        """
        from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

        # Escape column name for special characters
        escaped_name = escape_column_name(column_name)

        # Build aggregation expressions based on column type
        agg_exprs = [
            count(col(escaped_name)).alias(f"{column_name}_non_null_count"),
            count(when(col(escaped_name).isNull(), 1)).alias(
                f"{column_name}_null_count"
            ),
            approx_count_distinct(col(escaped_name), rsd=0.05).alias(
                f"{column_name}_distinct_count"
            ),
        ]

        # Add type-specific aggregations
        if isinstance(column_type, NumericType):
            numeric_aggs = [
                spark_min(col(escaped_name)).alias(f"{column_name}_min"),
                spark_max(col(escaped_name)).alias(f"{column_name}_max"),
                mean(col(escaped_name)).alias(f"{column_name}_mean"),
                stddev(col(escaped_name)).alias(f"{column_name}_std"),
            ]

            # Use median function if available (PySpark 3.4.0+), otherwise use percentile_approx
            if HAS_MEDIAN:
                numeric_aggs.append(
                    median(col(escaped_name)).alias(f"{column_name}_median")
                )
            else:
                numeric_aggs.append(
                    expr(f"percentile_approx({escaped_name}, 0.5)").alias(
                        f"{column_name}_median"
                    )
                )

            numeric_aggs.extend(
                [
                    expr(f"percentile_approx({escaped_name}, 0.25)").alias(
                        f"{column_name}_q1"
                    ),
                    expr(f"percentile_approx({escaped_name}, 0.75)").alias(
                        f"{column_name}_q3"
                    ),
                ]
            )

            # Add advanced statistics if requested
            if include_advanced:
                numeric_aggs.extend(
                    [
                        skewness(col(escaped_name)).alias(f"{column_name}_skewness"),
                        kurtosis(col(escaped_name)).alias(f"{column_name}_kurtosis"),
                    ]
                )

            agg_exprs.extend(numeric_aggs)
        elif isinstance(column_type, StringType):
            agg_exprs.extend(
                [
                    spark_min(length(col(escaped_name))).alias(
                        f"{column_name}_min_length"
                    ),
                    spark_max(length(col(escaped_name))).alias(
                        f"{column_name}_max_length"
                    ),
                    mean(length(col(escaped_name))).alias(f"{column_name}_avg_length"),
                    count(when(col(escaped_name) == "", 1)).alias(
                        f"{column_name}_empty_count"
                    ),
                ]
            )
        elif isinstance(column_type, (TimestampType, DateType)):
            agg_exprs.extend(
                [
                    spark_min(col(escaped_name)).alias(f"{column_name}_min_date"),
                    spark_max(col(escaped_name)).alias(f"{column_name}_max_date"),
                ]
            )

        return agg_exprs

    def _extract_column_stats(
        self,
        column_name: str,
        column_type: Any,
        result_row: Any,
        total_rows: int,
        include_advanced: bool = True,
        include_quality: bool = True,
    ) -> Dict[str, Any]:
        """
        Extract statistics for a single column from the aggregation result.

        Args:
            column_name: Name of the column
            column_type: PySpark data type of the column
            result_row: Row containing all aggregation results
            total_rows: Total number of rows in the DataFrame
            include_advanced: Whether to include advanced statistics
            include_quality: Whether to include data quality metrics

        Returns:
            Dictionary with column statistics
        """
        from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

        # Extract basic statistics
        non_null_count = result_row[f"{column_name}_non_null_count"]
        null_count = result_row[f"{column_name}_null_count"]
        distinct_count = result_row[f"{column_name}_distinct_count"]

        stats = {
            "data_type": str(column_type),
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

        # Add type-specific statistics
        if isinstance(column_type, NumericType):
            min_val = result_row[f"{column_name}_min"]
            max_val = result_row[f"{column_name}_max"]
            q1_val = result_row[f"{column_name}_q1"]
            q3_val = result_row[f"{column_name}_q3"]

            stats.update(
                {
                    "min": min_val,
                    "max": max_val,
                    "mean": result_row[f"{column_name}_mean"],
                    "std": (
                        result_row[f"{column_name}_std"]
                        if result_row[f"{column_name}_std"] is not None
                        else 0.0
                    ),
                    "median": result_row[f"{column_name}_median"],
                    "q1": q1_val,
                    "q3": q3_val,
                }
            )

            # Calculate derived statistics
            if min_val is not None and max_val is not None:
                stats["range"] = max_val - min_val

            if q1_val is not None and q3_val is not None:
                stats["iqr"] = q3_val - q1_val

            # Add advanced statistics if included and available
            if include_advanced:
                if f"{column_name}_skewness" in result_row:
                    stats["skewness"] = result_row[f"{column_name}_skewness"]
                if f"{column_name}_kurtosis" in result_row:
                    stats["kurtosis"] = result_row[f"{column_name}_kurtosis"]

                # Add outlier statistics (these require separate computation)
                if q1_val is not None and q3_val is not None:
                    outlier_stats = self.compute_outlier_stats(column_name)
                    stats["outliers"] = outlier_stats

        elif isinstance(column_type, StringType):
            stats.update(
                {
                    "min_length": result_row[f"{column_name}_min_length"],
                    "max_length": result_row[f"{column_name}_max_length"],
                    "avg_length": result_row[f"{column_name}_avg_length"],
                    "empty_count": result_row[f"{column_name}_empty_count"],
                }
            )

            # Add advanced string statistics if requested
            if include_advanced:
                # These require separate computation
                string_stats = self.compute_string_stats(
                    column_name, top_n=10, pattern_detection=True
                )
                # Only add the advanced parts
                if "top_values" in string_stats:
                    stats["top_values"] = string_stats["top_values"]
                if "patterns" in string_stats:
                    stats["patterns"] = string_stats["patterns"]
        elif isinstance(column_type, (TimestampType, DateType)):
            min_date = result_row[f"{column_name}_min_date"]
            max_date = result_row[f"{column_name}_max_date"]

            date_range_days = None
            if min_date and max_date:
                try:
                    date_range_days = (max_date - min_date).days
                except (AttributeError, TypeError):
                    date_range_days = None

            stats.update(
                {
                    "min_date": min_date,
                    "max_date": max_date,
                    "date_range_days": date_range_days,
                }
            )

        # Add data quality metrics if requested
        if include_quality:
            # Determine quality check type based on column type
            if isinstance(column_type, NumericType):
                quality_type = "numeric"
            elif isinstance(column_type, StringType):
                quality_type = "string"
            else:
                # For complex types (arrays, structs, etc.), skip type-specific quality checks
                quality_type = "other"

            quality_stats = self.compute_data_quality_stats(
                column_name, column_type=quality_type
            )
            stats["quality"] = quality_stats

        return stats
