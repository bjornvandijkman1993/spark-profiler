"""
Statistics computation with minimal DataFrame scans.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError, Py4JJavaError
from pyspark.sql import functions as F
from pyspark.sql import types as t

from .constants import (
    PATTERNS,
    OUTLIER_IQR_MULTIPLIER,
    APPROX_DISTINCT_RSD,
    DEFAULT_TOP_VALUES_LIMIT,
    ID_COLUMN_UNIQUENESS_THRESHOLD,
    QUALITY_OUTLIER_PENALTY_MAX,
)
from .utils import escape_column_name
from .exceptions import StatisticsError, SparkOperationError
from .logging import get_logger

# Check if median function is available (PySpark 3.4.0+)
try:
    F.median
    HAS_MEDIAN = True
except AttributeError:
    HAS_MEDIAN = False

logger = get_logger(__name__)


class StatisticsComputer:
    """Computes statistics for DataFrame columns using type-specific calculators."""

    def __init__(self, dataframe: DataFrame, total_rows: Optional[int] = None):
        """
        Initialize with a PySpark DataFrame.

        Args:
            dataframe: PySpark DataFrame to compute statistics for
            total_rows: Cached row count to avoid recomputation
        """
        self.df = dataframe
        self._total_rows = total_rows
        self._column_types = {
            field.name: field.dataType for field in self.df.schema.fields
        }
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

    def compute_all_columns_batch(
        self,
        columns: Optional[List[str]] = None,
        include_advanced: bool = True,
        include_quality: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compute statistics for multiple columns with minimal DataFrame scans.

        Args:
            columns: List of columns to profile. If None, profiles all columns.
            include_advanced: Include advanced statistics (always True)
            include_quality: Include data quality metrics

        Returns:
            Dictionary mapping column names to their statistics
        """
        if columns is None:
            columns = self.df.columns

        logger.debug("Computing basic statistics")
        logger.info(f"Starting optimized computation for {len(columns)} columns")

        # Filter out non-existent columns
        valid_columns = [col for col in columns if col in self._column_types]
        if len(valid_columns) < len(columns):
            invalid = set(columns) - set(valid_columns)
            logger.warning(f"Columns not found in DataFrame: {invalid}")

        total_rows = self._get_total_rows()

        # Build aggregation expressions for all columns
        agg_exprs = self._build_aggregation_expressions(
            valid_columns, include_quality, include_advanced
        )

        # Handle empty DataFrame or no valid columns
        if not valid_columns:
            logger.warning("No valid columns to process")
            return {}

        if not agg_exprs:
            logger.warning("No aggregation expressions generated")
            return {}

        try:
            # Execute single aggregation for all statistics
            logger.debug(f"Executing aggregation with {len(agg_exprs)} expressions")
            result_row = self.df.agg(*agg_exprs).collect()[0]

            # Unpack results into column dictionaries
            results = self._unpack_results(
                result_row, valid_columns, total_rows, include_quality, include_advanced
            )

            # Compute special cases that require separate scans
            special_stats = self._compute_special_cases(
                valid_columns, results, include_advanced
            )

            # Merge special statistics
            for col_name, special in special_stats.items():
                if col_name in results:
                    results[col_name].update(special)

            logger.info(f"Computation completed for {len(results)} columns")
            return results

        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(f"Spark error during batch computation: {str(e)}")
            raise SparkOperationError(
                f"Failed to compute statistics in batch: {str(e)}", e
            )
        except Exception as e:
            logger.error(f"Unexpected error during batch computation: {str(e)}")
            raise StatisticsError(f"Failed to compute statistics in batch: {str(e)}")

    def _build_aggregation_expressions(
        self, columns: List[str], include_quality: bool, include_advanced: bool
    ) -> List:
        """Build aggregation expressions for all columns in a single pass."""
        agg_exprs = []

        for col_name in columns:
            col_type = self._column_types[col_name]
            escaped = escape_column_name(col_name)

            # Basic statistics for all column types
            agg_exprs.extend(
                [
                    F.count(F.col(escaped)).alias(f"{col_name}__non_null_count"),
                    F.count(F.when(F.col(escaped).isNull(), 1)).alias(
                        f"{col_name}__null_count"
                    ),
                    F.approx_count_distinct(
                        F.col(escaped), rsd=APPROX_DISTINCT_RSD
                    ).alias(f"{col_name}__distinct_count"),
                ]
            )

            # Numeric column statistics
            if isinstance(col_type, t.NumericType):
                agg_exprs.extend(
                    self._build_numeric_expressions(col_name, escaped, include_advanced)
                )
                if include_quality:
                    agg_exprs.extend(
                        self._build_numeric_quality_expressions(col_name, escaped)
                    )

            # String column statistics
            elif isinstance(col_type, t.StringType):
                agg_exprs.extend(
                    self._build_string_expressions(col_name, escaped, include_advanced)
                )
                if include_quality:
                    agg_exprs.extend(
                        self._build_string_quality_expressions(col_name, escaped)
                    )

            # Temporal column statistics
            elif isinstance(col_type, (t.TimestampType, t.DateType)):
                agg_exprs.extend(self._build_temporal_expressions(col_name, escaped))

        return agg_exprs

    def _build_numeric_expressions(
        self, col_name: str, escaped: str, include_advanced: bool
    ) -> List:
        """Build numeric-specific aggregation expressions."""
        exprs = [
            F.min(F.col(escaped)).alias(f"{col_name}__min"),
            F.max(F.col(escaped)).alias(f"{col_name}__max"),
            F.mean(F.col(escaped)).alias(f"{col_name}__mean"),
            F.stddev(F.col(escaped)).alias(f"{col_name}__std"),
            F.sum(F.col(escaped)).alias(f"{col_name}__sum"),
            F.count(F.when(F.col(escaped) == 0, 1)).alias(f"{col_name}__zero_count"),
            F.count(F.when(F.col(escaped) < 0, 1)).alias(f"{col_name}__negative_count"),
        ]

        if include_advanced:
            # Add advanced statistics
            exprs.extend(
                [
                    F.skewness(F.col(escaped)).alias(f"{col_name}__skewness"),
                    F.kurtosis(F.col(escaped)).alias(f"{col_name}__kurtosis"),
                    F.variance(F.col(escaped)).alias(f"{col_name}__variance"),
                ]
            )

            # Add median
            if HAS_MEDIAN:
                exprs.append(F.median(F.col(escaped)).alias(f"{col_name}__median"))
            else:
                exprs.append(
                    F.expr(f"percentile_approx({escaped}, 0.5)").alias(
                        f"{col_name}__median"
                    )
                )

            # Add percentiles
            percentiles = [(0.25, "q1"), (0.75, "q3"), (0.05, "p5"), (0.95, "p95")]
            for p_val, p_name in percentiles:
                exprs.append(
                    F.expr(f"percentile_approx({escaped}, {p_val})").alias(
                        f"{col_name}__{p_name}"
                    )
                )

        return exprs

    def _build_numeric_quality_expressions(self, col_name: str, escaped: str) -> List:
        """Build numeric quality-specific expressions."""
        return [
            F.count(F.when(F.isnan(F.col(escaped)), 1)).alias(f"{col_name}__nan_count"),
            F.count(F.when(F.col(escaped) == float("inf"), 1)).alias(
                f"{col_name}__inf_count"
            ),
            F.count(F.when(F.col(escaped) == float("-inf"), 1)).alias(
                f"{col_name}__neg_inf_count"
            ),
        ]

    def _build_string_expressions(
        self, col_name: str, escaped: str, include_advanced: bool
    ) -> List:
        """Build string-specific aggregation expressions."""
        exprs = [
            F.min(F.length(F.col(escaped))).alias(f"{col_name}__min_length"),
            F.max(F.length(F.col(escaped))).alias(f"{col_name}__max_length"),
            F.mean(F.length(F.col(escaped))).alias(f"{col_name}__avg_length"),
            F.count(F.when(F.col(escaped) == "", 1)).alias(f"{col_name}__empty_count"),
        ]

        if include_advanced:
            # Add advanced string statistics
            exprs.append(
                F.count(F.when(F.trim(F.col(escaped)) != F.col(escaped), 1)).alias(
                    f"{col_name}__has_whitespace_count"
                )
            )

            # Pattern detection
            exprs.extend(
                [
                    F.count(F.when(F.col(escaped).rlike(PATTERNS["email"]), 1)).alias(
                        f"{col_name}__email_count"
                    ),
                    F.count(F.when(F.col(escaped).rlike(PATTERNS["url"]), 1)).alias(
                        f"{col_name}__url_count"
                    ),
                    F.count(F.when(F.col(escaped).rlike(PATTERNS["phone"]), 1)).alias(
                        f"{col_name}__phone_like_count"
                    ),
                    F.count(
                        F.when(F.col(escaped).rlike(PATTERNS["numeric_string"]), 1)
                    ).alias(f"{col_name}__numeric_string_count"),
                    F.count(
                        F.when(
                            (F.col(escaped).isNotNull())
                            & (F.col(escaped) == F.upper(F.col(escaped))),
                            1,
                        )
                    ).alias(f"{col_name}__uppercase_count"),
                    F.count(
                        F.when(
                            (F.col(escaped).isNotNull())
                            & (F.col(escaped) == F.lower(F.col(escaped))),
                            1,
                        )
                    ).alias(f"{col_name}__lowercase_count"),
                ]
            )

        return exprs

    def _build_string_quality_expressions(self, col_name: str, escaped: str) -> List:
        """Build string quality-specific expressions."""
        return [
            F.count(F.when(F.trim(F.col(escaped)) == "", 1)).alias(
                f"{col_name}__blank_count"
            ),
            F.count(F.when(F.col(escaped).rlike(r"[^\x00-\x7F]"), 1)).alias(
                f"{col_name}__non_ascii_count"
            ),
            F.count(F.when(F.length(F.col(escaped)) == 1, 1)).alias(
                f"{col_name}__single_char_count"
            ),
        ]

    def _build_temporal_expressions(self, col_name: str, escaped: str) -> List:
        """Build temporal-specific aggregation expressions."""
        return [
            F.min(F.col(escaped)).alias(f"{col_name}__min_date"),
            F.max(F.col(escaped)).alias(f"{col_name}__max_date"),
        ]

    def _unpack_results(
        self,
        result_row: Any,
        columns: List[str],
        total_rows: int,
        include_quality: bool,
        include_advanced: bool,
    ) -> Dict[str, Dict[str, Any]]:
        """Unpack flat aggregation results into column-specific dictionaries."""
        results = {}

        for col_name in columns:
            col_type = self._column_types[col_name]

            # Basic statistics - common to all types
            non_null_count = result_row[f"{col_name}__non_null_count"]
            null_count = result_row[f"{col_name}__null_count"]
            distinct_count = result_row[f"{col_name}__distinct_count"]

            stats: Dict[str, Any] = {
                "data_type": str(col_type),
                "total_count": int(total_rows),
                "non_null_count": non_null_count,
                "null_count": null_count,
                "null_percentage": (
                    (null_count / total_rows * 100) if total_rows > 0 else 0.0
                ),
                "distinct_count": distinct_count,
                "distinct_percentage": (
                    (distinct_count / non_null_count * 100)
                    if non_null_count > 0
                    else 0.0
                ),
            }

            # Type-specific statistics
            if isinstance(col_type, t.NumericType):
                self._unpack_numeric(
                    stats,
                    result_row,
                    col_name,
                    total_rows,
                    include_advanced,
                    include_quality,
                )
            elif isinstance(col_type, t.StringType):
                self._unpack_string(
                    stats, result_row, col_name, include_advanced, include_quality
                )
            elif isinstance(col_type, (t.TimestampType, t.DateType)):
                self._unpack_temporal(stats, result_row, col_name)

            # Add quality score if requested
            if include_quality:
                quality_metrics = self._calculate_quality_metrics(
                    stats, col_type, col_name
                )
                stats["quality"] = quality_metrics

            results[col_name] = stats

        return results

    def _unpack_numeric(
        self,
        stats: Dict[str, Any],
        result_row: Any,
        col_name: str,
        total_rows: int,
        include_advanced: bool,
        include_quality: bool,
    ) -> None:
        """Unpack all numeric statistics and quality metrics from result row."""
        # Basic numeric stats
        stats.update(
            {
                "min": result_row[f"{col_name}__min"],
                "max": result_row[f"{col_name}__max"],
                "mean": result_row[f"{col_name}__mean"],
                "std": (
                    result_row[f"{col_name}__std"]
                    if result_row[f"{col_name}__std"] is not None
                    else 0.0
                ),
                "sum": result_row[f"{col_name}__sum"],
                "zero_count": result_row[f"{col_name}__zero_count"],
                "negative_count": result_row[f"{col_name}__negative_count"],
            }
        )

        # Advanced statistics
        if include_advanced:
            stats.update(
                {
                    "median": result_row[f"{col_name}__median"],
                    "q1": result_row[f"{col_name}__q1"],
                    "q3": result_row[f"{col_name}__q3"],
                    "p5": result_row[f"{col_name}__p5"],
                    "p95": result_row[f"{col_name}__p95"],
                    "skewness": result_row[f"{col_name}__skewness"],
                    "kurtosis": result_row[f"{col_name}__kurtosis"],
                    "variance": result_row[f"{col_name}__variance"],
                }
            )

        # Quality metrics
        if include_quality:
            nan_count = result_row[f"{col_name}__nan_count"]
            inf_count = result_row[f"{col_name}__inf_count"]
            neg_inf_count = result_row[f"{col_name}__neg_inf_count"]

            if "quality" not in stats:
                stats["quality"] = {}

            stats["quality"].update(
                {
                    "nan_count": nan_count,
                    "infinity_count": inf_count + neg_inf_count,
                }
            )

        # Derived statistics
        if stats["min"] is not None and stats["max"] is not None:
            stats["range"] = stats["max"] - stats["min"]

        if (
            include_advanced
            and stats.get("q1") is not None
            and stats.get("q3") is not None
        ):
            stats["iqr"] = stats["q3"] - stats["q1"]
            # Calculate outlier bounds (counts computed separately)
            iqr = stats["iqr"]
            stats["outliers"] = {
                "method": "iqr",
                "lower_bound": stats["q1"] - OUTLIER_IQR_MULTIPLIER * iqr,
                "upper_bound": stats["q3"] + OUTLIER_IQR_MULTIPLIER * iqr,
                "outlier_count": 0,  # Updated in _compute_special_cases
                "outlier_percentage": 0.0,
                "lower_outlier_count": 0,
                "upper_outlier_count": 0,
            }

        # Coefficient of variation
        if stats["mean"] and stats["mean"] != 0 and stats["std"]:
            stats["cv"] = abs(stats["std"] / stats["mean"])

    def _unpack_string(
        self,
        stats: Dict[str, Any],
        result_row: Any,
        col_name: str,
        include_advanced: bool,
        include_quality: bool,
    ) -> None:
        """Unpack all string statistics and quality metrics from result row."""
        # Basic string stats
        stats.update(
            {
                "min_length": result_row[f"{col_name}__min_length"],
                "max_length": result_row[f"{col_name}__max_length"],
                "avg_length": result_row[f"{col_name}__avg_length"],
                "empty_count": result_row[f"{col_name}__empty_count"],
            }
        )

        # Advanced statistics
        if include_advanced:
            stats["has_whitespace_count"] = result_row[
                f"{col_name}__has_whitespace_count"
            ]
            # Pattern detection results
            stats["patterns"] = {
                "email_count": result_row[f"{col_name}__email_count"],
                "url_count": result_row[f"{col_name}__url_count"],
                "phone_like_count": result_row[f"{col_name}__phone_like_count"],
                "numeric_string_count": result_row[f"{col_name}__numeric_string_count"],
                "uppercase_count": result_row[f"{col_name}__uppercase_count"],
                "lowercase_count": result_row[f"{col_name}__lowercase_count"],
            }

        # Quality metrics
        if include_quality:
            if "quality" not in stats:
                stats["quality"] = {}

            stats["quality"].update(
                {
                    "blank_count": result_row[f"{col_name}__blank_count"],
                    "non_ascii_count": result_row[f"{col_name}__non_ascii_count"],
                    "single_char_count": result_row[f"{col_name}__single_char_count"],
                }
            )

    def _unpack_temporal(
        self, stats: Dict[str, Any], result_row: Any, col_name: str
    ) -> None:
        """Unpack temporal statistics from result row."""
        min_date = result_row[f"{col_name}__min_date"]
        max_date = result_row[f"{col_name}__max_date"]

        stats.update(
            {
                "min_date": min_date,
                "max_date": max_date,
            }
        )

        # Calculate date range in days
        if min_date and max_date:
            try:
                date_range_days = (max_date - min_date).days
                stats["date_range_days"] = date_range_days
            except (AttributeError, TypeError):
                stats["date_range_days"] = None
        else:
            stats["date_range_days"] = None

    def _calculate_quality_metrics(
        self, stats: Dict[str, Any], column_type: Any, col_name: str
    ) -> Dict[str, Any]:
        """Calculate overall quality metrics for a column."""
        null_percentage = stats.get("null_percentage", 0.0)
        distinct_percentage = stats.get("distinct_percentage", 0.0)
        non_null_count = stats.get("non_null_count", 0)

        quality_metrics = stats.get("quality", {})
        quality_metrics.update(
            {
                "completeness": 1.0 - (null_percentage / 100.0),
                "uniqueness": (
                    distinct_percentage / 100.0 if non_null_count > 0 else 0.0
                ),
                "null_count": stats.get("null_count", 0),
                "column_type": self._get_type_name(column_type),
            }
        )

        # Calculate quality score
        quality_score = quality_metrics["completeness"]

        # Penalize for outliers in numeric columns
        if isinstance(column_type, t.NumericType) and "outliers" in stats:
            outlier_percentage = stats["outliers"].get("outlier_percentage", 0.0)
            outlier_penalty = min(
                outlier_percentage / 100.0 * QUALITY_OUTLIER_PENALTY_MAX,
                QUALITY_OUTLIER_PENALTY_MAX,
            )
            quality_score *= 1 - outlier_penalty
            quality_metrics["outlier_percentage"] = outlier_percentage

        # Penalize for low uniqueness in ID-like columns
        if (
            "id" in col_name.lower()
            and quality_metrics["uniqueness"] < ID_COLUMN_UNIQUENESS_THRESHOLD
        ):
            quality_score *= quality_metrics["uniqueness"]

        quality_metrics["quality_score"] = round(quality_score, 3)

        return dict(quality_metrics)

    def _get_type_name(self, column_type: Any) -> str:
        """Get simplified type name for reporting."""
        if isinstance(column_type, t.NumericType):
            return "numeric"
        elif isinstance(column_type, t.StringType):
            return "string"
        elif isinstance(column_type, (t.TimestampType, t.DateType)):
            return "temporal"
        else:
            return "other"

    def _compute_special_cases(
        self,
        columns: List[str],
        results: Dict[str, Dict[str, Any]],
        include_advanced: bool,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compute statistics that require separate scans.
        Optimized to minimize the number of additional scans.
        """
        special_stats: Dict[str, Dict[str, Any]] = {}

        # Group columns by what special processing they need
        numeric_cols_needing_outliers = []
        string_cols_needing_top_values = []

        for col_name in columns:
            col_type = self._column_types[col_name]

            # Numeric columns need outlier counts (only if advanced stats are requested)
            if (
                isinstance(col_type, t.NumericType)
                and col_name in results
                and include_advanced
            ):
                if "outliers" in results[col_name]:
                    numeric_cols_needing_outliers.append(col_name)

            # String columns need top values (only if advanced stats are requested)
            elif isinstance(col_type, t.StringType) and include_advanced:
                string_cols_needing_top_values.append(col_name)

        # Compute outlier counts for numeric columns in one scan
        if numeric_cols_needing_outliers:
            outlier_stats = self._compute_outlier_counts_batch(
                numeric_cols_needing_outliers, results
            )
            for col_name, outlier_info in outlier_stats.items():
                if col_name not in special_stats:
                    special_stats[col_name] = {}
                special_stats[col_name]["outliers"] = outlier_info

        # Compute top values for string columns (requires separate groupBy per column)
        if string_cols_needing_top_values:
            # Process in batches to avoid too many concurrent operations
            batch_size = 10
            for i in range(0, len(string_cols_needing_top_values), batch_size):
                batch = string_cols_needing_top_values[i : i + batch_size]
                for col_name in batch:
                    top_values = self._get_top_values(col_name)
                    if col_name not in special_stats:
                        special_stats[col_name] = {}
                    special_stats[col_name]["top_values"] = top_values

        return special_stats

    def _compute_outlier_counts_batch(
        self, columns: List[str], results: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Compute outlier counts for multiple numeric columns in one scan."""
        agg_exprs = []
        bounds_map = {}

        for col_name in columns:
            if col_name in results and "outliers" in results[col_name]:
                outlier_info = results[col_name]["outliers"]
                lower_bound = outlier_info["lower_bound"]
                upper_bound = outlier_info["upper_bound"]
                bounds_map[col_name] = (lower_bound, upper_bound)

                escaped = escape_column_name(col_name)

                agg_exprs.extend(
                    [
                        F.count(F.when(F.col(escaped) < lower_bound, 1)).alias(
                            f"{col_name}__lower_outliers"
                        ),
                        F.count(F.when(F.col(escaped) > upper_bound, 1)).alias(
                            f"{col_name}__upper_outliers"
                        ),
                    ]
                )

        if not agg_exprs:
            return {}

        # Execute aggregation
        result_row = self.df.agg(*agg_exprs).collect()[0]

        # Unpack results
        outlier_results = {}
        total_rows = self._get_total_rows()

        for col_name in columns:
            if col_name in bounds_map:
                lower_count = result_row[f"{col_name}__lower_outliers"]
                upper_count = result_row[f"{col_name}__upper_outliers"]
                total_outliers = lower_count + upper_count

                lower_bound, upper_bound = bounds_map[col_name]

                outlier_results[col_name] = {
                    "method": "iqr",
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "outlier_count": total_outliers,
                    "outlier_percentage": (
                        (total_outliers / total_rows * 100) if total_rows > 0 else 0.0
                    ),
                    "lower_outlier_count": lower_count,
                    "upper_outlier_count": upper_count,
                }

        return outlier_results

    def _get_top_values(
        self, column_name: str, limit: int = DEFAULT_TOP_VALUES_LIMIT
    ) -> List[Dict[str, Any]]:
        """Get top frequent values for a column."""
        escaped = escape_column_name(column_name)

        try:
            top_values = (
                self.df.filter(F.col(escaped).isNotNull())
                .groupBy(column_name)
                .count()
                .orderBy(F.desc("count"))
                .limit(limit)
                .collect()
            )

            return [
                {"value": row[column_name], "count": row["count"]} for row in top_values
            ]
        except Exception as e:
            logger.warning(f"Failed to compute top values for {column_name}: {str(e)}")
            return []
