"""Data quality calculation module."""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql import types as t

from .constants import ID_COLUMN_UNIQUENESS_THRESHOLD, QUALITY_OUTLIER_PENALTY_MAX
from .logging import get_logger

logger = get_logger(__name__)


class QualityCalculator:
    """Calculates data quality metrics for DataFrame columns."""

    def __init__(self, df: DataFrame, column_name: str, total_rows: int):
        self.df = df
        self.column_name = column_name
        self.total_rows = total_rows

    def calculate_quality(
        self, stats: dict[str, Any], column_type: Any
    ) -> dict[str, Any]:
        """
        Calculate quality metrics based on column statistics.

        Args:
            stats: Pre-computed column statistics
            column_type: PySpark data type of the column

        Returns:
            Dictionary with quality metrics
        """
        # Basic quality metrics from stats
        null_percentage = stats.get("null_percentage", 0.0)
        distinct_percentage = stats.get("distinct_percentage", 0.0)
        non_null_count = stats.get("non_null_count", 0)

        quality_metrics = {
            "completeness": 1.0 - (null_percentage / 100.0),
            "uniqueness": distinct_percentage / 100.0 if non_null_count > 0 else 0.0,
            "null_count": stats.get("null_count", 0),
        }

        # Type-specific quality checks
        if isinstance(column_type, t.NumericType):
            self._add_numeric_quality(quality_metrics, stats)
        elif isinstance(column_type, t.StringType):
            self._add_string_quality(quality_metrics)

        # Calculate overall quality score
        quality_score = self._calculate_quality_score(quality_metrics)
        quality_metrics["quality_score"] = round(quality_score, 3)
        quality_metrics["column_type"] = self._get_type_name(column_type)

        return quality_metrics

    def _add_numeric_quality(
        self, quality_metrics: dict[str, Any], stats: dict[str, Any]
    ) -> None:
        """Add numeric-specific quality metrics."""
        # Check for numeric quality issues
        result = self.df.agg(
            F.count(F.when(F.col(self.column_name).isNaN(), 1)).alias("nan_count"),
            F.count(F.when(F.col(self.column_name) == float("inf"), 1)).alias(
                "inf_count"
            ),
            F.count(F.when(F.col(self.column_name) == float("-inf"), 1)).alias(
                "neg_inf_count"
            ),
        ).collect()[0]

        quality_metrics.update(
            {
                "nan_count": result["nan_count"],
                "infinity_count": result["inf_count"] + result["neg_inf_count"],
            }
        )

        # Add outlier percentage if available
        if "outliers" in stats:
            quality_metrics["outlier_percentage"] = stats["outliers"].get(
                "outlier_percentage", 0.0
            )

    def _add_string_quality(self, quality_metrics: dict[str, Any]) -> None:
        """Add string-specific quality metrics."""
        result = self.df.agg(
            F.count(F.when(F.trim(F.col(self.column_name)) == "", 1)).alias(
                "blank_count"
            ),
            F.count(F.when(F.col(self.column_name).rlike(r"[^\x00-\x7F]"), 1)).alias(
                "non_ascii_count"
            ),
            F.count(F.when(F.length(F.col(self.column_name)) == 1, 1)).alias(
                "single_char_count"
            ),
        ).collect()[0]

        quality_metrics.update(
            {
                "blank_count": result["blank_count"],
                "non_ascii_count": result["non_ascii_count"],
                "single_char_count": result["single_char_count"],
            }
        )

    def _calculate_quality_score(self, quality_metrics: dict[str, Any]) -> float:
        """Calculate overall quality score (0-1)."""
        quality_score = quality_metrics["completeness"]

        # Penalize for outliers in numeric columns
        if "outlier_percentage" in quality_metrics:
            outlier_penalty = min(
                quality_metrics["outlier_percentage"]
                / 100.0
                * QUALITY_OUTLIER_PENALTY_MAX,
                QUALITY_OUTLIER_PENALTY_MAX,
            )
            quality_score *= 1 - outlier_penalty

        # Penalize for low uniqueness in ID-like columns
        if (
            "id" in self.column_name.lower()
            and quality_metrics["uniqueness"] < ID_COLUMN_UNIQUENESS_THRESHOLD
        ):
            quality_score *= quality_metrics["uniqueness"]

        return float(quality_score)

    def _get_type_name(self, column_type: Any) -> str:
        """Get simplified type name for quality reporting."""
        if isinstance(column_type, t.NumericType):
            return "numeric"
        if isinstance(column_type, t.StringType):
            return "string"
        return "other"
