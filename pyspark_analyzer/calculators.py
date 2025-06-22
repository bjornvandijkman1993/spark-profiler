"""Type-specific statistics calculators for DataFrame columns."""

from typing import Dict, Any, Optional
from abc import ABC, abstractmethod
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
    trim,
    desc,
    skewness,
    kurtosis,
    variance,
    sum as spark_sum,
    approx_count_distinct,
    upper,
    lower,
)
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

from .constants import (
    PATTERNS,
    OUTLIER_IQR_MULTIPLIER,
    APPROX_DISTINCT_RSD,
    DEFAULT_TOP_VALUES_LIMIT,
)
from .utils import escape_column_name
from .logging import get_logger

# Check if median function is available (PySpark 3.4.0+)
try:
    from pyspark.sql.functions import median

    HAS_MEDIAN = True
except ImportError:
    HAS_MEDIAN = False

logger = get_logger(__name__)


class BaseCalculator(ABC):
    """Base calculator for column statistics."""

    def __init__(self, df: DataFrame, column_name: str, total_rows: int):
        self.df = df
        self.column_name = column_name
        self.escaped_name = escape_column_name(column_name)
        self.total_rows = total_rows

    @abstractmethod
    def calculate(self) -> Dict[str, Any]:
        """Calculate statistics for the column."""
        pass

    def _calculate_basic_stats(self) -> Dict[str, Any]:
        """Calculate basic statistics common to all column types."""
        result = self.df.agg(
            count(col(self.escaped_name)).alias("non_null_count"),
            count(when(col(self.escaped_name).isNull(), 1)).alias("null_count"),
            approx_count_distinct(
                col(self.escaped_name), rsd=APPROX_DISTINCT_RSD
            ).alias("distinct_count"),
        ).collect()[0]

        non_null_count = result["non_null_count"]
        null_count = result["null_count"]
        distinct_count = result["distinct_count"]

        return {
            "total_count": self.total_rows,
            "non_null_count": non_null_count,
            "null_count": null_count,
            "null_percentage": (
                (null_count / self.total_rows * 100) if self.total_rows > 0 else 0.0
            ),
            "distinct_count": distinct_count,
            "distinct_percentage": (
                (distinct_count / non_null_count * 100) if non_null_count > 0 else 0.0
            ),
        }


class NumericCalculator(BaseCalculator):
    """Calculator for numeric column statistics."""

    def calculate(self) -> Dict[str, Any]:
        """Calculate numeric statistics."""
        stats = self._calculate_basic_stats()

        # Build aggregation expressions
        agg_exprs = [
            spark_min(col(self.escaped_name)).alias("min"),
            spark_max(col(self.escaped_name)).alias("max"),
            mean(col(self.escaped_name)).alias("mean"),
            stddev(col(self.escaped_name)).alias("std"),
            skewness(col(self.escaped_name)).alias("skewness"),
            kurtosis(col(self.escaped_name)).alias("kurtosis"),
            variance(col(self.escaped_name)).alias("variance"),
            spark_sum(col(self.escaped_name)).alias("sum"),
            count(when(col(self.escaped_name) == 0, 1)).alias("zero_count"),
            count(when(col(self.escaped_name) < 0, 1)).alias("negative_count"),
        ]

        # Add median
        if HAS_MEDIAN:
            agg_exprs.append(median(col(self.escaped_name)).alias("median"))
        else:
            agg_exprs.append(
                expr(f"percentile_approx({self.escaped_name}, 0.5)").alias("median")
            )

        # Add percentiles
        percentiles = [0.25, 0.75, 0.05, 0.95]
        for p in percentiles:
            agg_exprs.append(
                expr(f"percentile_approx({self.escaped_name}, {p})").alias(
                    f"p{int(p*100)}"
                )
            )

        # Execute aggregation
        result = self.df.agg(*agg_exprs).collect()[0]

        # Extract values
        stats.update(
            {
                "min": result["min"],
                "max": result["max"],
                "mean": result["mean"],
                "std": result["std"] if result["std"] is not None else 0.0,
                "median": result["median"],
                "q1": result["p25"],
                "q3": result["p75"],
                "p5": result["p5"],
                "p95": result["p95"],
                "skewness": result["skewness"],
                "kurtosis": result["kurtosis"],
                "variance": result["variance"],
                "sum": result["sum"],
                "zero_count": result["zero_count"],
                "negative_count": result["negative_count"],
            }
        )

        # Calculate derived statistics
        if result["min"] is not None and result["max"] is not None:
            stats["range"] = result["max"] - result["min"]

        if result["p25"] is not None and result["p75"] is not None:
            stats["iqr"] = result["p75"] - result["p25"]

        # Coefficient of variation
        if result["mean"] and result["mean"] != 0 and result["std"]:
            stats["cv"] = abs(result["std"] / result["mean"])

        # Add outlier detection
        outliers = self._calculate_outliers(result["p25"], result["p75"])
        stats["outliers"] = outliers

        return stats

    def _calculate_outliers(
        self, q1: Optional[float], q3: Optional[float]
    ) -> Dict[str, Any]:
        """Calculate outlier statistics using IQR method."""
        if q1 is None or q3 is None:
            return {"outlier_count": 0, "outlier_percentage": 0.0}

        iqr = q3 - q1
        lower_bound = q1 - OUTLIER_IQR_MULTIPLIER * iqr
        upper_bound = q3 + OUTLIER_IQR_MULTIPLIER * iqr

        outlier_result = self.df.agg(
            count(when(col(self.escaped_name) < lower_bound, 1)).alias(
                "lower_outliers"
            ),
            count(when(col(self.escaped_name) > upper_bound, 1)).alias(
                "upper_outliers"
            ),
        ).collect()[0]

        lower_count = outlier_result["lower_outliers"]
        upper_count = outlier_result["upper_outliers"]
        total_outliers = lower_count + upper_count

        return {
            "method": "iqr",
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "outlier_count": total_outliers,
            "outlier_percentage": (
                (total_outliers / self.total_rows * 100) if self.total_rows > 0 else 0.0
            ),
            "lower_outlier_count": lower_count,
            "upper_outlier_count": upper_count,
        }


class StringCalculator(BaseCalculator):
    """Calculator for string column statistics."""

    def calculate(self) -> Dict[str, Any]:
        """Calculate string statistics."""
        stats = self._calculate_basic_stats()

        # String-specific aggregations
        result = self.df.agg(
            spark_min(length(col(self.escaped_name))).alias("min_length"),
            spark_max(length(col(self.escaped_name))).alias("max_length"),
            mean(length(col(self.escaped_name))).alias("avg_length"),
            count(when(col(self.escaped_name) == "", 1)).alias("empty_count"),
            count(
                when(trim(col(self.escaped_name)) != col(self.escaped_name), 1)
            ).alias("has_whitespace_count"),
        ).collect()[0]

        stats.update(
            {
                "min_length": result["min_length"],
                "max_length": result["max_length"],
                "avg_length": result["avg_length"],
                "empty_count": result["empty_count"],
                "has_whitespace_count": result["has_whitespace_count"],
            }
        )

        # Pattern detection
        patterns = self._detect_patterns()
        stats["patterns"] = patterns

        # Top frequent values
        top_values = self._get_top_values()
        stats["top_values"] = top_values

        return stats

    def _detect_patterns(self) -> Dict[str, int]:
        """Detect common patterns in string values."""
        pattern_counts = {}

        # Email pattern
        email_count = self.df.filter(
            col(self.escaped_name).rlike(PATTERNS["email"])
        ).count()
        pattern_counts["email_count"] = email_count

        # URL pattern
        url_count = self.df.filter(
            col(self.escaped_name).rlike(PATTERNS["url"])
        ).count()
        pattern_counts["url_count"] = url_count

        # Phone-like pattern
        phone_count = self.df.filter(
            col(self.escaped_name).rlike(PATTERNS["phone"])
        ).count()
        pattern_counts["phone_like_count"] = phone_count

        # Numeric string
        numeric_count = self.df.filter(
            col(self.escaped_name).rlike(PATTERNS["numeric_string"])
        ).count()
        pattern_counts["numeric_string_count"] = numeric_count

        # Case patterns
        uppercase_count = self.df.filter(
            (col(self.escaped_name).isNotNull())
            & (col(self.escaped_name) == upper(col(self.escaped_name)))
        ).count()
        pattern_counts["uppercase_count"] = uppercase_count

        lowercase_count = self.df.filter(
            (col(self.escaped_name).isNotNull())
            & (col(self.escaped_name) == lower(col(self.escaped_name)))
        ).count()
        pattern_counts["lowercase_count"] = lowercase_count

        return pattern_counts

    def _get_top_values(self, limit: int = DEFAULT_TOP_VALUES_LIMIT) -> list:
        """Get top frequent values."""
        top_values = (
            self.df.filter(col(self.escaped_name).isNotNull())
            .groupBy(self.column_name)
            .count()
            .orderBy(desc("count"))
            .limit(limit)
            .collect()
        )

        return [
            {"value": row[self.column_name], "count": row["count"]}
            for row in top_values
        ]


class TemporalCalculator(BaseCalculator):
    """Calculator for temporal column statistics."""

    def calculate(self) -> Dict[str, Any]:
        """Calculate temporal statistics."""
        stats = self._calculate_basic_stats()

        # Temporal-specific aggregations
        result = self.df.agg(
            spark_min(col(self.escaped_name)).alias("min_date"),
            spark_max(col(self.escaped_name)).alias("max_date"),
        ).collect()[0]

        min_date = result["min_date"]
        max_date = result["max_date"]

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

        return stats


def create_calculator(
    df: DataFrame, column_name: str, column_type: Any, total_rows: int
) -> BaseCalculator:
    """Factory function to create appropriate calculator based on column type."""
    if isinstance(column_type, NumericType):
        return NumericCalculator(df, column_name, total_rows)
    elif isinstance(column_type, StringType):
        return StringCalculator(df, column_name, total_rows)
    elif isinstance(column_type, (TimestampType, DateType)):
        return TemporalCalculator(df, column_name, total_rows)
    else:
        # For other types (arrays, structs, etc.), return basic calculator
        return BasicCalculator(df, column_name, total_rows)


class BasicCalculator(BaseCalculator):
    """Calculator for basic statistics only (used for complex types)."""

    def calculate(self) -> Dict[str, Any]:
        """Calculate only basic statistics."""
        return self._calculate_basic_stats()
