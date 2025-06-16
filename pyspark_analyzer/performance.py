"""
Performance optimization utilities for large dataset profiling.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DataType


class BatchStatisticsComputer:
    """
    Optimized statistics computer for large datasets using batch processing.

    This class combines multiple statistics computations into single operations
    to minimize the number of passes over the data.
    """

    def __init__(self, dataframe: DataFrame, total_rows: Optional[int] = None):
        """
        Initialize with a PySpark DataFrame.

        Args:
            dataframe: PySpark DataFrame to compute statistics for
            total_rows: Cached row count to avoid recomputation
        """
        self.df = dataframe
        self.cache_enabled = False
        self.total_rows = total_rows

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
        self, columns: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compute statistics for multiple columns in batch operations.

        This method optimizes performance by:
        1. Combining multiple aggregations into single operations
        2. Using approximate functions where possible
        3. Minimizing data shuffling

        Args:
            columns: List of columns to profile. If None, profiles all columns.

        Returns:
            Dictionary mapping column names to their statistics
        """
        if columns is None:
            columns = self.df.columns

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
                        column, column_types[column]
                    )
                    all_agg_exprs.extend(agg_exprs)

            # Execute single aggregation for all columns
            if all_agg_exprs:
                result_row = self.df.agg(*all_agg_exprs).collect()[0]

                # Get total rows if not cached
                total_rows = (
                    self.total_rows if self.total_rows is not None else self.df.count()
                )

                # Extract results for each column
                results = {}
                for column in columns_to_process:
                    results[column] = self._extract_column_stats(
                        column, column_types[column], result_row, total_rows
                    )

                return results
            else:
                return {}
        finally:
            # Always clean up caching
            self.disable_caching()

    def _build_column_agg_exprs(
        self, column_name: str, column_type: DataType
    ) -> List[Any]:
        """
        Build aggregation expressions for a single column.

        Args:
            column_name: Name of the column
            column_type: PySpark data type of the column

        Returns:
            List of aggregation expressions for this column
        """
        from pyspark.sql.functions import (
            count,
            when,
            min as spark_min,
            max as spark_max,
            mean,
            stddev,
            expr,
            length,
            approx_count_distinct,
            col,
        )
        from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

        # Build aggregation expressions based on column type
        agg_exprs = [
            count(col(column_name)).alias(f"{column_name}_non_null_count"),
            count(when(col(column_name).isNull(), 1)).alias(
                f"{column_name}_null_count"
            ),
            approx_count_distinct(col(column_name), rsd=0.05).alias(
                f"{column_name}_distinct_count"
            ),
        ]

        # Add type-specific aggregations
        if isinstance(column_type, NumericType):
            agg_exprs.extend(
                [
                    spark_min(col(column_name)).alias(f"{column_name}_min"),
                    spark_max(col(column_name)).alias(f"{column_name}_max"),
                    mean(col(column_name)).alias(f"{column_name}_mean"),
                    stddev(col(column_name)).alias(f"{column_name}_std"),
                    expr(f"percentile_approx({column_name}, 0.5)").alias(
                        f"{column_name}_median"
                    ),
                    expr(f"percentile_approx({column_name}, 0.25)").alias(
                        f"{column_name}_q1"
                    ),
                    expr(f"percentile_approx({column_name}, 0.75)").alias(
                        f"{column_name}_q3"
                    ),
                ]
            )
        elif isinstance(column_type, StringType):
            agg_exprs.extend(
                [
                    spark_min(length(col(column_name))).alias(
                        f"{column_name}_min_length"
                    ),
                    spark_max(length(col(column_name))).alias(
                        f"{column_name}_max_length"
                    ),
                    mean(length(col(column_name))).alias(f"{column_name}_avg_length"),
                    count(when(col(column_name) == "", 1)).alias(
                        f"{column_name}_empty_count"
                    ),
                ]
            )
        elif isinstance(column_type, (TimestampType, DateType)):
            agg_exprs.extend(
                [
                    spark_min(col(column_name)).alias(f"{column_name}_min_date"),
                    spark_max(col(column_name)).alias(f"{column_name}_max_date"),
                ]
            )

        return agg_exprs

    def _extract_column_stats(
        self, column_name: str, column_type: DataType, result_row: Any, total_rows: int
    ) -> Dict[str, Any]:
        """
        Extract statistics for a single column from the aggregation result.

        Args:
            column_name: Name of the column
            column_type: PySpark data type of the column
            result_row: Row containing all aggregation results
            total_rows: Total number of rows in the DataFrame

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
            stats.update(
                {
                    "min": result_row[f"{column_name}_min"],
                    "max": result_row[f"{column_name}_max"],
                    "mean": result_row[f"{column_name}_mean"],
                    "std": (
                        result_row[f"{column_name}_std"]
                        if result_row[f"{column_name}_std"] is not None
                        else 0.0
                    ),
                    "median": result_row[f"{column_name}_median"],
                    "q1": result_row[f"{column_name}_q1"],
                    "q3": result_row[f"{column_name}_q3"],
                }
            )
        elif isinstance(column_type, StringType):
            stats.update(
                {
                    "min_length": result_row[f"{column_name}_min_length"],
                    "max_length": result_row[f"{column_name}_max_length"],
                    "avg_length": result_row[f"{column_name}_avg_length"],
                    "empty_count": result_row[f"{column_name}_empty_count"],
                }
            )
        elif isinstance(column_type, (TimestampType, DateType)):
            min_date = result_row[f"{column_name}_min_date"]
            max_date = result_row[f"{column_name}_max_date"]

            date_range_days = None
            if min_date and max_date:
                try:
                    date_range_days = (max_date - min_date).days
                except (AttributeError, TypeError):
                    # Log the error or handle it appropriately
                    # For now, we'll set date_range_days to None to indicate calculation failed
                    date_range_days = None

            stats.update(
                {
                    "min_date": min_date,
                    "max_date": max_date,
                    "date_range_days": date_range_days,
                }
            )

        return stats

    def _compute_column_stats_optimized(
        self, column_name: str, column_type: DataType
    ) -> Dict[str, Any]:
        """
        Compute optimized statistics for a single column.

        Args:
            column_name: Name of the column
            column_type: PySpark data type of the column

        Returns:
            Dictionary with column statistics
        """
        from pyspark.sql.functions import (
            count,
            when,
            min as spark_min,
            max as spark_max,
            mean,
            stddev,
            expr,
            length,
            approx_count_distinct,
        )
        from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

        # Build aggregation expressions based on column type
        agg_exprs = [
            count(col(column_name)).alias(f"{column_name}_non_null_count"),
            count(when(col(column_name).isNull(), 1)).alias(
                f"{column_name}_null_count"
            ),
            approx_count_distinct(col(column_name), rsd=0.05).alias(
                f"{column_name}_distinct_count"
            ),
        ]

        # Add type-specific aggregations
        if isinstance(column_type, NumericType):
            agg_exprs.extend(
                [
                    spark_min(col(column_name)).alias(f"{column_name}_min"),
                    spark_max(col(column_name)).alias(f"{column_name}_max"),
                    mean(col(column_name)).alias(f"{column_name}_mean"),
                    stddev(col(column_name)).alias(f"{column_name}_std"),
                    expr(f"percentile_approx({column_name}, 0.5)").alias(
                        f"{column_name}_median"
                    ),
                    expr(f"percentile_approx({column_name}, 0.25)").alias(
                        f"{column_name}_q1"
                    ),
                    expr(f"percentile_approx({column_name}, 0.75)").alias(
                        f"{column_name}_q3"
                    ),
                ]
            )
        elif isinstance(column_type, StringType):
            agg_exprs.extend(
                [
                    spark_min(length(col(column_name))).alias(
                        f"{column_name}_min_length"
                    ),
                    spark_max(length(col(column_name))).alias(
                        f"{column_name}_max_length"
                    ),
                    mean(length(col(column_name))).alias(f"{column_name}_avg_length"),
                    count(when(col(column_name) == "", 1)).alias(
                        f"{column_name}_empty_count"
                    ),
                ]
            )
        elif isinstance(column_type, (TimestampType, DateType)):
            agg_exprs.extend(
                [
                    spark_min(col(column_name)).alias(f"{column_name}_min_date"),
                    spark_max(col(column_name)).alias(f"{column_name}_max_date"),
                ]
            )

        # Execute single aggregation
        result = self.df.agg(*agg_exprs).collect()[0]

        # Extract results and compute derived metrics
        total_rows = self.total_rows if self.total_rows is not None else self.df.count()
        non_null_count = result[f"{column_name}_non_null_count"]
        null_count = result[f"{column_name}_null_count"]
        distinct_count = result[f"{column_name}_distinct_count"]

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
            stats.update(
                {
                    "min": result[f"{column_name}_min"],
                    "max": result[f"{column_name}_max"],
                    "mean": result[f"{column_name}_mean"],
                    "std": (
                        result[f"{column_name}_std"]
                        if result[f"{column_name}_std"] is not None
                        else 0.0
                    ),
                    "median": result[f"{column_name}_median"],
                    "q1": result[f"{column_name}_q1"],
                    "q3": result[f"{column_name}_q3"],
                }
            )
        elif isinstance(column_type, StringType):
            stats.update(
                {
                    "min_length": result[f"{column_name}_min_length"],
                    "max_length": result[f"{column_name}_max_length"],
                    "avg_length": result[f"{column_name}_avg_length"],
                    "empty_count": result[f"{column_name}_empty_count"],
                }
            )
        elif isinstance(column_type, (TimestampType, DateType)):
            min_date = result[f"{column_name}_min_date"]
            max_date = result[f"{column_name}_max_date"]

            date_range_days = None
            if min_date and max_date:
                try:
                    date_range_days = (max_date - min_date).days
                except (AttributeError, TypeError):
                    # Log the error or handle it appropriately
                    # For now, we'll set date_range_days to None to indicate calculation failed
                    date_range_days = None

            stats.update(
                {
                    "min_date": min_date,
                    "max_date": max_date,
                    "date_range_days": date_range_days,
                }
            )

        return stats


def optimize_dataframe_for_profiling(
    df: DataFrame, sample_fraction: Optional[float] = None
) -> DataFrame:
    """
    Optimize DataFrame for profiling operations.

    Args:
        df: Input DataFrame
        sample_fraction: If provided, sample the DataFrame to this fraction for faster profiling

    Returns:
        Optimized DataFrame
    """
    optimized_df = df

    # Sample if requested
    if sample_fraction and 0 < sample_fraction < 1.0:
        optimized_df = optimized_df.sample(fraction=sample_fraction, seed=42)

    # Repartition for better parallelism if the DataFrame is very small or very large
    row_count = optimized_df.count()
    current_partitions = optimized_df.rdd.getNumPartitions()

    # Heuristic: aim for 10MB-128MB per partition
    # Assuming average row size, adjust partitions accordingly
    if row_count < 1000 and current_partitions > 1:
        # Too few rows for multiple partitions
        optimized_df = optimized_df.coalesce(1)
    elif row_count > 1000000 and current_partitions < 8:
        # Large dataset might benefit from more partitions
        target_partitions = min(200, max(8, row_count // 100000))
        optimized_df = optimized_df.repartition(int(target_partitions))

    return optimized_df
