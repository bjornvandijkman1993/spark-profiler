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
    df: DataFrame,
    sample_fraction: Optional[float] = None,
    row_count: Optional[int] = None,
) -> DataFrame:
    """
    Optimize DataFrame for profiling operations.

    Args:
        df: Input DataFrame
        sample_fraction: If provided, sample the DataFrame to this fraction for faster profiling
        row_count: Optional known row count to avoid redundant count operation

    Returns:
        Optimized DataFrame
    """
    optimized_df = df

    # Sample if requested
    if sample_fraction and 0 < sample_fraction < 1.0:
        optimized_df = optimized_df.sample(fraction=sample_fraction, seed=42)
        # If we sampled, the row count needs to be recalculated
        row_count = None

    # Use adaptive partitioning for better performance
    optimized_df = _adaptive_partition(optimized_df, row_count)

    return optimized_df


def _adaptive_partition(df: DataFrame, row_count: Optional[int] = None) -> DataFrame:
    """
    Intelligently partition DataFrame based on data characteristics and cluster configuration.

    This function considers:
    - Spark's Adaptive Query Execution (AQE) settings
    - Data size and characteristics
    - Current partition count and target partition size
    - Data skew detection (when possible)

    Args:
        df: Input DataFrame
        row_count: Known row count to avoid recomputation

    Returns:
        DataFrame with optimized partitioning
    """
    spark = df.sparkSession

    # Check if AQE is enabled - if so, let Spark handle partition optimization
    aqe_enabled = (
        spark.conf.get("spark.sql.adaptive.enabled", "false").lower() == "true"
    )
    if aqe_enabled:
        # With AQE enabled, Spark will automatically optimize partitions
        # We only need to handle extreme cases
        current_partitions = df.rdd.getNumPartitions()

        # Only intervene for very small datasets
        if row_count is not None and row_count < 1000 and current_partitions > 1:
            return df.coalesce(1)

        # Let AQE handle the rest
        return df

    # Manual partition optimization when AQE is disabled
    current_partitions = df.rdd.getNumPartitions()

    # Get cluster configuration hints
    default_parallelism = spark.sparkContext.defaultParallelism
    shuffle_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))

    # Optimal partition size targets (in bytes)
    # These are based on Spark best practices
    target_partition_bytes = 128 * 1024 * 1024  # 128 MB

    # If we don't have row count, get it
    if row_count is None:
        row_count = df.count()

    # Estimate average row size (this is a heuristic)
    # For profiling, we typically deal with mixed data types
    estimated_row_bytes = _estimate_row_size(df)
    estimated_total_bytes = row_count * estimated_row_bytes

    # Calculate optimal partition count based on data size
    optimal_partitions = int(estimated_total_bytes / target_partition_bytes)
    optimal_partitions = max(1, optimal_partitions)  # At least 1 partition

    # Apply cluster-aware bounds
    # Don't exceed shuffle partitions or create too many small partitions
    optimal_partitions = min(optimal_partitions, shuffle_partitions)
    # Ensure we use available parallelism but not excessively
    optimal_partitions = min(optimal_partitions, default_parallelism * 4)

    # Special cases based on data size
    if row_count < 10000:
        # Very small dataset - minimize overhead
        optimal_partitions = min(optimal_partitions, max(1, default_parallelism // 4))
    elif row_count > 10000000:
        # Very large dataset - ensure sufficient parallelism
        optimal_partitions = max(optimal_partitions, default_parallelism)

    # Only repartition if there's a significant difference
    partition_ratio = (
        current_partitions / optimal_partitions if optimal_partitions > 0 else 1
    )

    if partition_ratio > 2.0 or partition_ratio < 0.5:
        # Significant difference - worth repartitioning
        if optimal_partitions < current_partitions:
            # Reduce partitions - use coalesce to avoid shuffle
            return df.coalesce(optimal_partitions)
        else:
            # Increase partitions - requires shuffle
            # Consider using repartitionByRange for better distribution if there's a sortable key
            return df.repartition(optimal_partitions)

    # No significant benefit from repartitioning
    return df


def _estimate_row_size(df: DataFrame) -> int:
    """
    Estimate average row size in bytes based on schema.

    This is a heuristic estimation based on column data types.

    Args:
        df: Input DataFrame

    Returns:
        Estimated bytes per row
    """
    # Base overhead per row
    row_overhead = 20  # bytes

    # Estimate based on data types
    total_size = row_overhead

    for field in df.schema.fields:
        dtype = field.dataType
        dtype_str = str(dtype)

        # Estimate size based on data type
        if "IntegerType" in dtype_str:
            total_size += 4
        elif "LongType" in dtype_str or "DoubleType" in dtype_str:
            total_size += 8
        elif "FloatType" in dtype_str:
            total_size += 4
        elif "BooleanType" in dtype_str:
            total_size += 1
        elif "DateType" in dtype_str:
            total_size += 8
        elif "TimestampType" in dtype_str:
            total_size += 12
        elif "StringType" in dtype_str:
            # Strings are variable - use a conservative estimate
            total_size += 50  # Average string length assumption
        elif "DecimalType" in dtype_str:
            total_size += 16
        elif "BinaryType" in dtype_str:
            total_size += 100  # Conservative estimate for binary data
        elif (
            "ArrayType" in dtype_str
            or "MapType" in dtype_str
            or "StructType" in dtype_str
        ):
            # Complex types - harder to estimate
            total_size += 200
        else:
            # Unknown type - conservative estimate
            total_size += 50

    return total_size
