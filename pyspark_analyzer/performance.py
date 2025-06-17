"""
Performance optimization utilities for large dataset profiling.
"""

from typing import Optional
from pyspark.sql import DataFrame


def optimize_dataframe_for_profiling(
    df: DataFrame,
    sample_fraction: Optional[float] = None,
    row_count: Optional[int] = None,
) -> DataFrame:
    """
    Optimize DataFrame for profiling operations with lazy evaluation support.

    Args:
        df: Input DataFrame
        sample_fraction: If provided, sample the DataFrame to this fraction for faster profiling
        row_count: Optional known row count to avoid redundant count operation

    Returns:
        Optimized DataFrame
    """
    optimized_df = df

    # Sample if requested (note: sampling is now handled by SamplingDecisionEngine)
    if sample_fraction and 0 < sample_fraction < 1.0:
        optimized_df = optimized_df.sample(fraction=sample_fraction, seed=42)
        # If we sampled, the row count needs to be recalculated
        row_count = None

    # Use adaptive partitioning for better performance
    # Pass row_count to avoid unnecessary count operations in lazy evaluation context
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
    aqe_setting = spark.conf.get("spark.sql.adaptive.enabled", "false")
    aqe_enabled = aqe_setting.lower() == "true" if aqe_setting else False
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
    shuffle_partitions_setting = spark.conf.get("spark.sql.shuffle.partitions", "200")
    shuffle_partitions = (
        int(shuffle_partitions_setting) if shuffle_partitions_setting else 200
    )

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
