"""
Performance optimization utilities for large dataset profiling.
"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError, Py4JJavaError

from .exceptions import SparkOperationError
from .logging import get_logger

logger = get_logger(__name__)


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
    logger.debug(f"Optimizing DataFrame with row_count={row_count}")
    optimized_df = df

    # Sample if requested (note: sampling is now handled by SamplingDecisionEngine)
    if sample_fraction and 0 < sample_fraction < 1.0:
        logger.warning(
            f"Legacy sampling detected with fraction={sample_fraction}. "
            "Consider using SamplingConfig instead."
        )
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
    try:
        aqe_setting = spark.conf.get("spark.sql.adaptive.enabled", "false")
        aqe_enabled = aqe_setting.lower() == "true" if aqe_setting else False
    except Exception as e:
        logger.warning(f"Could not check AQE setting, assuming disabled: {str(e)}")
        aqe_enabled = False
    if aqe_enabled:
        logger.debug(
            "Adaptive Query Execution is enabled, deferring to Spark optimization"
        )
        # With AQE enabled, Spark will automatically optimize partitions
        # We only need to handle extreme cases
        current_partitions = df.rdd.getNumPartitions()

        # Only intervene for very small datasets
        if row_count is not None and row_count < 1000 and current_partitions > 1:
            logger.info(
                f"Coalescing small dataset from {current_partitions} to 1 partition"
            )
            return df.coalesce(1)

        # Let AQE handle the rest
        logger.debug(
            f"AQE will handle optimization for {current_partitions} partitions"
        )
        return df

    # Manual partition optimization when AQE is disabled
    current_partitions = df.rdd.getNumPartitions()
    logger.debug(
        f"Manual partition optimization: current partitions = {current_partitions}"
    )

    # Get cluster configuration hints
    try:
        default_parallelism = spark.sparkContext.defaultParallelism
        shuffle_partitions_setting = spark.conf.get(
            "spark.sql.shuffle.partitions", "200"
        )
        shuffle_partitions = (
            int(shuffle_partitions_setting) if shuffle_partitions_setting else 200
        )
        logger.debug(
            f"Cluster config: default_parallelism={default_parallelism}, "
            f"shuffle_partitions={shuffle_partitions}"
        )
    except Exception as e:
        logger.warning(f"Could not get cluster configuration, using defaults: {str(e)}")
        default_parallelism = 8
        shuffle_partitions = 200

    # Optimal partition size targets (in bytes)
    # These are based on Spark best practices
    target_partition_bytes = 128 * 1024 * 1024  # 128 MB

    # If we don't have row count, get it
    if row_count is None:
        try:
            row_count = df.count()
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(f"Failed to count DataFrame rows for optimization: {str(e)}")
            raise SparkOperationError(
                f"Failed to count DataFrame rows during optimization: {str(e)}", e
            )

    # Estimate average row size (this is a heuristic)
    # For profiling, we typically deal with mixed data types
    estimated_row_bytes = _estimate_row_size(df)
    estimated_total_bytes = row_count * estimated_row_bytes

    # Calculate optimal partition count based on data size
    optimal_partitions = int(estimated_total_bytes / target_partition_bytes)
    optimal_partitions = max(1, optimal_partitions)  # At least 1 partition

    logger.debug(
        f"Data size estimate: {estimated_total_bytes / (1024*1024):.2f} MB, "
        f"target partitions: {optimal_partitions}"
    )

    # Apply cluster-aware bounds
    # Don't exceed shuffle partitions or create too many small partitions
    optimal_partitions = min(optimal_partitions, shuffle_partitions)
    # Ensure we use available parallelism but not excessively
    optimal_partitions = min(optimal_partitions, default_parallelism * 4)

    # Special cases based on data size
    if row_count < 10000:
        # Very small dataset - minimize overhead
        optimal_partitions = min(optimal_partitions, max(1, default_parallelism // 4))
        logger.debug(f"Small dataset optimization: {optimal_partitions} partitions")
    elif row_count > 10000000:
        # Very large dataset - ensure sufficient parallelism
        optimal_partitions = max(optimal_partitions, default_parallelism)
        logger.debug(f"Large dataset optimization: {optimal_partitions} partitions")

    # Only repartition if there's a significant difference
    partition_ratio = (
        current_partitions / optimal_partitions if optimal_partitions > 0 else 1
    )

    if partition_ratio > 2.0 or partition_ratio < 0.5:
        # Significant difference - worth repartitioning
        if optimal_partitions < current_partitions:
            # Reduce partitions - use coalesce to avoid shuffle
            logger.info(
                f"Coalescing partitions: {current_partitions} -> {optimal_partitions} "
                f"(ratio: {partition_ratio:.2f})"
            )
            return df.coalesce(optimal_partitions)
        else:
            # Increase partitions - requires shuffle
            # Consider using repartitionByRange for better distribution if there's a sortable key
            logger.info(
                f"Repartitioning: {current_partitions} -> {optimal_partitions} "
                f"(ratio: {partition_ratio:.2f})"
            )
            return df.repartition(optimal_partitions)

    # No significant benefit from repartitioning
    logger.debug(
        f"No repartitioning needed: current={current_partitions}, "
        f"optimal={optimal_partitions}, ratio={partition_ratio:.2f}"
    )
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
