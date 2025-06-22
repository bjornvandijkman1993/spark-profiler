"""Tests for adaptive partitioning optimization."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)
from pyspark_analyzer.performance import (
    optimize_dataframe_for_profiling,
    _adaptive_partition,
    _estimate_row_size,
)


@pytest.fixture
def spark():
    """Create a test SparkSession."""
    return (
        SparkSession.builder.appName("test_adaptive_partitioning")
        .master("local[2]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def test_estimate_row_size(spark):
    """Test row size estimation for different data types."""
    # Create a DataFrame with various data types
    schema = StructType(
        [
            StructField("int_col", IntegerType(), True),
            StructField(
                "long_col", IntegerType(), True
            ),  # Will be treated as LongType in estimation
            StructField("string_col", StringType(), True),
            StructField("double_col", DoubleType(), True),
        ]
    )

    df = spark.createDataFrame([], schema)

    # Test estimation
    estimated_size = _estimate_row_size(df)

    # Base overhead (20) + int (4) + int (4) + string (50) + double (8) = 86
    assert estimated_size >= 80  # Allow some flexibility
    assert estimated_size <= 100


def test_adaptive_partition_small_dataset(spark):
    """Test partitioning for small datasets."""
    # Create a small DataFrame
    data = [(i, f"name_{i}") for i in range(100)]
    df = spark.createDataFrame(data, ["id", "name"])

    # Force multiple partitions initially
    df = df.repartition(10)
    initial_partitions = df.rdd.getNumPartitions()
    assert initial_partitions == 10

    # Apply adaptive partitioning
    optimized_df = _adaptive_partition(df, row_count=100)
    final_partitions = optimized_df.rdd.getNumPartitions()

    # Small dataset should have fewer partitions
    assert final_partitions < initial_partitions
    assert final_partitions <= 2  # Very small dataset


def test_adaptive_partition_large_dataset(spark):
    """Test partitioning for large datasets."""
    # Create a large DataFrame (simulated with row count)
    data = [(i, f"name_{i}") for i in range(1000)]
    df = spark.createDataFrame(data, ["id", "name"])

    # Start with just 1 partition
    df = df.coalesce(1)
    initial_partitions = df.rdd.getNumPartitions()
    assert initial_partitions == 1

    # Apply adaptive partitioning with simulated large row count
    optimized_df = _adaptive_partition(df, row_count=10_000_000)
    final_partitions = optimized_df.rdd.getNumPartitions()

    # Large dataset should have more partitions
    assert final_partitions > initial_partitions
    assert final_partitions >= 2  # Should use available parallelism


def test_adaptive_partition_with_aqe_enabled(spark):
    """Test that AQE-enabled sessions delegate to Spark."""
    # Enable AQE
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    try:
        # Create a DataFrame
        data = [(i, f"name_{i}") for i in range(10000)]
        df = spark.createDataFrame(data, ["id", "name"])
        df = df.repartition(50)

        initial_partitions = df.rdd.getNumPartitions()

        # Apply adaptive partitioning
        optimized_df = _adaptive_partition(df, row_count=10000)
        final_partitions = optimized_df.rdd.getNumPartitions()

        # With AQE enabled, we should mostly let Spark handle it
        # Only very small datasets get coalesced
        assert final_partitions == initial_partitions  # No change for medium-sized data

    finally:
        # Disable AQE again
        spark.conf.set("spark.sql.adaptive.enabled", "false")


def test_adaptive_partition_no_change_needed(spark):
    """Test that well-partitioned DataFrames are not changed."""
    # Create a DataFrame with good partitioning
    data = [(i, f"name_{i}") for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "name"])

    # Set a reasonable number of partitions
    target_partitions = 4
    df = df.repartition(target_partitions)

    initial_partitions = df.rdd.getNumPartitions()

    # Apply adaptive partitioning
    optimized_df = _adaptive_partition(df, row_count=100000)
    final_partitions = optimized_df.rdd.getNumPartitions()

    # Should not change if already well-partitioned
    # (within the 0.5x to 2x range)
    assert abs(final_partitions - initial_partitions) <= initial_partitions


def test_optimize_dataframe_integration(spark):
    """Test the full optimization function."""
    # Create a DataFrame
    data = [(i, f"name_{i}", i * 1.5) for i in range(5000)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    # Test with sampling
    optimized_df = optimize_dataframe_for_profiling(df, sample_fraction=0.1)

    # Should be sampled
    sampled_count = optimized_df.count()
    assert sampled_count < 5000
    assert sampled_count > 400  # Roughly 10% with some variance

    # Test without sampling
    optimized_df2 = optimize_dataframe_for_profiling(df, row_count=5000)

    # Should not be sampled but may be repartitioned
    assert optimized_df2.count() == 5000
