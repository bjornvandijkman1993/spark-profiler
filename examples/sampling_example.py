#!/usr/bin/env python3
"""
Example demonstrating sampling capabilities of the PySpark DataFrame Profiler.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from datetime import datetime
from pyspark_analyzer import DataFrameProfiler, SamplingConfig, create_sampling_config


def create_large_sample_data():
    """Create a larger sample DataFrame for sampling demonstration."""
    spark = (
        SparkSession.builder.appName("SamplingExample").master("local[*]").getOrCreate()
    )

    # Define schema
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("department", StringType(), True),
            StructField("hire_date", TimestampType(), True),
        ]
    )

    # Create a larger dataset for sampling demonstration
    data = []
    departments = ["Engineering", "Marketing", "Sales", "HR", "Finance"]
    names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"]

    for i in range(1, 50001):  # 50,000 rows
        data.append(
            (
                i,
                f"{names[i % len(names)]} {i}",
                25 + (i % 40),  # Ages 25-64
                50000.0 + (i % 50000),  # Salaries 50k-100k
                departments[i % len(departments)],
                datetime(2020 + (i % 4), ((i % 12) + 1), ((i % 28) + 1)),
            )
        )

    return spark.createDataFrame(data, schema)


def main():
    """Main example function demonstrating sampling features."""
    print("🚀 Creating large sample DataFrame...")
    df = create_large_sample_data()

    print(f"📊 Created DataFrame with {df.count():,} rows")

    print("\n" + "=" * 70)
    print("1. AUTO-SAMPLING (Default Behavior)")
    print("=" * 70)

    # Default profiler with auto-sampling
    profiler = DataFrameProfiler(df)
    profile = profiler.profile(output_format="dict")  # Get as dictionary

    sampling_info = profile["sampling"]
    print(f"Sampling applied: {sampling_info['is_sampled']}")
    if sampling_info["is_sampled"]:
        print(f"Original size: {sampling_info['original_size']:,} rows")
        print(f"Sample size: {sampling_info['sample_size']:,} rows")
        print(f"Sampling fraction: {sampling_info['sampling_fraction']:.4f}")
        print(f"Quality score: {sampling_info['quality_score']:.3f}")
        print(f"Estimated speedup: {sampling_info['estimated_speedup']:.1f}x")
        print(f"Sampling time: {sampling_info['sampling_time']:.2f} seconds")

    print("\n" + "=" * 70)
    print("2. CUSTOM SAMPLING CONFIGURATION")
    print("=" * 70)

    # Custom sampling with specific target size
    custom_config = SamplingConfig(
        target_size=10000,
        seed=42,
        auto_sample=False,  # Force sampling even for smaller datasets
    )

    profiler_custom = DataFrameProfiler(df, sampling_config=custom_config)
    profile_custom = profiler_custom.profile(output_format="dict")

    sampling_info = profile_custom["sampling"]
    print("Custom sampling results:")
    print(f"  Sample size: {sampling_info['sample_size']:,} rows")
    print(f"  Quality score: {sampling_info['quality_score']:.3f}")
    print(f"  Reduction ratio: {sampling_info['reduction_ratio']:.4f}")

    print("\n" + "=" * 70)
    print("3. FRACTION-BASED SAMPLING")
    print("=" * 70)

    # Fraction-based sampling
    fraction_config = create_sampling_config(
        target_fraction=0.02, seed=123
    )  # 2% sample

    profiler_fraction = DataFrameProfiler(df, sampling_config=fraction_config)
    profile_fraction = profiler_fraction.profile(output_format="dict")

    sampling_info = profile_fraction["sampling"]
    print("Fraction-based sampling results:")
    print("  Target fraction: 2%")
    print(f"  Actual sample size: {sampling_info['sample_size']:,} rows")
    print(f"  Actual fraction: {sampling_info['sampling_fraction']:.4f}")
    print(f"  Quality score: {sampling_info['quality_score']:.3f}")

    print("\n" + "=" * 70)
    print("4. PERFORMANCE COMPARISON")
    print("=" * 70)

    import time

    # Time full profiling
    print("⏱️  Timing full dataset profiling...")
    start_time = time.time()
    profiler_full = DataFrameProfiler(
        df, sampling_config=SamplingConfig(auto_sample=False)
    )
    profile_full = profiler_full.profile(output_format="dict")
    full_time = time.time() - start_time

    # Time sampled profiling
    print("⏱️  Timing sampled profiling...")
    start_time = time.time()
    profiler_sampled = DataFrameProfiler(df)  # Auto-sampling enabled
    profile_sampled = profiler_sampled.profile(output_format="dict")
    sampled_time = time.time() - start_time

    print("\nPerformance Results:")
    print(f"  Full dataset time: {full_time:.2f} seconds")
    print(f"  Sampled time: {sampled_time:.2f} seconds")
    print(f"  Actual speedup: {full_time / sampled_time:.1f}x")

    # Compare some statistics to show accuracy
    print("\nAccuracy Comparison (age column):")
    full_age_stats = profile_full["columns"]["age"]
    sampled_age_stats = profile_sampled["columns"]["age"]

    print(f"  Full dataset mean: {full_age_stats['mean']:.2f}")
    print(f"  Sampled mean: {sampled_age_stats['mean']:.2f}")
    print(
        f"  Difference: {abs(full_age_stats['mean'] - sampled_age_stats['mean']):.2f}"
    )

    print("\n" + "=" * 70)
    print("5. LEGACY COMPATIBILITY")
    print("=" * 70)

    # Show legacy sample_fraction parameter still works
    profiler_legacy = DataFrameProfiler(df, sample_fraction=0.01)
    profile_legacy = profiler_legacy.profile(output_format="dict")

    sampling_info = profile_legacy["sampling"]
    print("Legacy sample_fraction=0.01 results:")
    print(f"  Sample size: {sampling_info['sample_size']:,} rows")
    print(f"  Quality score: {sampling_info['quality_score']:.3f}")

    print("\n✅ Sampling demonstration completed!")

    # Clean up
    df.sparkSession.stop()


if __name__ == "__main__":
    main()
