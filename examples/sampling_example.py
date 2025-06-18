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
from pyspark_analyzer import DataFrameProfiler, SamplingConfig


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
    # For datasets > 10M rows, auto-sampling kicks in automatically
    profiler = DataFrameProfiler(df)
    profile = profiler.profile(output_format="dict")

    sampling_info = profile["sampling"]
    print(f"Sampling applied: {sampling_info['is_sampled']}")
    if sampling_info["is_sampled"]:
        print(f"Original size: {sampling_info['original_size']:,} rows")
        print(f"Sample size: {sampling_info['sample_size']:,} rows")
        print(f"Sampling fraction: {sampling_info['sampling_fraction']:.4f}")
        print(f"Estimated speedup: {sampling_info['estimated_speedup']:.1f}x")
        print(f"Sampling time: {sampling_info['sampling_time']:.2f} seconds")

    print("\n" + "=" * 70)
    print("2. DISABLE SAMPLING")
    print("=" * 70)

    # Disable sampling completely
    no_sampling_config = SamplingConfig(enabled=False)
    profiler_no_sample = DataFrameProfiler(df, sampling_config=no_sampling_config)
    profile_no_sample = profiler_no_sample.profile(output_format="dict")

    print(f"Sampling applied: {profile_no_sample['sampling']['is_sampled']}")
    print(f"Rows processed: {profile_no_sample['sampling']['sample_size']:,}")

    print("\n" + "=" * 70)
    print("3. SAMPLE TO SPECIFIC NUMBER OF ROWS")
    print("=" * 70)

    # Sample to exactly 10,000 rows
    target_config = SamplingConfig(target_rows=10000, seed=42)  # For reproducibility

    profiler_target = DataFrameProfiler(df, sampling_config=target_config)
    profile_target = profiler_target.profile(output_format="dict")

    sampling_info = profile_target["sampling"]
    print("Target rows: 10,000")
    print(f"Actual sample size: {sampling_info['sample_size']:,} rows")
    print(f"Sampling fraction: {sampling_info['sampling_fraction']:.4f}")

    print("\n" + "=" * 70)
    print("4. FRACTION-BASED SAMPLING")
    print("=" * 70)

    # Sample 2% of the data
    fraction_config = SamplingConfig(fraction=0.02, seed=123)

    profiler_fraction = DataFrameProfiler(df, sampling_config=fraction_config)
    profile_fraction = profiler_fraction.profile(output_format="dict")

    sampling_info = profile_fraction["sampling"]
    print("Target fraction: 2%")
    print(f"Actual sample size: {sampling_info['sample_size']:,} rows")
    print(f"Actual fraction: {sampling_info['sampling_fraction']:.4f}")

    print("\n" + "=" * 70)
    print("5. CUSTOM AUTO-SAMPLING THRESHOLD")
    print("=" * 70)

    # Lower the auto-sampling threshold to 25,000 rows
    custom_threshold_config = SamplingConfig(
        auto_threshold=25000  # Auto-sample datasets larger than 25k rows
    )

    profiler_custom = DataFrameProfiler(df, sampling_config=custom_threshold_config)
    profile_custom = profiler_custom.profile(output_format="dict")

    sampling_info = profile_custom["sampling"]
    print("Auto-sampling threshold: 25,000 rows")
    print("Dataset size: 50,000 rows")
    print(f"Sampling applied: {sampling_info['is_sampled']}")
    print(f"Sample size: {sampling_info['sample_size']:,} rows")

    print("\n" + "=" * 70)
    print("6. PERFORMANCE COMPARISON")
    print("=" * 70)

    import time

    # Time full profiling
    print("⏱️  Timing full dataset profiling...")
    start_time = time.time()
    profiler_full = DataFrameProfiler(df, sampling_config=SamplingConfig(enabled=False))
    profile_full = profiler_full.profile(output_format="dict")
    full_time = time.time() - start_time

    # Time sampled profiling
    print("⏱️  Timing sampled profiling (10% sample)...")
    start_time = time.time()
    profiler_sampled = DataFrameProfiler(
        df, sampling_config=SamplingConfig(fraction=0.1)
    )
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

    print("\n✅ Sampling demonstration completed!")

    # Clean up
    df.sparkSession.stop()


if __name__ == "__main__":
    main()
