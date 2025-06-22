#!/usr/bin/env python3
"""
Progress Bar Demo
================

This example demonstrates the progress tracking feature of pyspark-analyzer.
The progress bar shows real-time updates during analysis of large DataFrames.

You can control progress bar behavior with:
- show_progress parameter in analyze()
- PYSPARK_ANALYZER_PROGRESS environment variable (always/never/auto)
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)
import random
import string
from pyspark_analyzer import analyze
import os


def generate_large_dataframe(spark, num_rows=5_000_000):
    """Generate a large DataFrame to demonstrate progress tracking."""
    print(f"Generating DataFrame with {num_rows:,} rows...")

    # Create schema
    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
            StructField("salary", DoubleType(), nullable=True),
            StructField("department", StringType(), nullable=True),
            StructField("score", DoubleType(), nullable=True),
        ]
    )

    # Generate data
    departments = ["Sales", "Engineering", "Marketing", "HR", "Finance", "Operations"]

    def generate_row(idx):
        return (
            idx,
            (
                "".join(random.choices(string.ascii_letters, k=10))
                if random.random() > 0.05
                else None
            ),
            random.randint(20, 65) if random.random() > 0.1 else None,
            random.uniform(30000, 150000) if random.random() > 0.08 else None,
            random.choice(departments) if random.random() > 0.03 else None,
            random.uniform(0, 100) if random.random() > 0.15 else None,
        )

    # Create RDD and convert to DataFrame
    rdd = spark.sparkContext.parallelize(range(num_rows)).map(generate_row)
    df = spark.createDataFrame(rdd, schema=schema)

    return df


def main():
    """Run the progress bar demonstration."""
    # Create Spark session
    spark = (
        SparkSession.builder.appName("ProgressBarDemo")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== PySpark Analyzer Progress Bar Demo ===\n")

    # Generate a large DataFrame
    df = generate_large_dataframe(spark, num_rows=1_000_000)

    print("\nDataFrame created successfully!")
    print(f"Total partitions: {df.rdd.getNumPartitions()}")

    # Demo 1: Progress bar enabled (default for large datasets)
    print("\n--- Demo 1: Progress bar enabled (default behavior) ---")
    print("Analyzing DataFrame with progress tracking...\n")

    profile1 = analyze(df, show_progress=True)
    print("\nAnalysis complete! Here's a sample of the results:")
    print(
        profile1[
            ["column_name", "data_type", "null_percentage", "distinct_count"]
        ].to_string()
    )

    # Demo 2: Force progress bar off
    print("\n\n--- Demo 2: Progress bar disabled ---")
    print("Analyzing same DataFrame without progress tracking...\n")

    analyze(df, show_progress=False)
    print("Analysis complete!")

    # Demo 3: Environment variable control
    print("\n\n--- Demo 3: Environment variable control ---")
    print("Setting PYSPARK_ANALYZER_PROGRESS=always")
    os.environ["PYSPARK_ANALYZER_PROGRESS"] = "always"

    # Create a smaller DataFrame to show progress always works
    small_df = df.limit(10000)
    print("\nAnalyzing smaller DataFrame (10k rows) with 'always' setting...\n")

    analyze(small_df)

    # Reset environment variable
    os.environ.pop("PYSPARK_ANALYZER_PROGRESS", None)

    # Show sampling info
    print("\n\n--- Sampling Information ---")
    print(f"Original dataset rows: {profile1.attrs['overview']['total_rows']:,}")
    sampling_info = profile1.attrs["sampling"]
    if sampling_info["is_sampled"]:
        print(f"Sample size used: {sampling_info['sample_size']:,}")
        print(f"Speedup achieved: {sampling_info['estimated_speedup']:.1f}x")
        print(f"Quality score: {sampling_info['quality_score']:.2f}")

    # Cleanup
    spark.stop()
    print("\n✅ Progress bar demo completed!")


if __name__ == "__main__":
    main()
