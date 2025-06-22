#!/usr/bin/env python3
"""
Example demonstrating progress tracking during DataFrame profiling.
"""

import os
from pyspark.sql import SparkSession
from pyspark_analyzer import analyze

# Create Spark session
spark = SparkSession.builder.appName("ProgressExample").master("local[*]").getOrCreate()

# Create a larger sample dataset to see progress
print("Creating sample dataset...")
data = [
    (i, f"Product_{i}", 10.0 + i * 0.5, i % 5, f"user_{i % 100}") for i in range(10000)
]
df = spark.createDataFrame(data, ["id", "product_name", "price", "category", "user_id"])

print("\n" + "=" * 60)
print("Example 1: Default progress tracking (auto-detect)")
print("=" * 60)
# Auto-detects if we're in a terminal
profile = analyze(df)
print("Profile completed!")

print("\n" + "=" * 60)
print("Example 2: Force progress tracking ON")
print("=" * 60)
profile = analyze(df, show_progress=True)

print("\n" + "=" * 60)
print("Example 3: Disable progress tracking")
print("=" * 60)
profile = analyze(df, show_progress=False)
print("Profile completed (no progress shown)")

print("\n" + "=" * 60)
print("Example 4: Progress with sampling")
print("=" * 60)
# Create a much larger dataset
large_data = [(i, f"Item_{i}", float(i), i % 10) for i in range(100000)]
large_df = spark.createDataFrame(large_data, ["id", "name", "value", "type"])

print("Profiling large dataset with sampling...")
profile = analyze(large_df, target_rows=10000, show_progress=True)

print("\n" + "=" * 60)
print("Example 5: Using environment variable")
print("=" * 60)
# Set environment variable to control progress
os.environ["PYSPARK_ANALYZER_PROGRESS"] = "always"
profile = analyze(df)

# Clean up
os.environ.pop("PYSPARK_ANALYZER_PROGRESS", None)

print("\n" + "=" * 60)
print("Example 6: Progress in non-interactive mode")
print("=" * 60)
# This simulates running in a non-TTY environment (like a cron job)
# Progress will be shown as log messages instead of a progress bar
os.environ["PYSPARK_ANALYZER_PROGRESS"] = "always"
profile = analyze(df, output_format="summary", show_progress=True)
print("\nSummary with progress tracking:")
print(profile[:500] + "...")  # Show first 500 chars

# Clean up
spark.stop()
print("\nAll examples completed!")
