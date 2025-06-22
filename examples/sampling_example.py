#!/usr/bin/env python3
"""
Sampling capabilities for efficient profiling of large datasets.
"""

import time

from pyspark.sql import SparkSession

from pyspark_analyzer import analyze

# Create Spark session
spark = SparkSession.builder.appName("SamplingExample").master("local[*]").getOrCreate()

# Create a large dataset (1 million rows)
print("Creating dataset with 1 million rows...")
df = spark.range(0, 1000000).selectExpr(
    "id",
    "id % 100 as category",
    "rand() * 1000 as value",
    "concat('user_', id) as username",
)

# 1. Default behavior - auto-sampling for large datasets
print("\n1. Default behavior (auto-sampling):")
profile = analyze(df, output_format="dict")
print(f"   Original size: {profile['sampling']['original_size']:,} rows")
print(f"   Sample size: {profile['sampling']['sample_size']:,} rows")
print(f"   Speedup: {profile['sampling']['estimated_speedup']:.1f}x")

# 2. Sample to specific number of rows
print("\n2. Sample to 10,000 rows:")
profile = analyze(df, target_rows=10000, output_format="dict")
print(f"   Sample size: {profile['sampling']['sample_size']:,} rows")

# 3. Sample by fraction
print("\n3. Sample 1% of data:")
profile = analyze(df, fraction=0.01, output_format="dict")
print(f"   Sample size: {profile['sampling']['sample_size']:,} rows")

# 4. Disable sampling (process full dataset)
print("\n4. Disable sampling:")
profile = analyze(df, sampling=False, output_format="dict")
print(f"   Rows processed: {profile['sampling']['sample_size']:,} (full dataset)")

# 5. Performance comparison
print("\n5. Performance comparison:")
# Time with sampling
start = time.time()
analyze(df, target_rows=10000)
sampled_time = time.time() - start

# Time without sampling
start = time.time()
analyze(df.limit(10000), sampling=False)  # Use limit to compare same size
full_time = time.time() - start

print(f"   With sampling: {sampled_time:.2f}s")
print(f"   Without sampling: {full_time:.2f}s")
print(f"   Speedup: {full_time/sampled_time:.1f}x")

# 6. Reproducible sampling with seed
print("\n6. Reproducible sampling:")
profile1 = analyze(df, fraction=0.01, seed=42, output_format="dict")
profile2 = analyze(df, fraction=0.01, seed=42, output_format="dict")
print(
    f"   Same seed produces same sample: {profile1['sampling']['sample_size'] == profile2['sampling']['sample_size']}"
)

spark.stop()
