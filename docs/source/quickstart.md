# Quick Start Guide

This guide will help you get started with pyspark-analyzer in just a few minutes.

## Basic Usage

### 1. Import and Initialize

```python
from pyspark.sql import SparkSession
from pyspark_analyzer import DataFrameProfiler

# Create Spark session
spark = SparkSession.builder \
    .appName("SparkProfilerQuickStart") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
```

### 2. Load Your Data

```python
# From CSV
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# From Parquet
df = spark.read.parquet("data.parquet")

# From JSON
df = spark.read.json("data.json")
```

### 3. Profile Your DataFrame

```python
# Create profiler instance
profiler = DataFrameProfiler(df)

# Generate profile
profile = profiler.profile()

# View results
print(profile)
```

## Output Formats

### Dictionary Format (default)

```python
profile = profiler.profile()
print(profile["overview"])
print(profile["columns"]["age"])
```

### JSON Format

```python
json_profile = profiler.get_profile("json")
print(json_profile)
```

### Summary Report

```python
summary = profiler.get_profile("summary")
print(summary)
```

## Working with Large Datasets

### Automatic Optimization

```python
# Automatically optimizes for datasets > 10M rows
profiler = DataFrameProfiler(df, optimize_for_large_datasets=True)
profile = profiler.profile()
```

### Custom Sampling

```python
from pyspark_analyzer import SamplingConfig

# Configure sampling
config = SamplingConfig(
    target_size=100_000,  # Target 100k rows
    min_fraction=0.01,    # At least 1% of data
    quality_threshold=0.8  # Minimum quality score
)

profiler = DataFrameProfiler(df, sampling_config=config)
profile = profiler.profile()

# Check sampling info
print(profile["sampling"])
```

## Profile Specific Columns

```python
# Profile only specific columns
profile = profiler.profile(columns=["age", "salary", "department"])
```

## Common Use Cases

### Data Quality Assessment

```python
profile = profiler.profile()

# Check for data quality issues
for col_name, col_stats in profile["columns"].items():
    null_ratio = col_stats["null_count"] / col_stats["count"]
    if null_ratio > 0.5:
        print(f"Warning: {col_name} has {null_ratio:.1%} null values")

    if col_stats["distinct_count"] == 1:
        print(f"Warning: {col_name} has only one unique value")
```

### Pre-Processing Analysis

```python
# Identify columns that need cleaning
profile = profiler.profile()

numeric_cols = []
categorical_cols = []

for col_name, col_stats in profile["columns"].items():
    if col_stats["data_type"] in ["integer", "double", "float"]:
        numeric_cols.append(col_name)
    elif col_stats["distinct_count"] < 100:  # Potential categorical
        categorical_cols.append(col_name)

print(f"Numeric columns: {numeric_cols}")
print(f"Categorical candidates: {categorical_cols}")
```

## Next Steps

- Explore the [User Guide](user_guide.md) for advanced features
- Check out [Examples](examples.md) for more use cases
- Read the [API Reference](api_reference.rst) for detailed documentation
