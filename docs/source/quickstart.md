# Quick Start Guide

This guide will help you get started with pyspark-analyzer in just a few minutes.

## Basic Usage

### 1. Import and Initialize

```python
from pyspark.sql import SparkSession
from pyspark_analyzer import analyze

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
# Generate profile with the analyze function
profile = analyze(df)

# View results as a pandas DataFrame
print(profile)
```

## Output Formats

### Pandas DataFrame (default)

```python
# Default output is a pandas DataFrame
profile = analyze(df)
print(profile)
```

### Dictionary Format

```python
# Get dictionary output
profile_dict = analyze(df, output_format="dict")
print(profile_dict["overview"])
print(profile_dict["columns"]["age"])
```

### JSON Format

```python
# Get JSON string output
json_profile = analyze(df, output_format="json")
print(json_profile)
```

## Working with Large Datasets

### Automatic Sampling

```python
# Enable automatic sampling for large datasets
profile = analyze(df, sampling=True)

# Specify target number of rows
profile = analyze(df, sampling=True, target_rows=100_000)

# Or specify sampling fraction
profile = analyze(df, sampling=True, fraction=0.1)
```

### Custom Sampling Configuration

```python
from pyspark_analyzer import SamplingConfig

# For advanced control, use SamplingConfig
config = SamplingConfig(
    target_size=100_000,  # Target 100k rows
    min_fraction=0.01,    # At least 1% of data
    quality_threshold=0.8  # Minimum quality score
)

profile_dict = analyze(df, sampling_config=config, output_format="dict")

# Check sampling info
print(profile_dict["sampling"])
```

## Profile Specific Columns

```python
# Profile only specific columns
profile = analyze(df, columns=["age", "salary", "department"])
```

## Common Use Cases

### Data Quality Assessment

```python
# Get profile with quality metrics
profile_dict = analyze(df, include_quality=True, output_format="dict")

# Check for data quality issues
for col_name, col_stats in profile_dict["columns"].items():
    null_ratio = col_stats["null_count"] / col_stats["count"]
    if null_ratio > 0.5:
        print(f"Warning: {col_name} has {null_ratio:.1%} null values")

    if col_stats["distinct_count"] == 1:
        print(f"Warning: {col_name} has only one unique value")
```

### Pre-Processing Analysis

```python
# Identify columns that need cleaning
profile_dict = analyze(df, output_format="dict")

numeric_cols = []
categorical_cols = []

for col_name, col_stats in profile_dict["columns"].items():
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
