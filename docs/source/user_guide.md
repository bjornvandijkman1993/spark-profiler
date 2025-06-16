# User Guide

## Overview

pyspark-analyzer is designed to provide comprehensive statistical analysis of PySpark DataFrames with a focus on performance and scalability. This guide covers advanced usage patterns and best practices.

## Understanding Profile Output

### Profile Structure

A complete profile contains three main sections:

```python
{
    "overview": {
        "row_count": 1000000,
        "column_count": 15,
        "memory_usage_bytes": 125000000,
        "partitions": 8
    },
    "columns": {
        "column_name": {
            "data_type": "integer",
            "count": 1000000,
            "null_count": 5000,
            "distinct_count": 850,
            # Type-specific statistics...
        }
    },
    "sampling": {
        "method": "random",
        "sample_fraction": 0.1,
        "sample_size": 100000,
        "quality_score": 0.95
    }
}
```

### Column Statistics by Type

#### Numeric Columns
- `min`, `max`: Range of values
- `mean`: Average value
- `std`: Standard deviation
- `median`: 50th percentile
- `q1`, `q3`: 25th and 75th percentiles

#### String Columns
- `min_length`, `max_length`: Length range
- `avg_length`: Average string length
- `empty_count`: Number of empty strings

#### Temporal Columns
- `min_date`, `max_date`: Date range
- Additional timestamp-specific metrics

## Performance Optimization

### Automatic Optimization

The library automatically applies optimizations for large datasets:

```python
# Triggers optimizations for DataFrames > 10M rows
profiler = DataFrameProfiler(df, optimize_for_large_datasets=True)
```

Optimizations include:
- Intelligent sampling
- Batch aggregations
- Approximate algorithms
- Smart caching

### Manual Performance Tuning

#### 1. Sampling Configuration

```python
from pyspark_analyzer import SamplingConfig

# Aggressive sampling for very large datasets
config = SamplingConfig(
    target_size=50_000,      # Smaller sample
    min_fraction=0.001,      # 0.1% minimum
    max_fraction=0.1,        # 10% maximum
    seed=42                  # Reproducible results
)

profiler = DataFrameProfiler(df, sampling_config=config)
```

#### 2. Column Selection

```python
# Profile only essential columns
essential_cols = ["user_id", "revenue", "timestamp"]
profile = profiler.profile(columns=essential_cols)
```

#### 3. Partition Optimization

```python
# Optimize partitions before profiling
df = df.repartition(200)  # Adjust based on cluster size
profiler = DataFrameProfiler(df)
```

## Advanced Sampling

### Quality-Based Sampling

The library uses statistical methods to ensure sample quality:

```python
config = SamplingConfig(
    quality_threshold=0.9,  # Require 90% quality score
    confidence_level=0.95   # 95% confidence interval
)

profiler = DataFrameProfiler(df, sampling_config=config)
profile = profiler.profile()

# Check actual quality achieved
sampling_info = profile["sampling"]
print(f"Quality score: {sampling_info['quality_score']:.2f}")
print(f"Confidence: {sampling_info['confidence_interval']}")
```

### Stratified Sampling (Future Feature)

```python
# Coming soon: Stratified sampling by column
config = SamplingConfig(
    stratify_by="category",
    target_size=100_000
)
```

## Integration Patterns

### With MLlib

```python
from pyspark.ml.feature import StandardScaler, VectorAssembler

# Use profile to identify numeric columns
profile = profiler.profile()
numeric_cols = [
    col for col, stats in profile["columns"].items()
    if stats["data_type"] in ["integer", "double"]
]

# Prepare features for ML
assembler = VectorAssembler(
    inputCols=numeric_cols,
    outputCol="features"
)
```

### With Data Quality Frameworks

```python
def generate_quality_report(df):
    profiler = DataFrameProfiler(df)
    profile = profiler.profile()

    issues = []
    for col, stats in profile["columns"].items():
        # Check for high null rates
        null_rate = stats["null_count"] / stats["count"]
        if null_rate > 0.1:
            issues.append(f"{col}: {null_rate:.1%} nulls")

        # Check for low cardinality
        if stats["distinct_count"] < 2:
            issues.append(f"{col}: Low cardinality")

    return issues
```

### With Reporting Tools

```python
import json

# Export for visualization tools
profile = profiler.profile()
with open("profile_report.json", "w") as f:
    json.dump(profile, f, indent=2)

# Generate markdown report
summary = profiler.get_profile("summary")
with open("profile_summary.md", "w") as f:
    f.write(summary)
```

## Best Practices

### 1. Cache Management

```python
# Reuse profiler instance for multiple operations
profiler = DataFrameProfiler(df)

# First profile - computes and caches
full_profile = profiler.profile()

# Subsequent calls use cache
subset_profile = profiler.profile(columns=["age", "salary"])
```

### 2. Memory Management

```python
# For very large datasets, process in chunks
columns = df.columns
chunk_size = 10

for i in range(0, len(columns), chunk_size):
    chunk_cols = columns[i:i + chunk_size]
    profile = profiler.profile(columns=chunk_cols)
    # Process chunk results...
```

### 3. Error Handling

```python
from pyspark.sql import AnalysisException

try:
    profile = profiler.profile()
except AnalysisException as e:
    print(f"Schema error: {e}")
except Exception as e:
    print(f"Profiling failed: {e}")
```

## Customization

### Custom Statistics (Future Feature)

```python
# Coming soon: Register custom statistics
@profiler.register_statistic("custom_metric")
def compute_custom_metric(df, column):
    return df.agg(...)
```

### Output Formatters (Future Feature)

```python
# Coming soon: Custom output formats
@profiler.register_formatter("html")
def html_formatter(profile):
    return generate_html_report(profile)
```

## Troubleshooting

### Common Issues

1. **OutOfMemoryError**: Reduce sample size or enable more aggressive sampling
2. **Slow Performance**: Check partition count and distribution
3. **Incorrect Statistics**: Verify data types and null handling

### Debug Mode

```python
import logging
logging.getLogger("pyspark_analyzer").setLevel(logging.DEBUG)

profiler = DataFrameProfiler(df)
profile = profiler.profile()  # Will show debug information
```
