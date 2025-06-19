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
from pyspark_analyzer import analyze

# Enable sampling for large datasets
profile = analyze(df, sampling=True)
```

Optimizations include:
- Intelligent sampling
- Batch aggregations
- Approximate algorithms
- Smart caching

### Manual Performance Tuning

#### 1. Sampling Configuration

```python
from pyspark_analyzer import analyze, SamplingConfig

# Simple sampling with target rows
profile = analyze(df, sampling=True, target_rows=50_000)

# Or use fraction-based sampling
profile = analyze(df, sampling=True, fraction=0.01)

# For advanced control, use SamplingConfig
config = SamplingConfig(
    target_size=50_000,      # Smaller sample
    min_fraction=0.001,      # 0.1% minimum
    max_fraction=0.1,        # 10% maximum
    seed=42                  # Reproducible results
)

profile = analyze(df, sampling_config=config)
```

#### 2. Column Selection

```python
# Profile only essential columns
essential_cols = ["user_id", "revenue", "timestamp"]
profile = analyze(df, columns=essential_cols)
```

#### 3. Partition Optimization

```python
# Optimize partitions before profiling
df = df.repartition(200)  # Adjust based on cluster size
profile = analyze(df)
```

## Advanced Sampling

### Quality-Based Sampling

The library uses statistical methods to ensure sample quality:

```python
config = SamplingConfig(
    quality_threshold=0.9,  # Require 90% quality score
    confidence_level=0.95   # 95% confidence interval
)

profile_dict = analyze(df, sampling_config=config, output_format="dict")

# Check actual quality achieved
sampling_info = profile_dict["sampling"]
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
from pyspark_analyzer import analyze

# Use profile to identify numeric columns
profile_dict = analyze(df, output_format="dict")
numeric_cols = [
    col for col, stats in profile_dict["columns"].items()
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
from pyspark_analyzer import analyze

def generate_quality_report(df):
    profile_dict = analyze(df, include_quality=True, output_format="dict")

    issues = []
    for col, stats in profile_dict["columns"].items():
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
from pyspark_analyzer import analyze

# Get JSON output directly
json_profile = analyze(df, output_format="json")
with open("profile_report.json", "w") as f:
    f.write(json_profile)

# Or get dictionary and convert
profile_dict = analyze(df, output_format="dict")
with open("profile_report.json", "w") as f:
    json.dump(profile_dict, f, indent=2)
```

## Best Practices

### 1. Cache Management

```python
# Cache DataFrame before profiling for multiple operations
df.cache()

# First profile
full_profile = analyze(df)

# Subsequent calls benefit from cached DataFrame
subset_profile = analyze(df, columns=["age", "salary"])

# Don't forget to unpersist when done
df.unpersist()
```

### 2. Memory Management

```python
# For very large datasets, process in chunks
columns = df.columns
chunk_size = 10

for i in range(0, len(columns), chunk_size):
    chunk_cols = columns[i:i + chunk_size]
    profile = analyze(df, columns=chunk_cols)
    # Process chunk results...
```

### 3. Error Handling

```python
from pyspark.sql import AnalysisException
from pyspark_analyzer import analyze

try:
    profile = analyze(df)
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
from pyspark_analyzer import analyze

logging.getLogger("pyspark_analyzer").setLevel(logging.DEBUG)

profile = analyze(df)  # Will show debug information
```
