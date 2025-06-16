# Advanced Statistics in spark-profiler

This document describes the advanced statistics features added to spark-profiler.

## Overview

The enhanced statistics module provides deeper insights into your data while maintaining excellent performance through optimized batch aggregations.

## New Features

### 1. Advanced Numeric Statistics

- **Distribution Metrics**: Skewness and kurtosis to understand data shape
- **Robust Statistics**: IQR (Interquartile Range), coefficient of variation
- **Percentiles**: P5 and P95 for better outlier understanding
- **Special Values**: Zero count, negative count, infinity detection
- **Outlier Detection**: Both IQR and z-score methods

### 2. Enhanced String Statistics

- **Pattern Detection**: Automatic detection of emails, URLs, phone numbers
- **Case Analysis**: Uppercase, lowercase, and mixed case patterns
- **Top Frequent Values**: Most common values with counts
- **Whitespace Issues**: Detection of leading/trailing spaces
- **Character Analysis**: Non-ASCII characters, single characters

### 3. Data Quality Metrics

- **Quality Score**: Overall quality rating (0-1) for each column
- **Completeness**: Percentage of non-null values
- **Uniqueness**: Ratio of distinct values
- **Type-Specific Checks**: NaN detection, blank strings, format validation
- **ID Column Detection**: Special handling for identifier columns

### 4. Outlier Detection

Two methods available:
- **IQR Method**: Values outside Q1-1.5*IQR to Q3+1.5*IQR
- **Z-Score Method**: Values with |z-score| > 3

## Usage Examples

### Basic Usage with Advanced Statistics

```python
from spark_profiler import DataFrameProfiler

# Create profiler
profiler = DataFrameProfiler(df)

# Full profile with all advanced features (default)
profile = profiler.profile()

# Get specific advanced statistics
price_stats = profile["columns"]["price"]
print(f"Skewness: {price_stats['skewness']}")
print(f"Outliers: {price_stats['outliers']['outlier_count']}")
print(f"Quality Score: {price_stats['quality']['quality_score']}")
```

### Quick Profile (Performance Optimized)

```python
# Basic statistics only - faster for large datasets
quick_profile = profiler.quick_profile()
```

### Data Quality Report

```python
# Generate a focused quality report
quality_df = profiler.quality_report()
print(quality_df)
```

### Custom Configuration

```python
# Profile with specific options
profile = profiler.profile(
    columns=["price", "email"],  # Specific columns
    include_advanced=True,       # Include advanced stats
    include_quality=True         # Include quality metrics
)
```

## Performance Considerations

All new statistics are computed efficiently:
- **Single Pass**: Most statistics computed in one DataFrame scan
- **Batch Aggregations**: Multiple metrics calculated together
- **Lazy Evaluation**: Only requested statistics are computed
- **Approximate Functions**: Fast approximations for large datasets

## New Methods in StatisticsComputer

```python
# Advanced numeric statistics
stats = computer.compute_numeric_stats(column, advanced=True)

# String statistics with patterns
stats = computer.compute_string_stats(column, top_n=10, pattern_detection=True)

# Outlier detection
outliers = computer.compute_outlier_stats(column, method="iqr")

# Data quality metrics
quality = computer.compute_data_quality_stats(column, column_type="auto")
```

## Example Output

```python
{
    "price": {
        # Basic stats
        "min": 10.0,
        "max": 1000.0,
        "mean": 55.5,
        "median": 50.0,

        # Advanced stats
        "skewness": 2.145,
        "kurtosis": 4.321,
        "cv": 0.453,  # Coefficient of variation
        "p5": 15.0,
        "p95": 150.0,

        # Outliers
        "outliers": {
            "method": "iqr",
            "outlier_count": 5,
            "outlier_percentage": 0.5,
            "lower_bound": -20.0,
            "upper_bound": 120.0
        },

        # Quality
        "quality": {
            "quality_score": 0.95,
            "completeness": 0.98,
            "outlier_percentage": 0.5,
            "nan_count": 0,
            "infinity_count": 2
        }
    }
}
```

## Demo Script

See `examples/advanced_statistics_demo.py` for a complete demonstration of all features.
