# Pandas DataFrame Output Design for Spark Profiler

## Overview
This document outlines the design for adding pandas DataFrame output support to the spark-profiler library. This feature will enable users to export profiling results as a pandas DataFrame for easier analysis, storage, and tracking of statistics over time.

## Motivation
- **Time-series tracking**: Store profiling results in databases to track data quality metrics over time
- **Comparison analysis**: Easily compare statistics across different datasets or time periods
- **Integration**: Seamless integration with pandas-based data pipelines and visualization tools
- **Flexibility**: Enable custom filtering, sorting, and aggregation of profiling results

## Design Decisions (Without Backward Compatibility Constraints)

### 1. Output Structure
The pandas DataFrame will have the following structure where each row represents a column from the profiled Spark DataFrame:

#### Primary DataFrame Structure
```
Columns:
- column_name (str): Name of the column
- data_type (str): Data type of the column
- total_count (int): Total number of rows
- non_null_count (int): Count of non-null values
- null_count (int): Count of null values
- null_percentage (float): Percentage of null values
- distinct_count (int): Number of distinct values
- distinct_percentage (float): Percentage of distinct values
- min (object): Minimum value (numeric/temporal columns)
- max (object): Maximum value (numeric/temporal columns)
- mean (float): Mean value (numeric columns)
- std (float): Standard deviation (numeric columns)
- median (float): Median value (numeric columns)
- q1 (float): First quartile (numeric columns)
- q3 (float): Third quartile (numeric columns)
- min_length (int): Minimum string length (string columns)
- max_length (int): Maximum string length (string columns)
- avg_length (float): Average string length (string columns)
- empty_count (int): Count of empty strings (string columns)
- min_date (datetime): Earliest date (temporal columns)
- max_date (datetime): Latest date (temporal columns)
- date_range_days (int): Date range in days (temporal columns)
```

### 2. Metadata Handling
Sampling and overview metadata will be accessible through DataFrame attributes:
```python
df.attrs['total_rows'] = overview['total_rows']
df.attrs['total_columns'] = overview['total_columns']
df.attrs['sampling'] = sampling_info  # Full sampling dictionary
df.attrs['profiling_timestamp'] = datetime.now()
```

### 3. Implementation Approach

#### A. Required Dependency
Pandas will be added as a core dependency since we're not concerned with backward compatibility:
```toml
[project.dependencies]
# ... existing dependencies
pandas = ">=1.0.0"

#### B. Direct Import
```python
import pandas as pd
from typing import Union

def format_profile_output(
    profile_data: Dict[str, Any],
    format_type: str = "pandas"
) -> Union[pd.DataFrame, Dict[str, Any], str]:
    if format_type == "pandas":
        return _create_pandas_dataframe(profile_data)
    elif format_type == "dict":
        return profile_data
    elif format_type == "json":
        import json
        return json.dumps(profile_data, indent=2, default=str)
    elif format_type == "summary":
        return _create_summary_report(profile_data)
    else:
        raise ValueError(f"Unsupported format type: {format_type}")
```

#### C. DataFrame Creation Function
```python
def _create_pandas_dataframe(profile_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert profile data to pandas DataFrame.

    Each row represents a column from the profiled Spark DataFrame.
    Metadata is stored in DataFrame.attrs.
    """
    import pandas as pd

    columns_data = profile_data.get("columns", {})

    # Convert nested dict to list of dicts for DataFrame constructor
    rows = []
    for col_name, col_stats in columns_data.items():
        row = {"column_name": col_name}
        row.update(col_stats)
        rows.append(row)

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Add metadata as attributes
    df.attrs['overview'] = profile_data.get("overview", {})
    df.attrs['sampling'] = profile_data.get("sampling", {})
    df.attrs['profiling_timestamp'] = pd.Timestamp.now()

    # Ensure consistent column order
    column_order = [
        'column_name', 'data_type', 'total_count', 'non_null_count',
        'null_count', 'null_percentage', 'distinct_count', 'distinct_percentage',
        'min', 'max', 'mean', 'std', 'median', 'q1', 'q3',
        'min_length', 'max_length', 'avg_length', 'empty_count',
        'min_date', 'max_date', 'date_range_days'
    ]

    # Reorder columns (only include columns that exist)
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]

    return df
```

### 4. Usage Examples

#### Basic Usage (Default to pandas)
```python
from spark_profiler import DataFrameProfiler

# Profile a Spark DataFrame
profiler = DataFrameProfiler(spark_df)
profile_df = profiler.profile()  # Returns pandas DataFrame by default

# Or explicitly request pandas format
profile_df = profiler.profile(output_format="pandas")

# Still support other formats if needed
profile_dict = profiler.profile(output_format="dict")
profile_json = profiler.profile(output_format="json")

# Access column statistics
print(profile_df.head())

# Access metadata
print(f"Total rows profiled: {profile_df.attrs['overview']['total_rows']:,}")
print(f"Sampling used: {profile_df.attrs['sampling']['is_sampled']}")
```

#### Time-Series Tracking
```python
import pandas as pd
from datetime import datetime

# Profile and add timestamp
profile_df = profiler.format_output("pandas")
profile_df['profiling_date'] = datetime.now().date()
profile_df['dataset_name'] = 'production_data'

# Append to existing tracking table
tracking_df = pd.read_csv('profiling_history.csv')
updated_tracking = pd.concat([tracking_df, profile_df], ignore_index=True)
updated_tracking.to_csv('profiling_history.csv', index=False)

# Or write to database
profile_df.to_sql('profiling_metrics', engine, if_exists='append', index=False)
```

#### Comparative Analysis
```python
# Compare two datasets
profile1_df = profiler1.format_output("pandas")
profile2_df = profiler2.format_output("pandas")

# Join on column name
comparison = profile1_df.merge(
    profile2_df,
    on='column_name',
    suffixes=('_dataset1', '_dataset2')
)

# Find columns with significant null percentage changes
null_changes = comparison[
    abs(comparison['null_percentage_dataset1'] - comparison['null_percentage_dataset2']) > 5
]
```

#### Data Quality Monitoring
```python
# Set quality thresholds
quality_issues = profile_df[
    (profile_df['null_percentage'] > 10) |
    (profile_df['distinct_count'] == 1) |
    (profile_df['empty_count'] > profile_df['non_null_count'] * 0.5)
]

if not quality_issues.empty:
    print(f"Found {len(quality_issues)} columns with quality issues:")
    print(quality_issues[['column_name', 'null_percentage', 'distinct_count']])
```

### 5. Testing Strategy

#### Unit Tests
- Test DataFrame creation with various column types
- Test handling of missing statistics (e.g., string stats for numeric columns)
- Test metadata preservation in attrs
- Test import error handling when pandas is not installed

#### Integration Tests
- Test with real Spark DataFrames of different sizes
- Test with sampled vs. full dataset profiling
- Test DataFrame serialization (to_csv, to_parquet, to_sql)

### 6. API Changes

#### DataFrameProfiler.profile() Method
```python
def profile(self, output_format: str = "pandas") -> Union[pd.DataFrame, Dict[str, Any], str]:
    """
    Profile the DataFrame and return results in specified format.

    Args:
        output_format: Output format ("pandas", "dict", "json", "summary")
                      Defaults to "pandas" for easy analysis.

    Returns:
        Profile results in requested format
    """
```

#### Direct DataFrame Methods
Add convenience methods to the profiler:
```python
def to_csv(self, path: str, **kwargs) -> None:
    """Save profile results to CSV."""
    df = self.profile(output_format="pandas")
    df.to_csv(path, **kwargs)

def to_parquet(self, path: str, **kwargs) -> None:
    """Save profile results to Parquet."""
    df = self.profile(output_format="pandas")
    df.to_parquet(path, **kwargs)

def to_sql(self, name: str, con, **kwargs) -> None:
    """Save profile results to SQL database."""
    df = self.profile(output_format="pandas")
    df.to_sql(name, con, **kwargs)
```

#### README Updates
Add section on pandas output:
- Installation with optional dependency
- Basic usage example
- Use case examples (time-series tracking, quality monitoring)

#### CLAUDE.md Updates
Add pandas-related commands and patterns for AI assistance

### 7. Breaking Changes (Acceptable)
- Default output format changes from "dict" to "pandas" DataFrame
- Pandas becomes a required dependency
- Users wanting dict output must explicitly specify `output_format="dict"`
- The `format_output()` method is integrated into `profile()` method

### 8. Future Enhancements
- **Visualization support**: Helper methods to create common plots from the DataFrame
- **Delta tracking**: Built-in methods to compare profiles and highlight changes
- **Export presets**: Pre-configured column selections for common use cases
- **Multi-index support**: Option to use multi-index for hierarchical organization

## Implementation Timeline
1. Add pandas as required dependency
2. Implement _create_pandas_dataframe function
3. Modify profile() method to return DataFrame by default
4. Add convenience methods (to_csv, to_parquet, to_sql)
5. Update all tests to work with DataFrame output
6. Update all examples to showcase DataFrame usage
7. Create example notebook demonstrating time-series tracking

## Conclusion
By making pandas DataFrame the default output format and removing backward compatibility constraints, we create a more intuitive and powerful API. Users can immediately leverage pandas' rich ecosystem for analysis, visualization, and storage without additional conversion steps. This positions spark-profiler as a modern, data-science-friendly tool that seamlessly integrates with existing pandas workflows.
