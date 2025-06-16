# Examples

This section provides practical examples of using pyspark-analyzer in various scenarios.

## Basic Examples

### Example 1: Simple CSV Profiling

```python
from pyspark.sql import SparkSession
from pyspark_analyzer import DataFrameProfiler

# Initialize Spark
spark = SparkSession.builder \
    .appName("CSVProfiling") \
    .getOrCreate()

# Load CSV data
df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

# Profile the data
profiler = DataFrameProfiler(df)
profile = profiler.profile()

# Print overview
print(f"Total rows: {profile['overview']['row_count']:,}")
print(f"Total columns: {profile['overview']['column_count']}")

# Examine a specific column
sales_stats = profile['columns']['sales_amount']
print(f"\nSales Amount Statistics:")
print(f"  Mean: ${sales_stats['mean']:,.2f}")
print(f"  Median: ${sales_stats['median']:,.2f}")
print(f"  Std Dev: ${sales_stats['std']:,.2f}")
```

### Example 2: Data Quality Check

```python
def check_data_quality(df, null_threshold=0.05, unique_threshold=0.95):
    """
    Check data quality using profiler results.

    Args:
        df: PySpark DataFrame
        null_threshold: Maximum acceptable null rate
        unique_threshold: Minimum uniqueness for ID columns

    Returns:
        List of quality issues
    """
    profiler = DataFrameProfiler(df)
    profile = profiler.profile()

    issues = []

    for col_name, stats in profile['columns'].items():
        # Check null rate
        null_rate = stats['null_count'] / stats['count']
        if null_rate > null_threshold:
            issues.append({
                'column': col_name,
                'issue': 'high_nulls',
                'rate': null_rate,
                'message': f"{col_name} has {null_rate:.1%} null values"
            })

        # Check uniqueness for potential ID columns
        if 'id' in col_name.lower():
            unique_rate = stats['distinct_count'] / stats['count']
            if unique_rate < unique_threshold:
                issues.append({
                    'column': col_name,
                    'issue': 'low_uniqueness',
                    'rate': unique_rate,
                    'message': f"{col_name} is only {unique_rate:.1%} unique"
                })

        # Check for constant columns
        if stats['distinct_count'] == 1:
            issues.append({
                'column': col_name,
                'issue': 'constant',
                'message': f"{col_name} has only one unique value"
            })

    return issues

# Usage
issues = check_data_quality(df)
for issue in issues:
    print(f"⚠️  {issue['message']}")
```

## Advanced Examples

### Example 3: Comparative Profiling

```python
def compare_datasets(df1, df2, df1_name="Dataset 1", df2_name="Dataset 2"):
    """
    Compare profiles of two datasets.
    """
    profiler1 = DataFrameProfiler(df1)
    profiler2 = DataFrameProfiler(df2)

    profile1 = profiler1.profile()
    profile2 = profiler2.profile()

    print(f"\n📊 Dataset Comparison: {df1_name} vs {df2_name}")
    print("=" * 50)

    # Compare overview
    print("\nOverview:")
    print(f"  Rows: {profile1['overview']['row_count']:,} vs {profile2['overview']['row_count']:,}")
    print(f"  Columns: {profile1['overview']['column_count']} vs {profile2['overview']['column_count']}")

    # Compare common columns
    cols1 = set(profile1['columns'].keys())
    cols2 = set(profile2['columns'].keys())
    common_cols = cols1.intersection(cols2)

    print(f"\nCommon columns: {len(common_cols)}")
    print(f"Unique to {df1_name}: {cols1 - cols2}")
    print(f"Unique to {df2_name}: {cols2 - cols1}")

    # Compare statistics for numeric columns
    print("\nNumeric Column Comparison:")
    for col in common_cols:
        stats1 = profile1['columns'][col]
        stats2 = profile2['columns'][col]

        if stats1['data_type'] in ['integer', 'double']:
            mean_diff = abs(stats1['mean'] - stats2['mean'])
            mean_pct = mean_diff / stats1['mean'] * 100 if stats1['mean'] != 0 else 0

            print(f"\n  {col}:")
            print(f"    Mean: {stats1['mean']:.2f} vs {stats2['mean']:.2f} ({mean_pct:.1f}% diff)")
            print(f"    Std:  {stats1['std']:.2f} vs {stats2['std']:.2f}")

# Usage
train_df = spark.read.parquet("train_data.parquet")
test_df = spark.read.parquet("test_data.parquet")
compare_datasets(train_df, test_df, "Training", "Test")
```

### Example 4: Automated Feature Engineering

```python
from pyspark_analyzer import DataFrameProfiler, SamplingConfig

def identify_feature_types(df, cardinality_threshold=50):
    """
    Automatically identify feature types for ML preprocessing.
    """
    # Use sampling for large datasets
    config = SamplingConfig(target_size=100_000)
    profiler = DataFrameProfiler(df, sampling_config=config)
    profile = profiler.profile()

    feature_types = {
        'numeric': [],
        'categorical': [],
        'high_cardinality': [],
        'datetime': [],
        'text': [],
        'binary': [],
        'to_drop': []
    }

    for col_name, stats in profile['columns'].items():
        # Skip target variable if specified
        if col_name == 'target':
            continue

        data_type = stats['data_type']
        distinct_count = stats['distinct_count']
        null_rate = stats['null_count'] / stats['count']

        # Drop columns with too many nulls
        if null_rate > 0.9:
            feature_types['to_drop'].append(col_name)
            continue

        # Identify feature type
        if data_type in ['integer', 'double', 'float']:
            if distinct_count == 2:
                feature_types['binary'].append(col_name)
            else:
                feature_types['numeric'].append(col_name)

        elif data_type == 'string':
            if distinct_count == 2:
                feature_types['binary'].append(col_name)
            elif distinct_count < cardinality_threshold:
                feature_types['categorical'].append(col_name)
            elif stats.get('avg_length', 0) > 50:
                feature_types['text'].append(col_name)
            else:
                feature_types['high_cardinality'].append(col_name)

        elif data_type in ['timestamp', 'date']:
            feature_types['datetime'].append(col_name)

    return feature_types

# Usage
feature_types = identify_feature_types(df)
print("Feature Types Identified:")
for ftype, columns in feature_types.items():
    if columns:
        print(f"\n{ftype.upper()}: {len(columns)} features")
        print(f"  {', '.join(columns[:5])}" + (" ..." if len(columns) > 5 else ""))
```

### Example 5: Performance Monitoring

```python
import time
from pyspark_analyzer import DataFrameProfiler, SamplingConfig

def profile_with_monitoring(df, name="DataFrame"):
    """
    Profile DataFrame with performance monitoring.
    """
    print(f"\n⏱️  Profiling {name}...")

    # Test different configurations
    configs = [
        ("No sampling", None),
        ("Auto sampling", SamplingConfig()),
        ("Aggressive sampling", SamplingConfig(target_size=10_000)),
    ]

    results = []

    for config_name, config in configs:
        start_time = time.time()

        if config:
            profiler = DataFrameProfiler(df, sampling_config=config)
        else:
            profiler = DataFrameProfiler(df)

        profile = profiler.profile()
        elapsed = time.time() - start_time

        # Get actual sampling info
        sampling_info = profile.get('sampling', {})
        actual_fraction = sampling_info.get('sample_fraction', 1.0)
        quality_score = sampling_info.get('quality_score', 1.0)

        results.append({
            'config': config_name,
            'time': elapsed,
            'sample_fraction': actual_fraction,
            'quality': quality_score
        })

        print(f"\n  {config_name}:")
        print(f"    Time: {elapsed:.2f}s")
        print(f"    Sample: {actual_fraction:.1%}")
        print(f"    Quality: {quality_score:.3f}")

    # Show speedup
    baseline_time = results[0]['time']
    for result in results[1:]:
        speedup = baseline_time / result['time']
        print(f"\n  {result['config']} speedup: {speedup:.1f}x")

    return results

# Usage with large dataset
large_df = spark.range(10_000_000).selectExpr(
    "id",
    "rand() as value1",
    "randn() as value2",
    "cast(rand() * 100 as int) as category"
)

profile_with_monitoring(large_df, "10M row dataset")
```

## Real-World Scenarios

### Example 6: E-commerce Data Profiling

```python
# Profile e-commerce transaction data
def profile_ecommerce_data(transactions_df):
    profiler = DataFrameProfiler(transactions_df, optimize_for_large_datasets=True)
    profile = profiler.profile()

    # Generate business insights
    insights = []

    # Revenue analysis
    revenue_stats = profile['columns'].get('revenue', {})
    if revenue_stats:
        insights.append(f"Average order value: ${revenue_stats['mean']:.2f}")
        insights.append(f"Revenue range: ${revenue_stats['min']:.2f} - ${revenue_stats['max']:.2f}")

    # Customer analysis
    customer_stats = profile['columns'].get('customer_id', {})
    if customer_stats:
        repeat_rate = 1 - (customer_stats['distinct_count'] / customer_stats['count'])
        insights.append(f"Repeat purchase rate: {repeat_rate:.1%}")

    # Product analysis
    product_stats = profile['columns'].get('product_category', {})
    if product_stats:
        insights.append(f"Number of categories: {product_stats['distinct_count']}")

    return insights
```

### Example 7: Time Series Data Profiling

```python
def profile_time_series(df, timestamp_col='timestamp', value_col='value'):
    """
    Specialized profiling for time series data.
    """
    profiler = DataFrameProfiler(df)
    profile = profiler.profile()

    # Get temporal statistics
    ts_stats = profile['columns'][timestamp_col]
    value_stats = profile['columns'][value_col]

    # Calculate additional time series metrics
    from pyspark.sql import functions as F

    # Time range
    time_range = pd.to_datetime(ts_stats['max']) - pd.to_datetime(ts_stats['min'])

    # Sampling frequency
    total_points = profile['overview']['row_count']
    avg_frequency = total_points / time_range.total_seconds()

    # Missing periods (simplified)
    expected_points = time_range.total_seconds() * avg_frequency
    missing_rate = 1 - (total_points / expected_points)

    print(f"Time Series Profile for {value_col}:")
    print(f"  Period: {ts_stats['min']} to {ts_stats['max']}")
    print(f"  Duration: {time_range}")
    print(f"  Data points: {total_points:,}")
    print(f"  Average frequency: {avg_frequency:.2f} points/second")
    print(f"  Missing data rate: {missing_rate:.1%}")
    print(f"  Value range: [{value_stats['min']:.2f}, {value_stats['max']:.2f}]")
    print(f"  Value mean: {value_stats['mean']:.2f} (±{value_stats['std']:.2f})")
```

## Integration Examples

### Example 8: Integration with MLflow

```python
import mlflow

def log_data_profile_to_mlflow(df, dataset_name="training"):
    """
    Log data profile to MLflow for experiment tracking.
    """
    profiler = DataFrameProfiler(df)
    profile = profiler.profile()

    with mlflow.start_run():
        # Log overview metrics
        mlflow.log_metric(f"{dataset_name}_rows", profile['overview']['row_count'])
        mlflow.log_metric(f"{dataset_name}_columns", profile['overview']['column_count'])

        # Log column statistics
        for col_name, stats in profile['columns'].items():
            if stats['data_type'] in ['integer', 'double']:
                mlflow.log_metric(f"{dataset_name}_{col_name}_mean", stats['mean'])
                mlflow.log_metric(f"{dataset_name}_{col_name}_std", stats['std'])
                mlflow.log_metric(f"{dataset_name}_{col_name}_nulls", stats['null_count'])

        # Log profile as artifact
        import json
        with open(f"{dataset_name}_profile.json", "w") as f:
            json.dump(profile, f, indent=2)
        mlflow.log_artifact(f"{dataset_name}_profile.json")
```

### Example 9: Automated Report Generation

```python
def generate_html_report(df, output_file="profile_report.html"):
    """
    Generate an HTML report from profile data.
    """
    profiler = DataFrameProfiler(df)
    profile = profiler.profile()

    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Profile Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #4CAF50; color: white; }
            .overview { background-color: #f0f0f0; padding: 10px; margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <h1>Data Profile Report</h1>
        <div class="overview">
            <h2>Overview</h2>
            <p>Rows: {row_count:,}</p>
            <p>Columns: {column_count}</p>
            <p>Memory Usage: {memory_mb:.2f} MB</p>
        </div>

        <h2>Column Statistics</h2>
        <table>
            <tr>
                <th>Column</th>
                <th>Type</th>
                <th>Non-Null</th>
                <th>Unique</th>
                <th>Mean</th>
                <th>Std</th>
                <th>Min</th>
                <th>Max</th>
            </tr>
            {column_rows}
        </table>
    </body>
    </html>
    """

    # Generate column rows
    column_rows = []
    for col_name, stats in profile['columns'].items():
        non_null_pct = (1 - stats['null_count'] / stats['count']) * 100

        row = f"""
        <tr>
            <td>{col_name}</td>
            <td>{stats['data_type']}</td>
            <td>{non_null_pct:.1f}%</td>
            <td>{stats['distinct_count']:,}</td>
            <td>{stats.get('mean', 'N/A')}</td>
            <td>{stats.get('std', 'N/A')}</td>
            <td>{stats.get('min', 'N/A')}</td>
            <td>{stats.get('max', 'N/A')}</td>
        </tr>
        """
        column_rows.append(row)

    # Fill template
    html_content = html_template.format(
        row_count=profile['overview']['row_count'],
        column_count=profile['overview']['column_count'],
        memory_mb=profile['overview']['memory_usage_bytes'] / 1024 / 1024,
        column_rows=''.join(column_rows)
    )

    with open(output_file, 'w') as f:
        f.write(html_content)

    print(f"Report generated: {output_file}")

# Usage
generate_html_report(df)
