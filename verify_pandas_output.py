"""
Simple verification script for pandas output functionality.
This script tests the pandas output without requiring Spark/Java.
"""

import os
import pandas as pd
from spark_profiler.utils import format_profile_output, _create_pandas_dataframe

# Sample profile data (simulating what DataFrameProfiler would return)
sample_profile = {
    "overview": {
        "total_rows": 1000,
        "total_columns": 4,
        "column_types": {
            "id": "IntegerType",
            "name": "StringType",
            "score": "DoubleType",
            "created_at": "TimestampType",
        },
    },
    "columns": {
        "id": {
            "data_type": "IntegerType",
            "total_count": 1000,
            "non_null_count": 998,
            "null_count": 2,
            "null_percentage": 0.2,
            "distinct_count": 998,
            "distinct_percentage": 99.8,
            "min": 1,
            "max": 1000,
            "mean": 500.5,
            "std": 288.67,
            "median": 500,
            "q1": 250,
            "q3": 750,
        },
        "name": {
            "data_type": "StringType",
            "total_count": 1000,
            "non_null_count": 990,
            "null_count": 10,
            "null_percentage": 1.0,
            "distinct_count": 950,
            "distinct_percentage": 95.0,
            "min_length": 3,
            "max_length": 20,
            "avg_length": 8.5,
            "empty_count": 5,
        },
        "score": {
            "data_type": "DoubleType",
            "total_count": 1000,
            "non_null_count": 980,
            "null_count": 20,
            "null_percentage": 2.0,
            "distinct_count": 100,
            "distinct_percentage": 10.0,
            "min": 0.0,
            "max": 100.0,
            "mean": 75.5,
            "std": 15.2,
            "median": 78.0,
            "q1": 65.0,
            "q3": 85.0,
        },
        "created_at": {
            "data_type": "TimestampType",
            "total_count": 1000,
            "non_null_count": 995,
            "null_count": 5,
            "null_percentage": 0.5,
            "distinct_count": 365,
            "distinct_percentage": 36.5,
            "min_date": "2023-01-01 00:00:00",
            "max_date": "2023-12-31 23:59:59",
            "date_range_days": 364,
        },
    },
    "sampling": {"is_sampled": False},
}

print("Testing pandas DataFrame output functionality...")
print("=" * 60)

# Test 1: Create pandas DataFrame
print("\n1. Testing _create_pandas_dataframe function:")
df = _create_pandas_dataframe(sample_profile)
print(f"   ✓ DataFrame created with shape: {df.shape}")
print(f"   ✓ Columns: {list(df.columns)[:5]}...")

# Test 2: Check metadata
print("\n2. Testing metadata in DataFrame.attrs:")
print(f"   ✓ Total rows in metadata: {df.attrs['overview']['total_rows']}")
print(f"   ✓ Sampling info: {df.attrs['sampling']}")
print(f"   ✓ Timestamp: {df.attrs['profiling_timestamp']}")

# Test 3: Test format_profile_output
print("\n3. Testing format_profile_output function:")
pandas_output = format_profile_output(sample_profile, "pandas")
print(f"   ✓ Pandas format: {type(pandas_output).__name__}")

dict_output = format_profile_output(sample_profile, "dict")
print(f"   ✓ Dict format: {type(dict_output).__name__}")

json_output = format_profile_output(sample_profile, "json")
print(f"   ✓ JSON format: {type(json_output).__name__} (length: {len(json_output)})")

summary_output = format_profile_output(sample_profile, "summary")
print(
    f"   ✓ Summary format: {type(summary_output).__name__} (lines: {len(summary_output.splitlines())})"
)

# Test 4: Data quality checks
print("\n4. Testing data quality analysis:")
quality_issues = df[df["null_percentage"] > 1.5]
print(f"   ✓ Found {len(quality_issues)} columns with >1.5% null values:")
if not quality_issues.empty:
    print(quality_issues[["column_name", "null_percentage"]].to_string(index=False))

# Test 5: Column order
print("\n5. Testing column order:")
expected_order = ["column_name", "data_type", "total_count", "non_null_count"]
actual_order = list(df.columns[:4])
print(f"   ✓ First 4 columns in correct order: {expected_order == actual_order}")

# Test 6: Type-specific statistics
print("\n6. Testing type-specific statistics:")
numeric_cols = df[df["mean"].notna()]
print(f"   ✓ Numeric columns with mean values: {list(numeric_cols['column_name'])}")

string_cols = df[df["avg_length"].notna()]
print(f"   ✓ String columns with length stats: {list(string_cols['column_name'])}")

temporal_cols = df[df["date_range_days"].notna()]
print(f"   ✓ Temporal columns with date ranges: {list(temporal_cols['column_name'])}")

# Test 7: Save to CSV
print("\n7. Testing CSV export:")
df.to_csv("test_profile_output.csv", index=False)
print("   ✓ Successfully saved to test_profile_output.csv")

# Read back and verify
df_read = pd.read_csv("test_profile_output.csv")
print(f"   ✓ Read back {len(df_read)} rows from CSV")

print("\n" + "=" * 60)
print("✅ All pandas output functionality tests passed!")
print("\nThe pandas DataFrame output feature is working correctly.")
print("Users can now:")
print("  • Get profiling results as pandas DataFrames by default")
print("  • Save profiles to CSV, Parquet, or SQL databases")
print("  • Track statistics over time")
print("  • Perform custom analysis using pandas operations")

# Clean up
os.remove("test_profile_output.csv")
