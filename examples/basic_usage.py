"""
Basic usage example for the PySpark DataFrame Profiler.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from datetime import datetime
from spark_profiler import DataFrameProfiler


def create_sample_data():
    """Create a sample DataFrame for demonstration."""
    spark = SparkSession.builder.appName("ProfilerExample").getOrCreate()

    # Define schema
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("department", StringType(), True),
            StructField("hire_date", TimestampType(), True),
        ]
    )

    # Sample data
    data = [
        (1, "John Doe", 30, 75000.0, "Engineering", datetime(2020, 1, 15)),
        (2, "Jane Smith", 25, 65000.0, "Marketing", datetime(2021, 3, 10)),
        (3, "Bob Johnson", 35, 85000.0, "Engineering", datetime(2019, 7, 20)),
        (4, None, 28, 70000.0, "Sales", datetime(2022, 2, 5)),
        (5, "Alice Brown", None, 80000.0, "Engineering", datetime(2020, 11, 30)),
        (6, "Charlie Wilson", 32, None, "Marketing", datetime(2021, 8, 15)),
        (7, "", 29, 72000.0, "Sales", None),
        (8, "Diana Prince", 31, 78000.0, "Engineering", datetime(2020, 5, 12)),
    ]

    return spark.createDataFrame(data, schema)


def main():
    """Main example function."""
    print("Creating sample DataFrame...")
    df = create_sample_data()

    print("Sample data:")
    df.show()

    print("\n" + "=" * 60)
    print("PROFILING ENTIRE DATAFRAME (PANDAS OUTPUT)")
    print("=" * 60)

    # Create profiler and generate profile
    profiler = DataFrameProfiler(df)

    # Default output is now pandas DataFrame
    profile_df = profiler.profile()
    print(f"\nProfile DataFrame shape: {profile_df.shape}")
    print("\nProfile DataFrame preview:")
    print(profile_df[["column_name", "data_type", "null_percentage", "distinct_count"]].to_string())

    # Access metadata from DataFrame attributes
    print(f"\nTotal Rows: {profile_df.attrs['overview']['total_rows']:,}")
    print(f"Total Columns: {profile_df.attrs['overview']['total_columns']}")
    print(f"Profiling Timestamp: {profile_df.attrs['profiling_timestamp']}")

    # Get dictionary format for detailed display
    profile = profiler.profile(output_format="dict")
    overview = profile["overview"]

    # Display column details
    print("\nColumn Statistics:")
    print("-" * 50)

    for col_name, stats in profile["columns"].items():
        print(f"\nColumn: {col_name}")
        print(f"  Data Type: {stats['data_type']}")
        print(f"  Total Count: {stats['total_count']:,}")
        print(f"  Non-null Count: {stats['non_null_count']:,}")
        print(f"  Null Count: {stats['null_count']:,}")
        print(f"  Null Percentage: {stats['null_percentage']:.2f}%")
        print(f"  Distinct Count: {stats['distinct_count']:,}")
        print(f"  Distinct Percentage: {stats['distinct_percentage']:.2f}%")

        # Type-specific statistics
        if "min" in stats:  # Numeric
            print(f"  Min: {stats['min']}")
            print(f"  Max: {stats['max']}")
            print(f"  Mean: {stats['mean']:.2f}" if stats["mean"] else "  Mean: N/A")
            print(f"  Std Dev: {stats['std']:.2f}" if stats["std"] else "  Std Dev: N/A")
            print(f"  Median: {stats['median']}")
            print(f"  Q1: {stats['q1']}")
            print(f"  Q3: {stats['q3']}")
        elif "min_length" in stats:  # String
            print(f"  Min Length: {stats['min_length']}")
            print(f"  Max Length: {stats['max_length']}")
            print(f"  Avg Length: {stats['avg_length']:.2f}")
            print(f"  Empty Count: {stats['empty_count']}")
        elif "min_date" in stats:  # Temporal
            print(f"  Min Date: {stats['min_date']}")
            print(f"  Max Date: {stats['max_date']}")
            if stats["date_range_days"]:
                print(f"  Date Range: {stats['date_range_days']} days")

    print("\n" + "=" * 60)
    print("PROFILING SPECIFIC COLUMNS")
    print("=" * 60)

    # Profile only specific columns (returns pandas by default)
    numeric_profile_df = profiler.profile(columns=["age", "salary"])
    print("\nNumeric columns only (pandas DataFrame):")
    print(numeric_profile_df[["column_name", "min", "max", "mean"]].to_string())

    print("\n" + "=" * 60)
    print("FORMATTED OUTPUT EXAMPLES")
    print("=" * 60)

    # Example of formatted output

    # Summary format
    print("\nSummary Report:")
    summary = profiler.format_output("summary")
    print(summary)

    # Save to CSV
    print("\nSaving profile to CSV...")
    profiler.to_csv("employee_profile.csv", index=False)
    print("✓ Saved to employee_profile.csv")

    # Example of data quality checks with pandas
    print("\n" + "=" * 60)
    print("DATA QUALITY CHECKS")
    print("=" * 60)

    # Find columns with issues
    issues = profile_df[profile_df["null_percentage"] > 10]
    if not issues.empty:
        print("\nColumns with >10% null values:")
        print(issues[["column_name", "null_percentage"]].to_string())


if __name__ == "__main__":
    main()
