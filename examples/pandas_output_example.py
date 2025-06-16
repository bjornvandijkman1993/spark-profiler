"""
Example demonstrating pandas DataFrame output from pyspark-analyzer.

This example shows how to:
1. Generate profiles as pandas DataFrames
2. Save profiles to various formats (CSV, Parquet, SQL)
3. Track statistics over time
4. Compare profiles between datasets
"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)

from pyspark_analyzer import DataFrameProfiler


def create_sample_data(spark, num_rows=10000):
    """Create a sample e-commerce dataset."""
    import random
    from datetime import datetime, timedelta

    # Generate sample data
    data = []
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]

    for i in range(num_rows):
        # Some nulls to make it realistic
        customer_id = i + 1 if random.random() > 0.02 else None
        product_name = (
            f"Product_{random.randint(1, 100)}" if random.random() > 0.01 else None
        )
        category = random.choice(categories) if random.random() > 0.05 else None
        price = round(random.uniform(10, 500), 2) if random.random() > 0.03 else None
        quantity = random.randint(1, 10) if random.random() > 0.02 else None
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))

        data.append((customer_id, product_name, category, price, quantity, order_date))

    schema = StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("order_date", TimestampType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def main():
    # Initialize Spark
    spark = (
        SparkSession.builder.appName("PandasOutputExample")
        .master("local[*]")
        .getOrCreate()
    )

    print("Creating sample e-commerce data...")
    df = create_sample_data(spark)

    # Example 1: Basic pandas output
    print("\n=== Example 1: Basic Pandas Output ===")
    profiler = DataFrameProfiler(df)
    profile_df = profiler.profile()  # Returns pandas DataFrame by default

    print(f"Profile shape: {profile_df.shape}")
    print("\nFirst few rows:")
    print(profile_df.head())

    # Access metadata
    print(f"\nTotal rows in dataset: {profile_df.attrs['overview']['total_rows']:,}")
    print(f"Profiling timestamp: {profile_df.attrs['profiling_timestamp']}")

    # Example 2: Save to different formats
    print("\n=== Example 2: Saving to Different Formats ===")

    # Save to CSV
    profiler.to_csv("profile_results.csv", index=False)
    print("✓ Saved to profile_results.csv")

    # Save to Parquet (more efficient for larger profiles)
    profiler.to_parquet("profile_results.parquet")
    print("✓ Saved to profile_results.parquet")

    # Example 3: Data quality monitoring
    print("\n=== Example 3: Data Quality Monitoring ===")

    # Find columns with high null percentages
    quality_issues = profile_df[profile_df["null_percentage"] > 5]
    if not quality_issues.empty:
        print(f"Found {len(quality_issues)} columns with >5% null values:")
        print(quality_issues[["column_name", "null_percentage"]])

    # Find columns with low cardinality
    low_cardinality = profile_df[
        (profile_df["distinct_count"] < 10) & (profile_df["column_name"] != "quantity")
    ]
    if not low_cardinality.empty:
        print("\nColumns with low cardinality (<10 distinct values):")
        print(low_cardinality[["column_name", "distinct_count"]])

    # Example 4: Time-series tracking
    print("\n=== Example 4: Time-Series Tracking ===")

    # Add timestamp and dataset identifier
    profile_df["profiling_date"] = datetime.now().date()
    profile_df["dataset_name"] = "ecommerce_orders"
    profile_df["dataset_size"] = profile_df.attrs["overview"]["total_rows"]

    # Save for historical tracking
    tracking_df = profile_df[
        [
            "profiling_date",
            "dataset_name",
            "dataset_size",
            "column_name",
            "null_percentage",
            "distinct_count",
        ]
    ]
    tracking_df.to_csv("profiling_history.csv", mode="a", header=False, index=False)
    print("✓ Appended to profiling_history.csv for time-series tracking")

    # Example 5: Comparative analysis
    print("\n=== Example 5: Comparative Analysis ===")

    # Create a filtered dataset (simulate different time period)
    df_filtered = df.filter(df.price > 100)
    profiler_filtered = DataFrameProfiler(df_filtered)
    profile_filtered_df = profiler_filtered.profile()

    # Compare the two profiles
    comparison = profile_df.merge(
        profile_filtered_df, on="column_name", suffixes=("_full", "_filtered")
    )

    # Show differences in null percentages
    comparison["null_pct_diff"] = (
        comparison["null_percentage_filtered"] - comparison["null_percentage_full"]
    )

    print("Null percentage differences (filtered - full):")
    print(comparison[["column_name", "null_pct_diff"]].sort_values("null_pct_diff"))

    # Example 6: Custom analysis with pandas
    print("\n=== Example 6: Custom Analysis ===")

    # Find numeric columns with wide ranges
    numeric_cols = profile_df[profile_df["mean"].notna()]
    if not numeric_cols.empty:
        numeric_cols["range"] = numeric_cols["max"] - numeric_cols["min"]
        numeric_cols["cv"] = (
            numeric_cols["std"] / numeric_cols["mean"]
        )  # Coefficient of variation

        print("Numeric columns analysis:")
        print(numeric_cols[["column_name", "mean", "std", "cv", "range"]])

    # Example 7: Export subset for reporting
    print("\n=== Example 7: Export for Reporting ===")

    # Create a summary report DataFrame
    report_df = profile_df[
        ["column_name", "data_type", "null_percentage", "distinct_count"]
    ].copy()
    report_df["data_quality"] = report_df.apply(
        lambda row: "Good" if row["null_percentage"] < 5 else "Needs Review", axis=1
    )

    # Style the DataFrame for better visualization (when displayed in Jupyter)
    styled = report_df.style.background_gradient(
        subset=["null_percentage"], cmap="RdYlGn_r"
    )

    # Save styled report as HTML
    with open("data_quality_report.html", "w") as f:
        f.write(styled.to_html())
    print("✓ Saved styled report to data_quality_report.html")

    # Clean up
    spark.stop()
    print("\n✅ Examples completed successfully!")


if __name__ == "__main__":
    main()
