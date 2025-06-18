#!/usr/bin/env python3
"""
Example demonstrating the simplified analyze() API.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark_analyzer import analyze


def create_sample_data():
    """Create a sample DataFrame for demonstration."""
    spark = (
        SparkSession.builder.appName("SimpleAPIExample")
        .master("local[*]")
        .getOrCreate()
    )

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("department", StringType(), True),
        ]
    )

    data = [
        (1, "John Doe", 28, 65000.0, "Engineering"),
        (2, "Jane Smith", 32, 78000.0, "Marketing"),
        (3, "Bob Johnson", 45, 92000.0, "Sales"),
        (4, "Alice Brown", 26, 58000.0, "HR"),
        (5, "Charlie Wilson", 35, 71000.0, "Engineering"),
        (6, None, 29, 62000.0, "Marketing"),
        (7, "Diana Davis", None, 69000.0, "Sales"),
        (8, "Eve Martinez", 31, None, "HR"),
        (9, "Frank Miller", 38, 81000.0, None),
        (10, "Grace Lee", 27, 64000.0, "Engineering"),
    ]

    return spark.createDataFrame(data, schema)


def main():
    """Main example function demonstrating the simple API."""
    print("🚀 Creating sample DataFrame...")
    df = create_sample_data()

    print("\n" + "=" * 70)
    print("1. BASIC USAGE - Just call analyze()")
    print("=" * 70)

    # Simplest usage - just pass the DataFrame
    profile = analyze(df)
    print(profile)

    print("\n" + "=" * 70)
    print("2. DISABLE SAMPLING")
    print("=" * 70)

    # Disable sampling for small datasets
    profile = analyze(df, sampling=False, output_format="dict")
    print(f"Total rows processed: {profile['overview']['total_rows']}")
    print(f"Sampling applied: {profile['sampling']['is_sampled']}")

    print("\n" + "=" * 70)
    print("3. PROFILE SPECIFIC COLUMNS")
    print("=" * 70)

    # Analyze only specific columns
    profile = analyze(df, columns=["age", "salary"], output_format="pandas")
    print(profile)

    print("\n" + "=" * 70)
    print("4. GET RESULTS AS DICTIONARY")
    print("=" * 70)

    # Get results as a dictionary for programmatic access
    profile = analyze(df, output_format="dict")

    # Access specific statistics
    age_stats = profile["columns"]["age"]
    print("Age statistics:")
    print(f"  Mean: {age_stats['mean']:.1f}")
    print(f"  Min: {age_stats['min']}")
    print(f"  Max: {age_stats['max']}")
    print(f"  Null count: {age_stats['null_count']}")

    print("\n" + "=" * 70)
    print("5. GET HUMAN-READABLE SUMMARY")
    print("=" * 70)

    # Get a text summary
    summary = analyze(df, output_format="summary", include_advanced=False)
    print(summary)

    print("\n" + "=" * 70)
    print("6. SAMPLING EXAMPLES (for larger datasets)")
    print("=" * 70)

    # Create a larger dataset for sampling demo
    large_data = []
    for i in range(10000):
        large_data.append(
            (
                i,
                f"Person {i}",
                25 + (i % 40),
                50000.0 + (i % 50000),
                ["Engineering", "Marketing", "Sales", "HR"][i % 4],
            )
        )

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("department", StringType(), True),
        ]
    )

    large_df = df.sparkSession.createDataFrame(large_data, schema)

    # Sample to 1000 rows
    print("\nSampling to 1000 rows:")
    profile = analyze(large_df, target_rows=1000, output_format="dict")
    print(f"  Original size: {profile['sampling']['original_size']:,} rows")
    print(f"  Sample size: {profile['sampling']['sample_size']:,} rows")

    # Sample 10% of data
    print("\nSampling 10% of data:")
    profile = analyze(large_df, fraction=0.1, output_format="dict")
    print(f"  Sample size: {profile['sampling']['sample_size']:,} rows")
    print(f"  Sampling fraction: {profile['sampling']['sampling_fraction']:.2f}")

    print("\n✅ Simple API demonstration completed!")

    # Clean up
    df.sparkSession.stop()


if __name__ == "__main__":
    main()
