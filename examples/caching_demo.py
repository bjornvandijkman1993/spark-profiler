"""
Demonstration of the automatic caching in pyspark-analyzer.

This example shows how caching automatically improves performance
when profiling large DataFrames.
"""

import time

from pyspark.sql import SparkSession

from pyspark_analyzer import analyze


def main():
    """Demonstrate automatic caching functionality."""
    # Create Spark session
    spark = (
        SparkSession.builder.appName("PySpark Analyzer Caching Demo")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    # Create a large sample DataFrame
    print("Creating sample DataFrame with 2 million rows...")
    df = spark.range(0, 2_000_000).selectExpr(
        "id",
        "id * 2 as double_id",
        "cast(id % 100 as double) as mod_100",
        "cast(rand() * 100 as int) as random_int",
        "concat('user_', cast(id % 1000 as string)) as user_id",
        "case when rand() > 0.5 then 'A' else 'B' end as category",
    )

    print("\n" + "=" * 60)
    print("EXAMPLE 1: Profile with automatic caching")
    print("=" * 60)

    start_time = time.time()
    result1 = analyze(
        df, target_rows=500_000, output_format="dict"  # Sample to 500k rows
    )
    end_time = time.time()

    print(f"\nProfiling completed in {end_time - start_time:.2f} seconds")
    print(f"Analyzed {result1['overview']['total_columns']} columns")
    print(f"Sample size: {result1['sampling']['sample_size']:,} rows")

    if "caching" in result1:
        print("\nCaching statistics:")
        print(f"  - Cache enabled: {result1['caching']['enabled']}")
        print(f"  - Cache key: {result1['caching']['cache_key']}")
        print(f"  - Hit rate: {result1['caching']['hit_rate']:.2%}")

    print("\n" + "=" * 60)
    print("EXAMPLE 2: Multiple analyses with same DataFrame")
    print("=" * 60)
    print("\nThis demonstrates cache reuse when analyzing different column subsets")
    print("Note: Caching is automatic for DataFrames with >1M rows")

    # First analysis - all columns
    start_time = time.time()
    analyze(df, output_format="dict")
    time1 = time.time() - start_time
    print(f"\nFirst analysis (all columns): {time1:.2f} seconds")

    # Second analysis - subset of columns (may benefit from cache)
    start_time = time.time()
    analyze(df, columns=["id", "random_int"], output_format="dict")
    time2 = time.time() - start_time
    print(f"Second analysis (subset): {time2:.2f} seconds")

    if time2 < time1:
        print(
            f"Second analysis was {(time1/time2 - 1)*100:.0f}% faster (cache benefit)"
        )

    print("\n" + "=" * 60)
    print("EXAMPLE 3: Small DataFrame (no caching)")
    print("=" * 60)
    print("\nDataFrames with <1M rows are not cached by default")

    small_df = spark.range(0, 100_000).selectExpr("id", "id * 2 as double_id")

    start_time = time.time()
    result3 = analyze(small_df, output_format="dict")
    end_time = time.time()

    print(f"\nProfiling completed in {end_time - start_time:.2f} seconds")
    print(f"Analyzed {result3['overview']['total_rows']:,} rows (no caching applied)")

    # Clean up
    spark.stop()
    print("\n✓ Demo completed successfully!")


if __name__ == "__main__":
    main()
