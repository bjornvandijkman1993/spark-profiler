"""
Demonstration of advanced statistics features in pyspark-analyzer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    DateType,
)
from datetime import date, timedelta
import random
import numpy as np

from pyspark_analyzer import DataFrameProfiler


def create_sample_data(spark):
    """Create a sample dataset with various data quality issues."""
    # Generate sample data
    np.random.seed(42)
    random.seed(42)

    data = []
    fruits = ["apple", "banana", "cherry", "date", "elderberry", None, ""]
    emails = [
        "user@example.com",
        "admin@test.org",
        "invalid-email",
        None,
        "test@domain.co.uk",
    ]

    for i in range(1000):
        # ID column
        row_id = i + 1

        # Product name (with some patterns and issues)
        if i % 100 == 0:
            product = None  # Missing values
        elif i % 50 == 0:
            product = " "  # Blank values
        elif i % 30 == 0:
            product = f"PRODUCT_{i}"  # Uppercase pattern
        else:
            product = random.choice(fruits[:-2])  # Exclude None and empty

        # Price (with outliers and special values)
        if i % 200 == 0:
            price = None  # Missing
        elif i % 150 == 0:
            price = float("inf")  # Infinity
        elif i % 100 == 0:
            price = 0.0  # Zero values
        elif i < 10:
            price = 1000.0 + i * 100  # Outliers
        else:
            # Normal distribution with some noise
            price = max(0.1, np.random.normal(50, 15))

        # Email (with patterns)
        if i % 20 == 0:
            email = random.choice(emails)
        else:
            email = f"customer{i}@example.com"

        # Date
        if i % 50 == 0:
            order_date = None
        else:
            order_date = date(2023, 1, 1) + timedelta(days=i % 365)

        # Quantity (for demonstrating skewness)
        if i < 50:
            quantity = random.randint(100, 200)  # High values for skewness
        else:
            quantity = random.randint(1, 10)

        data.append((row_id, product, price, email, order_date, quantity))

    schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("product", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("customer_email", StringType(), True),
            StructField("order_date", DateType(), True),
            StructField("quantity", IntegerType(), False),
        ]
    )

    return spark.createDataFrame(data, schema)


def main():
    # Initialize Spark
    spark = (
        SparkSession.builder.appName("AdvancedStatisticsDemo")
        .master("local[*]")
        .getOrCreate()
    )

    # Create sample data
    print("Creating sample dataset with 1000 rows...")
    df = create_sample_data(spark)

    print(f"\nDataset shape: {df.count()} rows, {len(df.columns)} columns")
    print(f"Columns: {', '.join(df.columns)}")

    # Initialize profiler
    profiler = DataFrameProfiler(df)

    # 1. Full profile with all advanced statistics
    print("\n" + "=" * 60)
    print("1. FULL PROFILE WITH ADVANCED STATISTICS")
    print("=" * 60)

    full_profile = profiler.profile(
        output_format="dict", include_advanced=True, include_quality=True
    )

    # Show advanced numeric statistics for price column
    price_stats = full_profile["columns"]["price"]
    print("\nPrice Column Advanced Statistics:")
    print("  Basic Stats:")
    print(f"    - Mean: ${price_stats['mean']:.2f}")
    print(f"    - Median: ${price_stats['median']:.2f}")
    print(f"    - Std Dev: ${price_stats['std']:.2f}")
    print("  Distribution:")
    print(f"    - Skewness: {price_stats['skewness']:.3f}")
    print(f"    - Kurtosis: {price_stats['kurtosis']:.3f}")
    print("  Range:")
    print(f"    - Min: ${price_stats['min']:.2f}")
    print(f"    - Max: ${price_stats['max']:.2f}")
    print(f"    - Range: ${price_stats['range']:.2f}")
    print(f"    - IQR: ${price_stats['iqr']:.2f}")
    print("  Percentiles:")
    print(f"    - P5: ${price_stats['p5']:.2f}")
    print(f"    - P95: ${price_stats['p95']:.2f}")
    print("  Special Values:")
    print(f"    - Zero Count: {price_stats['zero_count']}")
    print(f"    - Negative Count: {price_stats['negative_count']}")

    # Show outlier detection
    if "outliers" in price_stats:
        outliers = price_stats["outliers"]
        print("  Outliers (IQR method):")
        print(
            f"    - Total Outliers: {outliers['outlier_count']} ({outliers['outlier_percentage']:.1f}%)"
        )
        print(f"    - Lower Bound: ${outliers['lower_bound']:.2f}")
        print(f"    - Upper Bound: ${outliers['upper_bound']:.2f}")

    # Show string column advanced statistics
    email_stats = full_profile["columns"]["customer_email"]
    print("\nCustomer Email Advanced Statistics:")
    print("  Patterns Detected:")
    if "patterns" in email_stats:
        patterns = email_stats["patterns"]
        print(f"    - Valid Emails: {patterns['email_count']}")
        print(f"    - URLs: {patterns['url_count']}")
        print(f"    - Numeric Only: {patterns['numeric_string_count']}")

    print("  Top 5 Values:")
    if "top_values" in email_stats:
        for i, item in enumerate(email_stats["top_values"][:5], 1):
            print(f"    {i}. {item['value']} (count: {item['count']})")

    # 2. Data Quality Report
    print("\n" + "=" * 60)
    print("2. DATA QUALITY REPORT")
    print("=" * 60)

    quality_df = profiler.quality_report()
    print("\nQuality Scores by Column:")
    print(quality_df.to_string(index=False))

    # 3. Quick Profile (performance optimized)
    print("\n" + "=" * 60)
    print("3. QUICK PROFILE (Basic Statistics Only)")
    print("=" * 60)

    import time

    start_time = time.time()
    _ = profiler.quick_profile()
    quick_time = time.time() - start_time

    print(f"\nQuick profile completed in {quick_time:.2f} seconds")
    print("Contains basic statistics without advanced metrics")

    # 4. Demonstrate outlier detection methods
    print("\n" + "=" * 60)
    print("4. OUTLIER DETECTION COMPARISON")
    print("=" * 60)

    stats_computer = profiler.stats_computer

    # IQR method
    iqr_outliers = stats_computer.compute_outlier_stats("quantity", method="iqr")
    print("\nIQR Method Results for 'quantity':")
    print(
        f"  - Outliers: {iqr_outliers['outlier_count']} ({iqr_outliers['outlier_percentage']:.1f}%)"
    )
    print(f"  - Lower outliers: {iqr_outliers['lower_outlier_count']}")
    print(f"  - Upper outliers: {iqr_outliers['upper_outlier_count']}")

    # Z-score method
    zscore_outliers = stats_computer.compute_outlier_stats("quantity", method="zscore")
    print("\nZ-Score Method Results for 'quantity':")
    print(
        f"  - Outliers (|z| > 3): {zscore_outliers['outlier_count']} ({zscore_outliers['outlier_percentage']:.1f}%)"
    )

    # 5. Pattern Analysis
    print("\n" + "=" * 60)
    print("5. PATTERN ANALYSIS FOR PRODUCT COLUMN")
    print("=" * 60)

    product_stats = full_profile["columns"]["product"]
    if "patterns" in product_stats:
        patterns = product_stats["patterns"]
        print("\nCase Patterns:")
        print(f"  - All Uppercase: {patterns['uppercase_count']}")
        print(f"  - All Lowercase: {patterns['lowercase_count']}")
        print(f"  - Has Whitespace Issues: {product_stats['has_whitespace_count']}")

    # Clean up
    spark.stop()

    print("\n" + "=" * 60)
    print("Demo completed! Advanced statistics provide deeper insights into:")
    print("  - Data distribution (skewness, kurtosis)")
    print("  - Outlier detection (IQR and z-score methods)")
    print("  - Pattern recognition (emails, URLs, case patterns)")
    print("  - Data quality metrics (completeness, uniqueness, quality scores)")
    print("  - Top frequent values for categorical analysis")
    print("=" * 60)


if __name__ == "__main__":
    main()
