"""
Advanced statistics features in pyspark-analyzer.
"""

from pyspark.sql import SparkSession
import numpy as np
from pyspark_analyzer import analyze

# Create Spark session
spark = SparkSession.builder.appName("AdvancedStats").master("local[*]").getOrCreate()

# Create data with outliers and patterns
np.random.seed(42)
data = []
for i in range(1000):
    # Create data with outliers
    if i < 10:
        price = 1000.0 + i * 100  # Outliers
    else:
        price = max(0.1, np.random.normal(50, 15))  # Normal distribution

    # Add email patterns
    email = f"user{i}@example.com" if i % 10 != 0 else "invalid-email"

    # Add quantity with skew
    quantity = np.random.randint(100, 200) if i < 50 else np.random.randint(1, 10)

    data.append((i, f"Product_{i}", price, email, quantity))

df = spark.createDataFrame(data, ["id", "product", "price", "email", "quantity"])

# Full profile with advanced statistics
print("Advanced Statistics Profile:")
profile = analyze(
    df,
    output_format="dict",
    include_advanced=True,
    include_quality=True,
    sampling=False,
)

# Show distribution metrics
price_stats = profile["columns"]["price"]
print("\nPrice Column Distribution:")
print(f"  Mean: ${price_stats['mean']:.2f}")
print(f"  Median: ${price_stats['median']:.2f}")
print(f"  Skewness: {price_stats['skewness']:.3f}")
print(f"  Kurtosis: {price_stats['kurtosis']:.3f}")

# Show outlier detection
if "outliers" in price_stats:
    outliers = price_stats["outliers"]
    print(
        f"\n  Outliers: {outliers['outlier_count']} ({outliers['outlier_percentage']:.1f}%)"
    )
    print(
        f"  IQR bounds: [{outliers['lower_bound']:.2f}, {outliers['upper_bound']:.2f}]"
    )

# Show pattern detection
email_stats = profile["columns"]["email"]
if "patterns" in email_stats:
    patterns = email_stats["patterns"]
    print("\nEmail Patterns:")
    print(f"  Valid emails: {patterns['email_count']}")
    print(f"  Invalid: {email_stats['total_count'] - patterns['email_count']}")

# Show top values
if "top_values" in email_stats:
    print("\n  Top values:")
    for item in email_stats["top_values"][:3]:
        print(f"    - {item['value']}: {item['count']} occurrences")

# Data quality report
print("\nData Quality Scores:")
for col_name, col_stats in profile["columns"].items():
    if "quality" in col_stats:
        q = col_stats["quality"]
        print(
            f"  {col_name}: {q['quality_score']:.1f}% (completeness: {q['completeness']:.1f}%)"
        )

spark.stop()
