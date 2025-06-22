#!/usr/bin/env python3
"""
Simple examples of the analyze() API.
"""

from pyspark.sql import SparkSession

from pyspark_analyzer import analyze

# Create Spark session
spark = SparkSession.builder.appName("SimpleAPI").master("local[*]").getOrCreate()

# Create sample data
data = [(i, f"Product_{i}", 10.0 + i * 0.5, i % 3) for i in range(100)]
df = spark.createDataFrame(data, ["id", "name", "price", "category"])

# 1. Basic usage - just pass the DataFrame
print("1. Basic usage:")
print(analyze(df))

# 2. Get results as dictionary
print("\n2. Access specific statistics:")
profile = analyze(df, output_format="dict")
print(f"Total rows: {profile['overview']['total_rows']}")
print(
    f"Price range: ${profile['columns']['price']['min']:.2f} - ${profile['columns']['price']['max']:.2f}"
)

# 3. Profile specific columns
print("\n3. Profile only numeric columns:")
print(analyze(df, columns=["price", "category"], output_format="pandas"))

# 4. Get human-readable summary
print("\n4. Summary report:")
print(analyze(df, output_format="summary", include_advanced=False))

# 5. Control sampling (useful for large datasets)
print("\n5. With sampling:")
profile = analyze(df, target_rows=50, output_format="dict")
print(f"Sample size: {profile['sampling']['sample_size']} rows")

# 6. With progress tracking
print("\n6. With progress tracking:")
profile = analyze(df, show_progress=True)
print("Analysis completed with progress tracking!")

spark.stop()
