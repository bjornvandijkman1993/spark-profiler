"""
Basic usage example for pyspark-analyzer.
"""

from pyspark.sql import SparkSession

from pyspark_analyzer import analyze

# Create Spark session
spark = SparkSession.builder.appName("BasicUsage").getOrCreate()

# Create a simple DataFrame
data = [
    (1, "Alice", 25, 50000.0, "Engineering"),
    (2, "Bob", 30, 65000.0, "Marketing"),
    (3, None, 28, 58000.0, "Sales"),
    (4, "David", 35, None, "Engineering"),
    (5, "Emma", None, 72000.0, "Marketing"),
]
columns = ["id", "name", "age", "salary", "department"]
df = spark.createDataFrame(data, columns)

print("Sample data:")
df.show()

# Generate profile
print("\nDataFrame Profile:")
profile = analyze(df, sampling=False)  # Disable sampling for small dataset
print(profile)

# Get profile as dictionary for programmatic access
profile_dict = analyze(df, output_format="dict", sampling=False)

# Access specific statistics
print("\nAge column statistics:")
age_stats = profile_dict["columns"]["age"]
print(f"  Mean: {age_stats['mean']:.1f}")
print(f"  Min: {age_stats['min']}")
print(f"  Max: {age_stats['max']}")
print(f"  Nulls: {age_stats['null_count']}")

# Profile specific columns only
print("\nNumeric columns profile:")
numeric_profile = analyze(
    df, columns=["age", "salary"], output_format="pandas", sampling=False
)
print(numeric_profile)

spark.stop()
