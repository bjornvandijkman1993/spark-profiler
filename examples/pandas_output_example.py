"""
Working with pandas DataFrame output from pyspark-analyzer.
"""

from pyspark.sql import SparkSession
from pyspark_analyzer import analyze

# Create Spark session
spark = SparkSession.builder.appName("PandasOutput").master("local[*]").getOrCreate()

# Create sample e-commerce data
data = [
    (1, "Laptop", "Electronics", 999.99, 2),
    (2, "T-Shirt", "Clothing", 19.99, 5),
    (3, "Book", "Books", 14.99, 1),
    (4, None, "Electronics", 299.99, 1),
    (5, "Shoes", "Clothing", None, 2),
    (6, "Phone", "Electronics", 799.99, 1),
    (7, "Jeans", None, 49.99, 3),
]
df = spark.createDataFrame(
    data, ["order_id", "product", "category", "price", "quantity"]
)

# 1. Get profile as pandas DataFrame (default)
print("1. Basic pandas output:")
profile_df = analyze(df, sampling=False)
print(profile_df.head())
print(f"\nShape: {profile_df.shape}")
print(f"Columns: {list(profile_df.columns)[:5]}...")  # Show first 5 columns

# 2. Access metadata
print("\n2. Access metadata:")
print(f"Total rows analyzed: {profile_df.attrs['overview']['total_rows']}")
print(f"Profiling timestamp: {profile_df.attrs['profiling_timestamp']}")

# 3. Data quality analysis
print("\n3. Data quality analysis:")
# Find columns with nulls
null_cols = profile_df[profile_df["null_count"] > 0][
    ["column_name", "null_count", "null_percentage"]
]
print("Columns with null values:")
print(null_cols)

# 4. Numeric column statistics
print("\n4. Numeric statistics:")
numeric_stats = profile_df[profile_df["mean"].notna()][
    ["column_name", "mean", "std", "min", "max"]
]
print(numeric_stats)

# 5. Save to different formats
print("\n5. Save to formats:")
profile_df.to_csv("profile.csv", index=False)
print("✓ Saved to profile.csv")

profile_df.to_parquet("profile.parquet")
print("✓ Saved to profile.parquet")

# 6. Compare two datasets
print("\n6. Compare datasets:")
# Filter to create a second dataset
expensive_df = df.filter(df.price > 50)
expensive_profile = analyze(expensive_df, sampling=False)

# Compare null percentages
comparison = profile_df.merge(
    expensive_profile, on="column_name", suffixes=("_all", "_expensive")
)
print("Null percentage comparison:")
print(comparison[["column_name", "null_percentage_all", "null_percentage_expensive"]])

spark.stop()
