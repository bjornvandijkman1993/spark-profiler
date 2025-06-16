#!/usr/bin/env python3
"""
Installation verification example for the PySpark DataFrame Profiler.

This script verifies that the pyspark-analyzer package is correctly installed
and demonstrates basic functionality with a sample dataset.
"""

import sys
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
from pyspark_analyzer import DataFrameProfiler


def verify_installation():
    """Verify the profiler installation and basic functionality."""
    print("🚀 Verifying PySpark DataFrame Profiler Installation...")

    # Create Spark session
    print("📊 Creating Spark session...")
    spark = (
        SparkSession.builder.appName("ProfilerTest").master("local[*]").getOrCreate()
    )
    spark.conf.set("spark.sql.adaptive.enabled", "false")  # Simplify for testing

    try:
        # Create test data
        print("📋 Creating test DataFrame...")
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

        df = spark.createDataFrame(data, schema)

        print("📈 Sample data:")
        df.show(5)

        # Test basic profiling
        print("\n🔍 Running basic profiling...")
        profiler = DataFrameProfiler(df)
        # Get profile as dictionary for easier access
        profile = profiler.profile(output_format="dict")

        # Display results
        overview = profile["overview"]
        print(f"✅ Total Rows: {overview['total_rows']}")
        print(f"✅ Total Columns: {overview['total_columns']}")
        print(f"✅ Column Types: {list(overview['column_types'].keys())}")

        # Test specific column profiling
        print("\n🎯 Testing specific column profiling...")
        numeric_profile = profiler.profile(
            columns=["age", "salary"], output_format="dict"
        )

        for col_name, stats in numeric_profile["columns"].items():
            print(
                f"✅ {col_name}: min={stats.get('min')}, max={stats.get('max')}, mean={stats.get('mean', 0):.2f}"
            )

        # Test performance optimization
        print("\n⚡ Testing performance optimization...")
        optimized_profiler = DataFrameProfiler(df, optimize_for_large_datasets=True)
        optimized_profile = optimized_profiler.profile(output_format="dict")

        print(
            f"✅ Optimized profiling completed for {len(optimized_profile['columns'])} columns"
        )

        # Test pandas output (new default)
        print("\n🐼 Testing pandas output format...")
        pandas_profile = profiler.profile()  # Default is pandas
        print(f"✅ Pandas DataFrame shape: {pandas_profile.shape}")
        print(f"✅ Columns in pandas output: {list(pandas_profile.columns)[:5]}...")

        # Test output formatting
        print("\n📄 Testing output formatting...")
        from pyspark_analyzer.utils import format_profile_output

        summary = format_profile_output(profile, format_type="summary")
        print("✅ Summary format generated successfully")
        print(f"Summary length: {len(summary)} characters")

        json_output = format_profile_output(profile, format_type="json")
        print("✅ JSON format generated successfully")
        print(f"JSON length: {len(json_output)} characters")

        print(
            "\n🎉 Installation verification successful! The library is working correctly."
        )
        print("\n📖 Next steps:")
        print("   - Check out examples/basic_usage.py for comprehensive usage examples")
        print("   - Try examples/sampling_example.py for advanced sampling features")
        print("   - Read CLAUDE.md for development guidance")
        return True

    except Exception as e:
        print(f"❌ Installation verification failed: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Ensure PySpark is installed: pip install pyspark")
        print("   - Verify Java is installed and accessible")
        print("   - Check that JAVA_HOME is set correctly")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Clean up
        spark.stop()


if __name__ == "__main__":
    success = verify_installation()
    sys.exit(0 if success else 1)
