#!/usr/bin/env python3
"""
Installation verification for pyspark-analyzer.
"""

import sys

from pyspark.sql import SparkSession

from pyspark_analyzer import analyze


def verify_installation():
    """Verify the profiler installation and basic functionality."""
    print("Verifying PySpark DataFrame Profiler Installation...")

    # Create Spark session
    print("Creating Spark session...")
    spark = (
        SparkSession.builder.appName("ProfilerTest").master("local[*]").getOrCreate()
    )
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    try:
        # Create test data
        print("Creating test DataFrame...")
        data = [
            (1, "John", 30, 75000.0, "Engineering"),
            (2, "Jane", 25, 65000.0, "Marketing"),
            (3, "Bob", 35, 85000.0, "Engineering"),
            (4, None, 28, 70000.0, "Sales"),
            (5, "Alice", None, 80000.0, "Engineering"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "age", "salary", "department"])

        print("\nTest data:")
        df.show()

        # Test basic profiling
        print("Running basic profiling...")
        profile = analyze(df, output_format="dict", sampling=False)

        # Display results
        overview = profile["overview"]
        print(f"✓ Total Rows: {overview['total_rows']}")
        print(f"✓ Total Columns: {overview['total_columns']}")
        print(f"✓ Column Types: {list(overview['column_types'].keys())}")

        # Test specific column profiling
        print("\nTesting column profiling...")
        numeric_profile = analyze(
            df, columns=["age", "salary"], output_format="dict", sampling=False
        )

        for col, stats in numeric_profile["columns"].items():
            print(
                f"✓ {col}: min={stats['min']}, max={stats['max']}, mean={stats['mean']:.0f}"
            )

        # Test pandas output
        print("\nTesting pandas output...")
        pandas_profile = analyze(df, sampling=False)
        print(f"✓ Pandas DataFrame shape: {pandas_profile.shape}")

        # Test output formatting
        print("\nTesting output formatting...")
        from pyspark_analyzer.utils import format_profile_output

        summary = format_profile_output(profile, format_type="summary")
        print(f"✓ Summary format: {len(summary)} characters")

        json_output = format_profile_output(profile, format_type="json")
        print(f"✓ JSON format: {len(json_output)} characters")

        print("\nInstallation verification successful!")
        print("\nNext steps:")
        print("  - Check examples/basic_usage.py for usage examples")
        print("  - Try examples/sampling_example.py for large datasets")
        return True

    except Exception as e:
        print(f"\nInstallation verification failed: {e}")
        print("\nTroubleshooting:")
        print("  - Ensure PySpark is installed: pip install pyspark")
        print("  - Verify Java is installed (version 8+)")
        print("  - Check JAVA_HOME environment variable")
        import traceback

        traceback.print_exc()
        return False

    finally:
        spark.stop()


if __name__ == "__main__":
    success = verify_installation()
    sys.exit(0 if success else 1)
