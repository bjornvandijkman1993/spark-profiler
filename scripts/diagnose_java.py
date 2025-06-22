#!/usr/bin/env python
"""Diagnostic script to check Java and PySpark setup."""

import os
import platform
import subprocess  # nosec B404
import sys


def check_java():
    """Check Java installation and configuration."""
    print("🔍 Checking Java installation...")

    # Check if java command exists
    try:
        result = subprocess.run(
            ["java", "-version"], capture_output=True, text=True
        )  # nosec B607, B603
        if result.returncode == 0:
            print("✅ Java is installed")
            print(f"   Version info: {result.stderr.strip().split('\\n')[0]}")
        else:
            print("❌ Java command failed")
            return False
    except FileNotFoundError:
        print("❌ Java is not installed or not in PATH")
        return False

    # Check JAVA_HOME
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        print(f"✅ JAVA_HOME is set: {java_home}")
        if os.path.exists(java_home):
            print("   Directory exists")
        else:
            print("   ⚠️  Directory does not exist!")
    else:
        print("⚠️  JAVA_HOME is not set")

        # Try to suggest JAVA_HOME based on platform
        if platform.system() == "Darwin":  # macOS
            try:
                result = subprocess.run(
                    ["/usr/libexec/java_home"], capture_output=True, text=True
                )  # nosec B603
                if result.returncode == 0:
                    suggested_home = result.stdout.strip()
                    print(f"   💡 Suggested JAVA_HOME: {suggested_home}")
                    print(f"   Run: export JAVA_HOME={suggested_home}")
            except Exception:
                # If java_home command fails, it's likely not on macOS or command not available
                print("   ⚠️  Could not determine Java home location automatically")

    return True


def check_pyspark():
    """Check PySpark installation."""
    print("\n🔍 Checking PySpark installation...")

    try:
        import pyspark

        print(f"✅ PySpark is installed: version {pyspark.__version__}")

        # Check if we can create a SparkSession
        print("\n🔍 Testing SparkSession creation...")

        # Set environment variables
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

        from pyspark.sql import SparkSession

        try:
            spark = (
                SparkSession.builder.appName("DiagnosticTest")
                .master("local[1]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.ui.enabled", "false")
                .getOrCreate()
            )

            print("✅ SparkSession created successfully")

            # Test basic functionality
            df = spark.range(5)
            count = df.count()
            print(f"✅ Basic Spark operations work (count: {count})")

            spark.stop()
            return True

        except Exception as e:
            print(f"❌ Failed to create SparkSession: {type(e).__name__}")
            print(f"   Error: {e!s}")

            # Common error hints
            if "Java gateway process exited" in str(e):
                print("\n💡 Hints:")
                print("   - Make sure Java is properly installed")
                print("   - Set JAVA_HOME environment variable")
                print("   - Try: export SPARK_LOCAL_IP=127.0.0.1")

            return False

    except ImportError:
        print("❌ PySpark is not installed")
        print("   Run: uv add pyspark")
        return False


def check_environment():
    """Check environment variables."""
    print("\n🔍 Checking environment variables...")

    important_vars = [
        "JAVA_HOME",
        "SPARK_HOME",
        "SPARK_LOCAL_IP",
        "PYSPARK_PYTHON",
        "PYSPARK_DRIVER_PYTHON",
    ]

    for var in important_vars:
        value = os.environ.get(var)
        if value:
            print(f"✅ {var} = {value}")
        else:
            print(f"   {var} is not set")


def main():
    """Run all diagnostics."""
    print("🏥 PySpark Diagnostic Tool")
    print("=" * 50)

    # System info
    print(f"Platform: {platform.system()} {platform.release()}")
    print(f"Python: {sys.version.split()[0]}")
    print(f"Python executable: {sys.executable}")
    print("=" * 50)

    # Run checks
    java_ok = check_java()
    check_environment()
    pyspark_ok = check_pyspark()

    # Summary
    print("\n" + "=" * 50)
    print("📊 Summary:")
    if java_ok and pyspark_ok:
        print("✅ All checks passed! You should be able to run tests.")
        print("\n🎯 Next steps:")
        print("   1. Run: source .env")
        print("   2. Run: uv run pytest")
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        print("\n🎯 Quick fixes:")
        if not java_ok:
            print("   - Install Java: brew install openjdk@17")
        print("   - Run setup: ./scripts/setup_test_environment.sh")
        print("   - Load env: source .env")


if __name__ == "__main__":
    main()
