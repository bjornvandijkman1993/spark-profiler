"""Shared test fixtures and configuration for pytest."""

import os
import subprocess
import sys
from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession


def setup_java_environment():
    """Setup Java environment for PySpark if not already configured.

    This function is called at module level to ensure Java is configured
    before any PySpark imports or operations.
    """
    # Skip if already configured
    if os.environ.get("_SPARK_PROFILER_JAVA_CONFIGURED"):
        return

    if not os.environ.get("JAVA_HOME"):
        # Try to find Java 17 in common locations
        java_paths = [
            "/opt/homebrew/opt/openjdk@17",  # Apple Silicon Macs
            "/usr/local/opt/openjdk@17",  # Intel Macs
        ]

        for path in java_paths:
            if os.path.exists(path):
                os.environ["JAVA_HOME"] = path
                os.environ["PATH"] = f"{path}/bin:{os.environ.get('PATH', '')}"
                break
        else:
            # Try to find Java using /usr/libexec/java_home on macOS
            try:
                result = subprocess.run(
                    ["/usr/libexec/java_home", "-v", "17"],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    java_home = result.stdout.strip()
                    os.environ["JAVA_HOME"] = java_home
                    os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
            except (FileNotFoundError, subprocess.SubprocessError):
                pass

    # Set other required environment variables
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    # Set PySpark to use the current Python interpreter
    if not os.environ.get("PYSPARK_PYTHON"):
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Reduce Spark log verbosity
    os.environ.setdefault("SPARK_SUBMIT_OPTS", "-Dlog4j.logLevel=ERROR")

    # Mark as configured to avoid repeated checks
    os.environ["_SPARK_PROFILER_JAVA_CONFIGURED"] = "1"


# Call setup at module level to ensure it runs before any tests
setup_java_environment()


@pytest.fixture(scope="session")
def spark_session():
    """Create a shared SparkSession for tests.

    This fixture is created once per test session and shared across all tests.
    It sets up Spark in local mode with minimal configuration to avoid Java issues.
    """

    # Create SparkSession with test configuration
    spark = (
        SparkSession.builder.appName("spark-profiler-tests")
        .master("local[1]")  # Use single thread for tests
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")  # Disable UI for tests
        .config("spark.sql.shuffle.partitions", "2")  # Reduce partitions for tests
        .config(
            "spark.sql.adaptive.enabled", "false"
        )  # Disable AQE for predictable tests
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config(
            "spark.sql.session.timeZone", "UTC"
        )  # Use UTC for consistent timestamp handling
        .getOrCreate()
    )

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    data = [
        (1, "Alice", 25, 50000.0),
        (2, "Bob", 30, 60000.0),
        (3, "Charlie", 35, 70000.0),
        (4, "David", 28, 55000.0),
        (5, "Eve", 32, 65000.0),
    ]
    columns = ["id", "name", "age", "salary"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def large_dataframe(spark_session):
    """Create a large DataFrame for testing sampling."""
    # Create a DataFrame with 10k rows (reduced from 100k for faster tests)
    return spark_session.range(0, 10000).selectExpr(
        "id",
        "id % 100 as category",
        "rand() * 1000 as value",
        "concat('user_', id) as name",
    )


@pytest.fixture
def null_dataframe(spark_session):
    """Create a DataFrame with null values for testing."""
    data = [
        (1, "Alice", None, 50000.0),
        (2, None, 30, 60000.0),
        (3, "Charlie", 35, None),
        (None, "David", 28, 55000.0),
        (5, "Eve", None, None),
    ]
    columns = ["id", "name", "age", "salary"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def simple_dataframe(spark_session):
    """Create a simple DataFrame with id, name, value columns for legacy tests."""
    data = [
        (1, "Alice", 100.5),
        (2, "Bob", 200.3),
        (3, "", 150.7),  # Empty string
        (4, "David", None),  # Null value
        (5, "Eve", 250.0),
    ]
    columns = ["id", "name", "value"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def mock_agg_result():
    """Create a mock aggregation result for testing."""
    mock_row = Mock()
    mock_row.__getitem__ = Mock(
        side_effect=lambda x: {
            "total_count": 5,
            "non_null_count": 4,
            "null_count": 1,
            "distinct_count": 3,
            "min": 10.0,
            "max": 100.0,
            "mean": 55.0,
            "std": 30.28,
            "median": 55.0,
            "q1": 30.0,
            "q3": 80.0,
            "min_length": 0,
            "max_length": 23,
            "avg_length": 8.4,
            "empty_count": 1,
        }.get(x, 0)
    )

    mock_result = Mock()
    mock_result.collect.return_value = [mock_row]
    return mock_result


@pytest.fixture
def mock_dataframe():
    """Create a mock DataFrame for testing without Spark operations."""
    mock_df = Mock()
    mock_df.count.return_value = 5
    mock_df.columns = ["id", "name", "value"]
    mock_df.schema = Mock()
    mock_df.schema.__getitem__ = Mock()
    mock_df.cache.return_value = mock_df
    mock_df.unpersist.return_value = mock_df
    return mock_df
