"""Shared test fixtures and configuration for pytest."""

import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Create a shared SparkSession for tests.

    This fixture is created once per test session and shared across all tests.
    It sets up Spark in local mode with minimal configuration to avoid Java issues.
    """
    # Set Java options to avoid common issues
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

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
    # Create a DataFrame with 100k rows
    return spark_session.range(0, 100000).selectExpr(
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
