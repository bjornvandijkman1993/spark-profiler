"""Tests for logging functionality."""

import logging
from io import StringIO
import pytest
from pyspark.sql import SparkSession

import pyspark_analyzer
from pyspark_analyzer.logging import (
    get_logger,
    set_log_level,
    configure_logging,
    disable_logging,
)


@pytest.fixture
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder.appName("test_logging").getOrCreate()


@pytest.fixture
def sample_df(spark):
    """Create a small sample DataFrame for testing."""
    data = [
        (1, "Alice", 25, 50000.0),
        (2, "Bob", 30, 60000.0),
        (3, "Charlie", 35, 70000.0),
        (4, "David", 40, 80000.0),
        (5, "Eve", 45, 90000.0),
    ]
    columns = ["id", "name", "age", "salary"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def log_capture():
    """Capture log output for testing."""
    # Create a string buffer to capture logs
    log_capture_string = StringIO()
    ch = logging.StreamHandler(log_capture_string)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)

    # Get the pyspark_analyzer logger
    logger = logging.getLogger("pyspark_analyzer")
    original_level = logger.level
    original_handlers = logger.handlers[:]

    # Clear existing handlers and add our capture handler
    logger.handlers = []
    logger.addHandler(ch)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    yield log_capture_string

    # Restore original configuration
    logger.handlers = original_handlers
    logger.setLevel(original_level)
    logger.propagate = True


class TestLoggingConfiguration:
    """Test logging configuration functions."""

    def test_get_logger(self):
        """Test getting a logger instance."""
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_module"

    def test_set_log_level(self):
        """Test setting log level."""
        logger = logging.getLogger("pyspark_analyzer")

        # Set to DEBUG
        set_log_level("DEBUG")
        assert logger.level == logging.DEBUG

        # Set to INFO
        set_log_level("INFO")
        assert logger.level == logging.INFO

        # Set to WARNING
        set_log_level("WARNING")
        assert logger.level == logging.WARNING

    def test_set_invalid_log_level(self):
        """Test setting invalid log level raises error."""
        with pytest.raises(ValueError, match="Invalid log level"):
            set_log_level("INVALID")

    def test_configure_logging(self):
        """Test full logging configuration."""
        configure_logging(level="INFO", format_string="%(levelname)s - %(message)s")

        logger = logging.getLogger("pyspark_analyzer")
        assert logger.level == logging.INFO
        assert len(logger.handlers) > 0

    def test_configure_logging_from_env(self, monkeypatch):
        """Test configuration from environment variable."""
        monkeypatch.setenv("PYSPARK_ANALYZER_LOG_LEVEL", "ERROR")

        # Reconfigure to pick up env var
        configure_logging()

        logger = logging.getLogger("pyspark_analyzer")
        assert logger.level == logging.ERROR

    def test_disable_logging(self):
        """Test disabling logging."""
        logger = logging.getLogger("pyspark_analyzer")

        # Disable logging
        disable_logging()

        # Logger level should be higher than CRITICAL
        assert logger.level > logging.CRITICAL


class TestLoggingOutput:
    """Test actual logging output from the library."""

    def test_api_logging(self, sample_df, log_capture):
        """Test logging from analyze() function."""
        # Configure logging to capture output
        configure_logging(level="INFO")

        # Run analysis
        pyspark_analyzer.analyze(sample_df)

        # Check log output
        log_contents = log_capture.getvalue()
        assert "Starting DataFrame analysis" in log_contents
        assert "DataFrame analysis completed successfully" in log_contents

    def test_debug_logging(self, sample_df, log_capture):
        """Test debug level logging."""
        # Configure logging to capture debug output
        configure_logging(level="DEBUG")

        # Run analysis
        pyspark_analyzer.analyze(sample_df)

        # Check log output
        log_contents = log_capture.getvalue()

        # Should see debug messages
        assert "Sampling configuration:" in log_contents
        assert "Computing basic statistics" in log_contents
        assert "Starting batch computation" in log_contents

    def test_error_logging(self, spark, log_capture):
        """Test error logging before exceptions."""
        from pyspark_analyzer import DataTypeError

        # Configure logging
        configure_logging(level="ERROR")

        # Try to analyze invalid input
        with pytest.raises(DataTypeError):
            pyspark_analyzer.analyze("not a dataframe")

        # Check error was logged
        log_contents = log_capture.getvalue()
        assert "ERROR" in log_contents
        assert "Input must be a PySpark DataFrame" in log_contents

    def test_sampling_logging(self, spark, log_capture):
        """Test sampling decision logging."""
        # Create a large DataFrame that triggers auto-sampling
        large_df = spark.range(15_000_000).toDF("id")

        # Configure logging
        configure_logging(level="INFO")

        # Run analysis
        pyspark_analyzer.analyze(large_df)

        # Check sampling was logged
        log_contents = log_capture.getvalue()
        assert "Auto-sampling triggered" in log_contents
        assert "15,000,000 rows" in log_contents

    def test_no_logging_by_default(self, sample_df, log_capture):
        """Test that WARNING level suppresses most logs by default."""
        # Reset to default configuration
        configure_logging(level="WARNING")

        # Run analysis
        pyspark_analyzer.analyze(sample_df)

        # Should have minimal output at WARNING level
        log_contents = log_capture.getvalue()
        assert "Starting DataFrame analysis" not in log_contents
        assert "INFO" not in log_contents
        assert "DEBUG" not in log_contents


class TestLoggerHierarchy:
    """Test hierarchical logger configuration."""

    def test_module_specific_logging(self, sample_df, log_capture):
        """Test configuring specific module loggers."""
        # Set different levels for different modules
        logging.getLogger("pyspark_analyzer.api").setLevel(logging.INFO)
        logging.getLogger("pyspark_analyzer.sampling").setLevel(logging.DEBUG)
        logging.getLogger("pyspark_analyzer.statistics").setLevel(logging.WARNING)

        # Run analysis
        pyspark_analyzer.analyze(sample_df)

        log_contents = log_capture.getvalue()

        # API logs should appear (INFO level)
        assert "pyspark_analyzer.api - INFO" in log_contents

        # Sampling debug logs should appear
        assert "pyspark_analyzer.sampling - DEBUG" in log_contents

        # Statistics info/debug logs should NOT appear (WARNING level)
        assert "pyspark_analyzer.statistics - INFO" not in log_contents
        assert "pyspark_analyzer.statistics - DEBUG" not in log_contents


class TestLoggingIntegration:
    """Test integration with user's logging configuration."""

    def test_respects_existing_handlers(self):
        """Test that library respects existing logging configuration."""
        # Set up custom handler
        custom_handler = logging.StreamHandler()
        custom_handler.setLevel(logging.INFO)

        root_logger = logging.getLogger()
        root_logger.addHandler(custom_handler)

        # Configure library logging
        configure_logging(level="DEBUG")

        # Library logger should have its own handler
        lib_logger = logging.getLogger("pyspark_analyzer")
        assert len(lib_logger.handlers) > 0

        # But shouldn't affect root logger
        assert custom_handler in root_logger.handlers

    def test_log_suppression_performance(self, sample_df):
        """Test that disabling logging completes without errors."""
        # Disable logging
        disable_logging()

        # Run analysis multiple times - should complete without errors
        for _ in range(5):
            pyspark_analyzer.analyze(sample_df)

        # Re-enable logging at WARNING level
        configure_logging(level="WARNING")
