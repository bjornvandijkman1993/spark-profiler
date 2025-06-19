# Exception Handling Guide

## Overview

The pyspark-analyzer library provides a comprehensive exception hierarchy to help users handle errors gracefully and understand what went wrong during profiling operations.

## Exception Hierarchy

```
ProfilingError (base exception)
├── ConfigurationError - Invalid configuration parameters
├── InvalidDataError - Issues with input data
│   ├── DataTypeError - Wrong data types
│   └── ColumnNotFoundError - Missing columns
├── SamplingError - Sampling operation failures
├── StatisticsError - Statistics computation failures
└── SparkOperationError - Spark-specific errors with context
```

## Common Exceptions

### ConfigurationError

Raised when configuration parameters are invalid.

**Common causes:**
- Invalid sampling fraction (not between 0 and 1)
- Negative target_rows
- Specifying both target_rows and fraction
- Invalid output format

**Example:**
```python
from pyspark_analyzer import analyze, ConfigurationError

try:
    # This will raise ConfigurationError
    result = analyze(df, fraction=1.5)
except ConfigurationError as e:
    print(f"Configuration error: {e}")
    # Use valid fraction between 0 and 1
    result = analyze(df, fraction=0.1)
```

### ColumnNotFoundError

Raised when specified columns don't exist in the DataFrame.

**Features:**
- Lists the missing columns
- Shows available columns (first 10)
- Helps identify typos or naming issues

**Example:**
```python
from pyspark_analyzer import analyze, ColumnNotFoundError

try:
    result = analyze(df, columns=["user_id", "username"])
except ColumnNotFoundError as e:
    print(f"Missing columns: {e.missing_columns}")
    print(f"Available columns: {e.available_columns}")
    # Use correct column names
    result = analyze(df, columns=["id", "name"])
```

### DataTypeError

Raised when input is not a valid PySpark DataFrame.

**Example:**
```python
from pyspark_analyzer import analyze, DataTypeError

try:
    # This will raise DataTypeError
    result = analyze("not a dataframe")
except DataTypeError as e:
    print(f"Invalid input: {e}")
    # Pass a valid PySpark DataFrame
    result = analyze(spark_df)
```

### SparkOperationError

Wraps Spark-specific errors with additional context.

**Features:**
- Preserves original Spark exception
- Adds context about which operation failed
- Helps debug Spark-related issues

**Example:**
```python
from pyspark_analyzer import analyze, SparkOperationError

try:
    result = analyze(corrupted_df)
except SparkOperationError as e:
    print(f"Spark error: {e}")
    if e.original_exception:
        print(f"Original error: {e.original_exception}")
    # Handle or log the error appropriately
```

### SamplingError

Raised when sampling operations fail.

**Common causes:**
- DataFrame is too small for requested sample
- Spark resource constraints
- Invalid sampling parameters

**Example:**
```python
from pyspark_analyzer import analyze, SamplingError

try:
    # May fail if DataFrame has issues
    result = analyze(df, target_rows=1000000)
except SamplingError as e:
    print(f"Sampling failed: {e}")
    # Try without sampling
    result = analyze(df, sampling=False)
```

### StatisticsError

Raised when statistics computation fails.

**Common causes:**
- Computation errors (e.g., division by zero)
- Memory constraints
- Data type incompatibilities

**Example:**
```python
from pyspark_analyzer import analyze, StatisticsError

try:
    result = analyze(df, include_advanced=True)
except StatisticsError as e:
    print(f"Statistics computation failed: {e}")
    # Try basic statistics only
    result = analyze(df, include_advanced=False)
```

## Best Practices

### 1. Specific Exception Handling

Catch specific exceptions rather than generic ones:

```python
# Good
try:
    result = analyze(df, columns=user_columns)
except ColumnNotFoundError as e:
    # Handle missing columns specifically
    logger.warning(f"Some columns not found: {e.missing_columns}")
    result = analyze(df)  # Profile all columns instead
except ConfigurationError as e:
    # Handle configuration issues
    logger.error(f"Invalid configuration: {e}")
    raise

# Avoid
try:
    result = analyze(df, columns=user_columns)
except Exception as e:
    # Too generic
    print(f"Something went wrong: {e}")
```

### 2. Error Recovery

Implement graceful degradation:

```python
from pyspark_analyzer import analyze, SamplingError, StatisticsError

def robust_analyze(df, **kwargs):
    """Analyze DataFrame with fallback options."""
    try:
        # Try with all features
        return analyze(df, **kwargs)
    except SamplingError:
        # Retry without sampling
        logger.warning("Sampling failed, analyzing full dataset")
        kwargs['sampling'] = False
        return analyze(df, **kwargs)
    except StatisticsError:
        # Retry with basic stats only
        logger.warning("Advanced stats failed, using basic only")
        kwargs['include_advanced'] = False
        kwargs['include_quality'] = False
        return analyze(df, **kwargs)
```

### 3. Logging and Monitoring

Log exceptions for debugging:

```python
import logging
from pyspark_analyzer import analyze, SparkOperationError

logger = logging.getLogger(__name__)

try:
    result = analyze(df)
except SparkOperationError as e:
    # Log with context
    logger.error(
        "Profiling failed",
        extra={
            "error_type": type(e).__name__,
            "error_message": str(e),
            "has_original": e.original_exception is not None,
            "dataframe_columns": df.columns if df else None
        }
    )
    raise
```

### 4. Validation Before Processing

Validate inputs early:

```python
from pyspark_analyzer import analyze, ConfigurationError

def validate_and_analyze(df, columns=None, fraction=None):
    """Validate parameters before analysis."""
    # Validate DataFrame
    if df is None or df.rdd.isEmpty():
        raise ValueError("DataFrame is empty or None")

    # Validate columns exist
    if columns:
        missing = set(columns) - set(df.columns)
        if missing:
            raise ValueError(f"Columns not found: {missing}")

    # Validate fraction
    if fraction is not None and not (0 < fraction <= 1):
        raise ValueError("Fraction must be between 0 and 1")

    # Now safe to analyze
    return analyze(df, columns=columns, fraction=fraction)
```

## Debugging Tips

### 1. Enable Detailed Logging

```python
import logging

# Enable debug logging for pyspark_analyzer
logging.getLogger('pyspark_analyzer').setLevel(logging.DEBUG)
```

### 2. Inspect Original Exceptions

When catching SparkOperationError, always check the original exception:

```python
try:
    result = analyze(df)
except SparkOperationError as e:
    print(f"Our error: {e}")
    if e.original_exception:
        print(f"Spark error: {e.original_exception}")
        # Original stack trace is preserved
        import traceback
        traceback.print_exception(type(e.original_exception),
                                  e.original_exception,
                                  e.original_exception.__traceback__)
```

### 3. Test Error Conditions

Write tests for error handling:

```python
import pytest
from pyspark_analyzer import analyze, ConfigurationError

def test_invalid_configuration():
    """Test that invalid config raises appropriate errors."""
    with pytest.raises(ConfigurationError, match="fraction must be between"):
        analyze(df, fraction=2.0)

    with pytest.raises(ConfigurationError, match="Cannot specify both"):
        analyze(df, target_rows=1000, fraction=0.1)
```

## Migration Guide

If you're upgrading from an older version that used generic exceptions:

### Before (old code):
```python
try:
    result = analyze(df, fraction=1.5)
except ValueError as e:
    print(f"Error: {e}")
```

### After (new code):
```python
from pyspark_analyzer import ConfigurationError

try:
    result = analyze(df, fraction=1.5)
except ConfigurationError as e:
    print(f"Configuration error: {e}")
```

The new exceptions provide better context and are more specific, making debugging easier.
