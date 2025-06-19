# Logging Guide for pyspark-analyzer

This guide explains how to configure and use logging in the pyspark-analyzer library.

## Overview

pyspark-analyzer uses Python's standard `logging` module to provide flexible, hierarchical logging throughout the library. By default, logging is set to WARNING level to minimize output in production environments.

## Quick Start

### Basic Usage

```python
import pyspark_analyzer

# Enable INFO level logging
pyspark_analyzer.set_log_level("INFO")

# Or configure logging with custom settings
pyspark_analyzer.configure_logging(
    level="DEBUG",
    format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Now analyze your DataFrame with logging enabled
profile = pyspark_analyzer.analyze(df)
```

### Using Environment Variables

Set the log level via environment variable:

```bash
export PYSPARK_ANALYZER_LOG_LEVEL=DEBUG
python your_script.py
```

## Log Levels

The library uses standard Python log levels:

- **DEBUG**: Detailed information for diagnosing problems
  - Sampling decisions and calculations
  - Partition optimization details
  - Individual column processing steps

- **INFO**: General informational messages
  - Analysis start/completion
  - Sampling applied (with ratios)
  - Major processing milestones

- **WARNING**: Warning messages (default level)
  - Empty DataFrames
  - Legacy API usage
  - Performance concerns

- **ERROR**: Error messages before exceptions
  - Invalid parameters
  - Missing columns
  - Type mismatches

## Configuration Options

### Programmatic Configuration

```python
from pyspark_analyzer import configure_logging, set_log_level

# Simple level change
set_log_level("DEBUG")

# Full configuration
configure_logging(
    level="INFO",
    format_string="%(levelname)s:%(name)s:%(message)s",
    disable_existing_loggers=False
)

# Disable logging completely
from pyspark_analyzer import disable_logging
disable_logging()
```

### Logger Hierarchy

The library uses a hierarchical logger structure:

- `pyspark_analyzer` - Root logger
  - `pyspark_analyzer.api` - API entry points
  - `pyspark_analyzer.profiler` - Core profiling logic
  - `pyspark_analyzer.sampling` - Sampling decisions
  - `pyspark_analyzer.statistics` - Statistical computations
  - `pyspark_analyzer.performance` - Performance optimizations

You can configure specific loggers:

```python
import logging

# Configure only sampling logs
logging.getLogger("pyspark_analyzer.sampling").setLevel(logging.DEBUG)

# Configure only statistics logs
logging.getLogger("pyspark_analyzer.statistics").setLevel(logging.INFO)
```

## Integration with Application Logging

### Using with Existing Logging Configuration

```python
import logging
import pyspark_analyzer

# Your application's logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)

# pyspark-analyzer will respect your configuration
# Just set the desired level for the library
logging.getLogger("pyspark_analyzer").setLevel(logging.DEBUG)
```

### Capturing Logs to File

```python
import logging
from pyspark_analyzer import get_logger

# Add a file handler to pyspark-analyzer logs
logger = get_logger("pyspark_analyzer")
file_handler = logging.FileHandler("profiling.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)
```

## Log Output Examples

### INFO Level Output

```
2024-01-15 10:30:45 - pyspark_analyzer.api - INFO - Starting DataFrame analysis with parameters: sampling=None, target_rows=None, fraction=None, columns=None, output_format=pandas
2024-01-15 10:30:45 - pyspark_analyzer.sampling - INFO - Auto-sampling triggered for large dataset: 15,000,000 rows -> 1,000,000 rows (fraction: 0.0667)
2024-01-15 10:30:46 - pyspark_analyzer.profiler - INFO - Sampling applied: 15000000 rows -> 1000000 rows (fraction: 0.0667)
2024-01-15 10:30:46 - pyspark_analyzer.profiler - INFO - Profiling 10 columns: ['id', 'name', 'age', 'salary', 'department']...
2024-01-15 10:30:48 - pyspark_analyzer.statistics - INFO - Batch computation completed for 10 columns
2024-01-15 10:30:48 - pyspark_analyzer.api - INFO - DataFrame analysis completed successfully
```

### DEBUG Level Output

```
2024-01-15 10:30:45 - pyspark_analyzer.api - DEBUG - Sampling configuration: SamplingConfig(enabled=True, target_rows=None, fraction=None, seed=42)
2024-01-15 10:30:45 - pyspark_analyzer.sampling - DEBUG - Computing row count for sampling decision
2024-01-15 10:30:45 - pyspark_analyzer.sampling - DEBUG - DataFrame has 15,000,000 rows
2024-01-15 10:30:45 - pyspark_analyzer.performance - DEBUG - Optimizing DataFrame with row_count=1000000
2024-01-15 10:30:45 - pyspark_analyzer.performance - DEBUG - Manual partition optimization: current partitions = 200
2024-01-15 10:30:45 - pyspark_analyzer.performance - DEBUG - Cluster config: default_parallelism=8, shuffle_partitions=200
2024-01-15 10:30:45 - pyspark_analyzer.performance - DEBUG - Data size estimate: 1220.70 MB, target partitions: 9
2024-01-15 10:30:46 - pyspark_analyzer.statistics - DEBUG - Computing basic statistics for column: age
2024-01-15 10:30:46 - pyspark_analyzer.statistics - DEBUG - Computing numeric statistics for column: age, advanced=True
```

## Performance Considerations

- Logging can impact performance, especially at DEBUG level
- Use INFO level or higher in production environments
- Consider disabling logging for batch processing:

```python
# Temporarily disable logging for batch processing
import pyspark_analyzer
import logging

# Save current level
original_level = logging.getLogger("pyspark_analyzer").level

# Disable logging
pyspark_analyzer.disable_logging()

# Process many DataFrames
for df in dataframes:
    profile = pyspark_analyzer.analyze(df)
    # Process profile...

# Restore logging
logging.getLogger("pyspark_analyzer").setLevel(original_level)
```

## Troubleshooting

### No Log Output

If you're not seeing logs:

1. Check the log level:
   ```python
   import logging
   print(logging.getLogger("pyspark_analyzer").level)
   ```

2. Ensure handlers are configured:
   ```python
   logger = logging.getLogger("pyspark_analyzer")
   print(f"Handlers: {logger.handlers}")
   print(f"Propagate: {logger.propagate}")
   ```

3. Try explicit configuration:
   ```python
   import pyspark_analyzer
   pyspark_analyzer.configure_logging(level="DEBUG")
   ```

### Too Much Output

To reduce log verbosity:

```python
# Only show warnings and errors
pyspark_analyzer.set_log_level("WARNING")

# Or disable specific noisy loggers
logging.getLogger("pyspark_analyzer.statistics").setLevel(logging.WARNING)
```

### Logging in Distributed Environments

When running on a Spark cluster, logs from executors won't appear in your driver logs. To see executor logs:

1. Check your cluster's log aggregation system (e.g., YARN logs, Kubernetes logs)
2. Use Spark's logging configuration to capture executor logs
3. Consider using structured logging for better log aggregation

## Best Practices

1. **Production**: Use WARNING level or higher
2. **Development**: Use INFO level for general development, DEBUG for troubleshooting
3. **Testing**: Consider using DEBUG level with file output
4. **Performance Testing**: Disable logging to get accurate measurements
5. **Log Rotation**: When logging to files, implement log rotation to manage disk space

## API Reference

### Functions

- `configure_logging(level=None, format_string=None, disable_existing_loggers=False)`: Configure logging for the library
- `set_log_level(level)`: Set the log level for all pyspark-analyzer loggers
- `disable_logging()`: Disable all logging from pyspark-analyzer
- `get_logger(name)`: Get a logger instance for the given module name

### Environment Variables

- `PYSPARK_ANALYZER_LOG_LEVEL`: Set the default log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
