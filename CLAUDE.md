# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python project called "spark-profiler" designed for profiling Apache Spark applications. The project is currently in its initial state with minimal setup.

## Development Setup

- **Python Version**: Requires Python >=3.13
- **Project Management**: Uses pyproject.toml for dependency management
- **Package Manager**: Standard pip/uv workflow

## Key Commands

```bash
# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e ".[dev]"

# Run tests
python -m pytest

# Run tests with coverage
python -m pytest --cov=spark_profiler

# Format code
python -m black spark_profiler/ tests/ examples/

# Type checking
python -m mypy spark_profiler/

# Lint code
python -m flake8 spark_profiler/

# Build package
python -m build
```

## Architecture Overview

The library is structured as follows:

### Core Components
- **`profiler.py`**: Main `DataFrameProfiler` class that orchestrates the profiling process
- **`statistics.py`**: `StatisticsComputer` class that handles individual statistic computations
- **`performance.py`**: `BatchStatisticsComputer` and optimization utilities for large datasets
- **`utils.py`**: Helper functions for data type detection and output formatting

### Key Design Principles
1. **Single-pass optimization**: Minimize DataFrame scans by combining multiple aggregations
2. **Type-aware statistics**: Different statistics computed based on column data types
3. **Performance scaling**: Batch processing and caching for large datasets
4. **Flexible output**: Multiple output formats (dict, JSON, summary report)

### Usage Patterns
```python
# Basic usage
profiler = DataFrameProfiler(spark_df)
profile = profiler.profile()

# Optimized for large datasets
profiler = DataFrameProfiler(spark_df, optimize_for_large_datasets=True)
profile = profiler.profile()

# Sample for faster profiling
profiler = DataFrameProfiler(spark_df, sample_fraction=0.1)
profile = profiler.profile()
```

### Statistics Computed
- **Basic**: null counts, distinct counts, data types
- **Numeric**: min, max, mean, std, median, quartiles
- **String**: length statistics, empty string counts
- **Temporal**: date ranges, min/max dates

### Performance Optimizations
- Approximate distinct counts for speed
- Batch aggregations to minimize data scans
- DataFrame caching for multiple operations
- Intelligent partitioning for different dataset sizes