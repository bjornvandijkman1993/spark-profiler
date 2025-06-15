# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python project called "spark-profiler" designed for profiling Apache Spark applications. The project is currently in its initial state with minimal setup.

## Development Setup

- **Python Version**: Requires Python >=3.8
- **Project Management**: Uses pyproject.toml for dependency management
- **Package Manager**: uv (ultra-fast Python package installer)

## Key Commands

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv sync

# Install with all optional dependencies (including dev)
uv sync --all-extras

# Add a new dependency
uv add <package-name>

# Add a new dev dependency
uv add --dev <package-name>

# Run commands in the virtual environment
uv run python examples/installation_verification.py

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=spark_profiler

# Format code
uv run black spark_profiler/ tests/ examples/

# Type checking
uv run mypy spark_profiler/

# Lint code
uv run flake8 spark_profiler/

# Build package
uv run python -m build

# Activate virtual environment manually (optional)
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate  # Windows
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
# Basic usage with auto-sampling
profiler = DataFrameProfiler(spark_df)
profile = profiler.profile()

# Custom sampling configuration
from spark_profiler import SamplingConfig
config = SamplingConfig(target_size=100_000, seed=42)
profiler = DataFrameProfiler(spark_df, sampling_config=config)
profile = profiler.profile()

# Optimized for large datasets with sampling
profiler = DataFrameProfiler(spark_df, optimize_for_large_datasets=True)
profile = profiler.profile()

# Legacy sample_fraction (still supported)
profiler = DataFrameProfiler(spark_df, sample_fraction=0.1)
profile = profiler.profile()

# Check sampling information
sampling_info = profile['sampling']
print(f"Sample quality: {sampling_info['quality_score']:.3f}")
print(f"Speedup: {sampling_info['estimated_speedup']:.1f}x")
```

### Statistics Computed
- **Basic**: null counts, distinct counts, data types
- **Numeric**: min, max, mean, std, median, quartiles
- **String**: length statistics, empty string counts
- **Temporal**: date ranges, min/max dates

### Performance Optimizations
- **Intelligent Sampling**: Automatic sampling for datasets >10M rows with quality estimation
- **Configurable Sampling**: Custom target sizes, fractions, and quality thresholds
- **Quality Monitoring**: Statistical quality scores and confidence reporting
- **Approximate Functions**: Fast distinct counts and percentile computations
- **Batch Aggregations**: Minimize data scans with combined operations
- **DataFrame Caching**: Smart caching for multiple operations
- **Adaptive Partitioning**: Intelligent partitioning for different dataset sizes

### Sampling Features
- **Auto-Sampling**: Automatically applies sampling for large datasets (>10M rows)
- **Random Sampling**: Reproducible random sampling with seed control
- **Quality Estimation**: Statistical quality scores for sampling accuracy
- **Performance Monitoring**: Track sampling time and estimated speedup
- **Flexible Configuration**: Target size, fraction, or auto-determination
- **Legacy Support**: Backward compatibility with sample_fraction parameter
