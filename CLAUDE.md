# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python project called "pyspark-analyzer" designed for profiling Apache Spark applications. The project is currently in its initial state with minimal setup.

## Development Setup

- **Python Version**: Requires Python >=3.8
- **Java Version**: Requires Java 17+ for PySpark (automatically detected by test scripts)
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

# Run tests (automatically sets up Java environment)
make test

# Run tests with coverage
make test-cov

# Run tests quickly (stop on first failure)
make test-quick

# Alternative: Run tests directly with uv (requires .env file)
source .env && uv run pytest

# Alternative: Run tests with coverage directly
source .env && uv run pytest --cov=pyspark_analyzer

# Format code
uv run black pyspark_analyzer/ tests/ examples/

# Type checking
uv run mypy pyspark_analyzer/

# Lint code
uv run flake8 pyspark_analyzer/

# Build package
uv run python -m build

# Activate virtual environment manually (optional)
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate  # Windows
```

## Java Setup for PySpark

PySpark requires Java 17+ to run. The test commands in the Makefile automatically handle Java setup:

```bash
# First-time setup (automatic when running tests)
./scripts/setup_test_environment.sh

# This creates a .env file with Java environment variables:
# - JAVA_HOME: Path to Java 17 installation
# - PATH: Updated to include Java binaries
# - SPARK_LOCAL_IP: Set to 127.0.0.1 for local testing
# - PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON: Set to virtual env Python

# To manually install Java 17 on macOS:
brew install openjdk@17

# The setup script automatically detects Java from common locations:
# - /opt/homebrew/opt/openjdk@17 (Apple Silicon Macs)
# - /usr/local/opt/openjdk@17 (Intel Macs)
# - /Library/Java/JavaVirtualMachines/*/Contents/Home
# - And other standard locations
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
from pyspark_analyzer import SamplingConfig
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
