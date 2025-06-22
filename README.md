# 🔍 PySpark DataFrame Profiler

[![CI](https://github.com/bjornvandijkman1993/pyspark-analyzer/workflows/CI/badge.svg)](https://github.com/bjornvandijkman1993/pyspark-analyzer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bjornvandijkman1993/pyspark-analyzer/branch/main/graph/badge.svg)](https://codecov.io/gh/bjornvandijkman1993/pyspark-analyzer)
[![Documentation Status](https://readthedocs.org/projects/pyspark-analyzer/badge/?version=latest)](https://pyspark-analyzer.readthedocs.io/en/latest/?badge=latest)
[![Security](https://github.com/bjornvandijkman1993/pyspark-analyzer/workflows/CodeQL/badge.svg)](https://github.com/bjornvandijkman1993/pyspark-analyzer/security)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://img.shields.io/pypi/v/pyspark-analyzer.svg)](https://pypi.org/project/pyspark-analyzer/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive PySpark DataFrame profiler for generating detailed statistics and data quality reports with intelligent sampling capabilities for large-scale datasets.

📚 **[Documentation](https://pyspark-analyzer.readthedocs.io)** | 🐛 **[Issues](https://github.com/bjornvandijkman1993/pyspark-analyzer/issues)** | 💡 **[Examples](https://github.com/bjornvandijkman1993/pyspark-analyzer/tree/main/examples)**

## ✨ Features

- **🚀 Intelligent Sampling**: Automatic sampling for datasets >10M rows with quality estimation
- **📊 Comprehensive Statistics**: Null counts, distinct values, min/max, mean, std, median, quartiles
- **🎯 Type-Aware Analysis**: Specialized statistics for numeric, string, and temporal columns
- **⚡ Performance Optimized**: Single-pass aggregations, batch processing, approximate functions
- **🔍 Quality Monitoring**: Statistical quality scores and confidence reporting
- **🎨 Flexible Output**: Dictionary, JSON, and human-readable summary formats
- **📈 Large Dataset Support**: Intelligent caching, partitioning, and sampling options
- **🎯 Adaptive Partitioning**: Smart partition optimization considering AQE, data size, and cluster configuration

## 🚀 Quick Start

### Prerequisites

- Python >=3.8
- PySpark >=3.0.0 (native median function available in 3.4.0+, fallback for older versions)
- Java 17+ (required for PySpark)
  - macOS: `brew install openjdk@17`
  - Ubuntu/Debian: `sudo apt-get install openjdk-17-jdk`
  - Windows: Download from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [OpenJDK](https://adoptium.net/)

## 📦 Installation

### From PyPI (Recommended)

```bash
pip install pyspark-analyzer
```

### From Source

```bash
git clone https://github.com/bjornvandijkman1993/pyspark-analyzer.git
cd pyspark-analyzer
pip install -e .
```

### Using uv (for development)

```bash
git clone https://github.com/bjornvandijkman1993/pyspark-analyzer.git
cd pyspark-analyzer
uv sync
```

### Basic Usage

```python
from pyspark.sql import SparkSession
from pyspark_analyzer import analyze

# Create Spark session
spark = SparkSession.builder.appName("DataProfiling").getOrCreate()

# Load your DataFrame
df = spark.read.parquet("your_data.parquet")

# Profile the DataFrame (returns pandas DataFrame by default)
profile_df = analyze(df)

# View column statistics
print(profile_df)

# Get dictionary format for programmatic access
profile_dict = analyze(df, output_format="dict")
print(f"Total Rows: {profile_dict['overview']['total_rows']:,}")
print(f"Total Columns: {profile_dict['overview']['total_columns']}")

# Get human-readable summary
summary = analyze(df, output_format="summary")
print(summary)
```

### Sampling Options

```python
from pyspark_analyzer import analyze

# Option 1: Disable sampling completely
profile = analyze(df, sampling=False)

# Option 2: Sample to specific number of rows
profile = analyze(df, target_rows=100_000, seed=42)

# Option 3: Sample by fraction
profile = analyze(df, fraction=0.1, seed=42)  # 10% sample

# Option 4: Auto-sampling (default behavior)
# Automatically samples large datasets (>10M rows)
profile = analyze(df)

# Check sampling information (with dict output)
profile_dict = analyze(df, output_format="dict")
sampling_info = profile_dict['sampling']
if sampling_info['is_sampled']:
    print(f"Sample size: {sampling_info['sample_size']:,} rows")
    print(f"Speedup: {sampling_info['estimated_speedup']:.1f}x")
```

### Advanced Features

```python
from pyspark_analyzer import analyze

# Include advanced statistics (skewness, kurtosis, percentiles)
profile = analyze(df, include_advanced=True)

# Include data quality analysis
profile = analyze(df, include_quality=True)

# Profile specific columns only
profile = analyze(df, columns=["age", "salary", "department"])

# Different output formats
profile_df = analyze(df)                    # Default: pandas DataFrame
profile_dict = analyze(df, output_format="dict")    # Dictionary format
summary = analyze(df, output_format="summary")      # Human-readable summary

# Save pandas output to various formats
profile_df = analyze(df)
profile_df.to_csv("profile.csv")
profile_df.to_parquet("profile.parquet")
profile_df.to_html("profile.html")
```


## 📊 Example Output

### Default: Pandas DataFrame
```python
profile_df = analyze(df)
print(profile_df)

# Output:
#                    column_name     data_type  null_count  null_percentage  distinct_count  ...
# 0                     user_id   IntegerType           0              0.0           50000  ...
# 1                         age   IntegerType         100              0.1             120  ...
# 2                       email    StringType           0              0.0           49950  ...

# Access metadata
print(profile_df.attrs['overview'])
# {'total_rows': 1000000, 'total_columns': 5, ...}
```

### Dictionary Format
```python
profile_dict = analyze(df, output_format="dict")

# Output:
{
    'overview': {
        'total_rows': 1000000,
        'total_columns': 5,
        'column_types': {...}
    },
    'sampling': {
        'is_sampled': True,
        'sample_size': 100000,
        'quality_score': 0.95,
        'estimated_speedup': 10.0
    },
    'columns': {
        'user_id': {
            'data_type': 'IntegerType()',
            'null_count': 0,
            'distinct_count': 50000,
            'min': 1,
            'max': 999999,
            'mean': 500000.0,
            ...
        }
    }
}
```

## 🏗️ Architecture

### Core Components

- **`analyze()`**: Simple, unified API for all profiling operations
- **`StatisticsComputer`**: Handles individual column statistics computation
- **`SamplingConfig`**: Simple, clear configuration for sampling behavior
- **`BatchStatisticsComputer`**: Optimized batch processing for large datasets

### Performance Optimizations

- **Intelligent Sampling**: Automatic sampling for datasets >10M rows
- **Quality Estimation**: Statistical quality scores for sampling accuracy
- **Batch Aggregations**: Minimize data scans with combined operations
- **Approximate Functions**: Fast distinct counts and percentile computations
- **Smart Caching**: Intelligent caching for multiple operations
- **Adaptive Partitioning**: Dynamic partition optimization based on:
  - Data size and row count estimation
  - Cluster configuration (parallelism, shuffle partitions)
  - Spark's Adaptive Query Execution (AQE) status
  - Avoids repartitioning overhead when not beneficial

## 📚 Examples

Check out the [examples](./examples/) directory for comprehensive usage examples:

- [`installation_verification.py`](./examples/installation_verification.py) - Verify your installation
- [`simple_api_example.py`](./examples/simple_api_example.py) - Quick start with the new `analyze()` API
- [`basic_usage.py`](./examples/basic_usage.py) - Traditional class-based usage demonstration
- [`sampling_example.py`](./examples/sampling_example.py) - All sampling configuration options
- [`pandas_output_example.py`](./examples/pandas_output_example.py) - Working with pandas DataFrame output
- [`advanced_statistics_demo.py`](./examples/advanced_statistics_demo.py) - Advanced features and data quality analysis

## 🧪 Development

### Setup with uv (Recommended)

```bash
# Install uv (ultra-fast Python package installer)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/bjornvandijkman1993/pyspark-analyzer.git
cd pyspark-analyzer

# Create virtual environment and install dependencies
uv sync --all-extras

# Verify installation
uv run python examples/installation_verification.py
```

### Traditional Setup

```bash
# Clone the repository
git clone https://github.com/bjornvandijkman1993/pyspark-analyzer.git
cd pyspark-analyzer

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e ".[dev]"

# Verify installation
python examples/installation_verification.py
```

### Testing

```bash
# Using Makefile (handles Java setup automatically)
make test                # Run all tests
make test-cov           # Run tests with coverage
make test-quick         # Run tests (stop on first failure)

# Using uv directly (requires .env file)
source .env && uv run pytest
source .env && uv run pytest --cov=pyspark_analyzer

# Traditional pytest
pytest
pytest --cov=pyspark_analyzer
pytest tests/test_sampling.py -v
```

### Code Quality

```bash
# Format code
uv run black pyspark_analyzer/ tests/ examples/
# or: black pyspark_analyzer/ tests/ examples/

# Lint code (using ruff)
uv run ruff check pyspark_analyzer/
# or: ruff check pyspark_analyzer/

# Type checking
uv run mypy pyspark_analyzer/
# or: mypy pyspark_analyzer/

# Security scanning
uv run bandit -c .bandit -r pyspark_analyzer/
# or: bandit -c .bandit -r pyspark_analyzer/

# Run all pre-commit hooks
pre-commit run --all-files
```

## 📋 Requirements

- **Python**: 3.8+
- **PySpark**: 3.0.0+ (native median function available in 3.4.0+, fallback for older versions)
- **Java**: 17+ (required by PySpark)

## 📚 Documentation

Comprehensive documentation is available at **[pyspark-analyzer.readthedocs.io](https://pyspark-analyzer.readthedocs.io)**

- **[Installation Guide](https://pyspark-analyzer.readthedocs.io/en/latest/installation.html)** - Detailed setup instructions
- **[Quick Start](https://pyspark-analyzer.readthedocs.io/en/latest/quickstart.html)** - Get up and running quickly
- **[User Guide](https://pyspark-analyzer.readthedocs.io/en/latest/user_guide.html)** - Advanced usage and best practices
- **[API Reference](https://pyspark-analyzer.readthedocs.io/en/latest/api_reference.html)** - Complete API documentation
- **[Examples](https://pyspark-analyzer.readthedocs.io/en/latest/examples.html)** - Real-world usage examples

## 🔒 Security

This project takes security seriously. We employ multiple layers of security scanning:

- **Dependency Scanning**: Safety and pip-audit check for known vulnerabilities
- **Static Analysis**: Bandit scans for security issues in code
- **Secret Detection**: detect-secrets prevents secrets from being committed
- **CodeQL**: GitHub's semantic code analysis for security vulnerabilities
- **Pre-commit Hooks**: Security checks before code is committed
- **Automated Updates**: Dependabot keeps dependencies up-to-date

For more information, see our [Security Policy](SECURITY.md).

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

All pull requests are automatically scanned for security issues.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [PySpark](https://spark.apache.org/docs/latest/api/python/) for distributed data processing
- Inspired by pandas-profiling for comprehensive data analysis
- Uses statistical sampling techniques for performance optimization

---

**⭐ Star this repo if you find it useful!**
