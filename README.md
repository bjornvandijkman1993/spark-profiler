# 🔍 PySpark DataFrame Profiler

[![CI](https://github.com/bjornvandijkman1993/pyspark-analyzer/workflows/CI/badge.svg)](https://github.com/bjornvandijkman1993/pyspark-analyzer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/bjornvandijkman1993/pyspark-analyzer/branch/main/graph/badge.svg)](https://codecov.io/gh/bjornvandijkman1993/pyspark-analyzer)
[![Documentation Status](https://readthedocs.org/projects/pyspark-analyzer/badge/?version=latest)](https://pyspark-analyzer.readthedocs.io/en/latest/?badge=latest)
[![Security](https://github.com/bjornvandijkman1993/pyspark-analyzer/workflows/CodeQL/badge.svg)](https://github.com/bjornvandijkman1993/pyspark-analyzer/security)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://badge.fury.io/py/pyspark-analyzer.svg)](https://pypi.org/project/pyspark-analyzer/)
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

## 🚀 Quick Start

### Prerequisites

- Python >=3.8
- Java 8 or 11 (required for PySpark)
  - macOS: `brew install openjdk@11`
  - Ubuntu/Debian: `sudo apt-get install openjdk-11-jdk`
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
from pyspark_analyzer import DataFrameProfiler

# Create Spark session
spark = SparkSession.builder.appName("DataProfiling").getOrCreate()

# Load your DataFrame
df = spark.read.parquet("your_data.parquet")

# Profile the DataFrame
profiler = DataFrameProfiler(df)
profile = profiler.profile()

# View results
print(f"Total Rows: {profile['overview']['total_rows']:,}")
print(f"Total Columns: {profile['overview']['total_columns']}")

# Check sampling information
sampling_info = profile['sampling']
if sampling_info['is_sampled']:
    print(f"Sample Quality: {sampling_info['quality_score']:.3f}")
    print(f"Speedup: {sampling_info['estimated_speedup']:.1f}x")
```

### Advanced Sampling

```python
from pyspark_analyzer import DataFrameProfiler, SamplingConfig

# Custom sampling configuration
config = SamplingConfig(
    target_size=100_000,  # Sample to 100k rows
    seed=42,              # Reproducible sampling
    auto_sample=True      # Enable intelligent sampling
)

profiler = DataFrameProfiler(df, sampling_config=config)
profile = profiler.profile()
```

## 📊 Example Output

```python
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
        },
        ...
    }
}
```

## 🏗️ Architecture

### Core Components

- **`DataFrameProfiler`**: Main interface for profiling operations
- **`StatisticsComputer`**: Handles individual column statistics computation
- **`SamplingConfig`**: Flexible configuration with validation
- **`RandomSamplingStrategy`**: Reproducible random sampling with quality estimation
- **`BatchStatisticsComputer`**: Optimized batch processing for large datasets

### Performance Optimizations

- **Intelligent Sampling**: Automatic sampling for datasets >10M rows
- **Quality Estimation**: Statistical quality scores for sampling accuracy
- **Batch Aggregations**: Minimize data scans with combined operations
- **Approximate Functions**: Fast distinct counts and percentile computations
- **Smart Caching**: Intelligent caching for multiple operations

## 📚 Examples

Check out the [examples](./examples/) directory for comprehensive usage examples:

- [`installation_verification.py`](./examples/installation_verification.py) - Verify your installation
- [`basic_usage.py`](./examples/basic_usage.py) - Complete usage demonstration
- [`sampling_example.py`](./examples/sampling_example.py) - Advanced sampling features

## 🧪 Development

### Setup

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
# Run tests
pytest

# Run tests with coverage
pytest --cov=pyspark_analyzer

# Run specific test file
pytest tests/test_sampling.py -v
```

### Code Quality

```bash
# Format code
black pyspark_analyzer/ tests/ examples/

# Lint code
flake8 pyspark_analyzer/

# Type checking
mypy pyspark_analyzer/
```

## 📋 Requirements

- **Python**: 3.8+
- **PySpark**: 3.0.0+
- **Java**: 11+ (required by PySpark)

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
