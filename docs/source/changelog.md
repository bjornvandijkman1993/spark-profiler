# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive documentation with Sphinx
- API reference documentation
- User guide with advanced examples
- Contributing guidelines

## [0.1.0] - 2024-01-XX

### Added
- Initial release of pyspark-analyzer
- Core `DataFrameProfiler` class for PySpark DataFrame analysis
- Automatic computation of data type-specific statistics
- Intelligent sampling for large datasets with `SamplingConfig`
- Performance optimizations including batch processing
- Multiple output formats (dict, JSON, summary report)
- Comprehensive test suite with >90% coverage
- GitHub Actions CI/CD pipeline
- Pre-commit hooks for code quality

### Features
- **Basic Statistics**: count, null count, distinct count for all columns
- **Numeric Statistics**: min, max, mean, std, median, quartiles
- **String Statistics**: length statistics, empty string detection
- **Temporal Statistics**: date/time range analysis
- **Sampling**: Automatic and configurable sampling with quality metrics
- **Performance**: Optimized aggregations and smart caching

### Performance
- Automatic optimization for DataFrames with >10M rows
- Configurable sampling with quality thresholds
- Batch aggregation to minimize Spark actions
- Approximate algorithms for large-scale computations

## [0.0.1] - 2024-01-01

### Added
- Project initialization
- Basic project structure
- Initial README and LICENSE
