# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Pandas DataFrame output format as default for better data analysis
- Advanced statistics including skewness, kurtosis, and outlier detection
- Intelligent sampling with quality metrics for large datasets
- Comprehensive documentation and examples
- CI/CD pipeline with automated testing
- Pre-commit hooks including markdown linting

### Changed
- Default output format changed from dictionary to pandas DataFrame
- Improved performance optimizations for large datasets

### Fixed
- Installation verification script for pandas output format
- Markdown linting issues in documentation

## [0.1.0] - 2025-01-16

### Added
- Initial release of pyspark-analyzer
- Basic statistics computation (count, nulls, data types)
- Numeric statistics (min, max, mean, std, median, quartiles)
- String statistics (length metrics, empty counts)
- Temporal statistics (date ranges)
- Performance optimizations for large DataFrames
- Sampling capabilities with configurable options
- Multiple output formats (dict, JSON, summary, pandas)
- Comprehensive test suite
- Example scripts and documentation

[Unreleased]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bjornvandijkman1993/pyspark-analyzer/releases/tag/v0.1.0
