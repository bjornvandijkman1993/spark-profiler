# [0.2.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.6...v0.2.0) (2025-06-17)


### Bug Fixes

* replace PyPI badge with shields.io for better reliability ([41f0645](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/41f0645dbcb6277814fbcd7ebc3765c90957d7dd))
* update Codecov configuration to match official documentation ([b1f6552](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/b1f6552cfca5b2fc288b279150e81cc85a12e42e))


### Features

* add official Python 3.13 support ([5fa4074](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/5fa4074abf159110ed4e8f2c9823b922af30185b))

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.6] - 2025-01-17

### Added
- Intelligent sampling with quality monitoring for datasets over 10M rows
- Statistical quality scores and confidence reporting for sampling accuracy
- Performance monitoring with sampling time and estimated speedup tracking
- SamplingConfig class for flexible sampling configuration
- Automatic sampling thresholds based on dataset size

### Changed
- Enhanced performance with configurable sampling strategies
- Improved sampling with reproducible random sampling using seed control

## [0.1.5] - 2025-01-17

### Fixed
- Critical bug fixes for division by zero errors in statistical computations
- Empty DataFrame handling to prevent runtime errors
- Performance issues with large datasets
- Corrected --of flag for cyclonedx-py output format in SBOM generation

## [0.1.4] - 2025-01-16

### Added
- Path filters to CI workflows to prevent duplicate runs

### Changed
- Updated dependencies and removed downloads badge
- Minor code cleanup and improvements

## [0.1.3] - 2025-01-16

### Fixed
- SBOM generation command in CI workflow

## [0.1.2] - 2025-01-16

### Added
- Enhanced security scanning with comprehensive security measures
- Automated version management with bump2version
- Cyclonedx-bom for SBOM generation

### Changed
- Renamed package from spark-profiler to pyspark-analyzer
- Replaced flake8 with ruff for linting
- Optimized CI/CD workflows to reduce duplication

### Fixed
- Version mismatch issues
- Minimum sample size in examples
- Data type issues in sampling example
- Black formatting and pre-commit configuration
- Examples to use dict output format

## [0.1.1] - 2025-01-16

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

[Unreleased]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.6...HEAD
[0.1.6]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/bjornvandijkman1993/pyspark-analyzer/releases/tag/v0.1.0
