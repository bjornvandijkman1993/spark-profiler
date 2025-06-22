# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

# [4.5.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v4.4.0...v4.5.0) (2025-06-22)


### Bug Fixes

* clean up pycache files and add Jupyter notebook support ([5570b74](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/5570b74713431b38ba59186e7d0ac4d32f258653))


### Features

* add backward compatibility for PySpark versions 3.0.0+ ([845edde](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/845edde12b13130faaab28b2e4dc897f59317f17))

# [4.4.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v4.3.0...v4.4.0) (2025-06-22)


### Features

* optimize Java environment setup in conftest.py ([434d4b7](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/434d4b71166d442e1f286e8deca02a1206ca5b58))

# [4.3.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v4.2.1...v4.3.0) (2025-06-19)


### Features

* simplify codebase architecture and improve code maintainability ([b3c20dc](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/b3c20dcd16ebd1eba6bce53c622719c31a2a2e1e))

## [4.2.1](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v4.2.0...v4.2.1) (2025-06-19)


### Bug Fixes

* address pre-commit checks for type annotations and formatting ([7fb74ff](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/7fb74ffbc53909df6d1355b3032519e85ef309d4))

# [4.2.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v4.1.0...v4.2.0) (2025-06-19)


### Features

* implement comprehensive exception handling system ([69200f0](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/69200f082187d830f77d16930337cf0ba31e2346))

# [4.1.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v4.0.0...v4.1.0) (2025-06-19)


### Features

* add comprehensive logging infrastructure ([c8cd5d2](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/c8cd5d2087129913a7761084bfa84b8ede0cc949))

# [4.0.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v3.2.0...v4.0.0) (2025-06-19)


### Features

* remove optimize_for_large_datasets parameter and simplify API ([40c5e41](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/40c5e41a2de73d86afffa007f20139088b0a175c))


### BREAKING CHANGES

* The optimize_for_large_datasets parameter has been removed from the analyze() function and DataFrameProfiler class. Performance optimizations are now always applied automatically.

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>

# [3.2.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v3.1.0...v3.2.0) (2025-06-19)


### Features

* simplify and improve example scripts for better user experience ([7233b56](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/7233b56fec1a1bad1a97aa3333265f5cc3c6e8a1))

# [3.1.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v3.0.0...v3.1.0) (2025-06-19)


### Features

* **makefile:** enhance Makefile with comprehensive development commands ([37930d0](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/37930d0caf4b8a56818984e2a75d75e49c67dd99))

# [3.0.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v2.0.0...v3.0.0) (2025-06-19)


### Bug Fixes

* **tests:** add automatic Java environment setup for PyCharm ([7330c05](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/7330c0525512aec2122339bc6341614bb61c3a3e))


### Code Refactoring

* remove auto_threshold parameter from sampling configuration ([75fe4b3](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/75fe4b3f87dcc8a392645a7268373953b1206b9e))


### BREAKING CHANGES

* The auto_threshold parameter has been removed from the analyze() function and SamplingConfig. Auto-sampling now uses a fixed threshold of 10 million rows.

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>

# [2.0.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v1.0.0...v2.0.0) (2025-06-18)


### Features

* **api:** simplify library with single analyze() function ([ed1a8dc](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/ed1a8dc51c543f2a72dfdecc04826ce6bf6dd98a))


### BREAKING CHANGES

* **api:** Remove DataFrameProfiler from public API. Users should now use the simple analyze() function instead.

- Add new analyze() function as the sole public API
- Remove DataFrameProfiler, SamplingConfig from public exports
- Remove convenience methods (to_csv, to_parquet, etc) from DataFrameProfiler
- Update all examples to use the new simplified API
- Update tests to use internal imports

The new API is much simpler:
```python
from pyspark_analyzer import analyze

# Basic usage
profile = analyze(df)

# Control sampling
profile = analyze(df, sampling=False)
profile = analyze(df, target_rows=100_000)
profile = analyze(df, fraction=0.1)
```

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>

# [1.0.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.3.3...v1.0.0) (2025-06-18)


### Bug Fixes

* **sampling:** simplify sampling configuration and remove quality estimation ([1a22656](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/1a22656f73ad2018824ce04d39dc89a9ab600c8e))


### BREAKING CHANGES

* **sampling:** Removed quality_score and strategy_used from sampling metadata.
The SamplingConfig API has been simplified but remains backward compatible.

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>

## [0.3.3](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.3.2...v0.3.3) (2025-06-18)


### Bug Fixes

* **ci:** replace semantic-release-pypi with manual PyPI publishing ([33e8fae](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/33e8faedb1c92b250da0082e87b82ba529c8c514))

## [0.3.2](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.3.1...v0.3.2) (2025-06-18)


### Bug Fixes

* **ci:** add build step to semantic-release prepare phase ([7687201](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/76872019569ceab232558420ca93cf5b025d28e4))
* **ci:** restore Python setup and use uv for package building ([3b37ddd](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/3b37ddda3a07864d7671b464caf8474af1f9667b))

## [0.3.1](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.3.0...v0.3.1) (2025-06-18)


### Bug Fixes

* **ci:** add missing semantic-release execution step ([4fe1276](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/4fe1276b80565bc8672ac5ffc6d84c4606177207))
* **ci:** use correct environment variable name for PyPI token ([7a83704](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/7a83704ecbd2ede23955d0e469b2f29cf566a56e))
* **profiler:** improve docstring clarity in DataFrameProfiler class ([0d14f01](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/0d14f017b254e464b4cbb0d09b19a2c7697ba6c9))

# [0.3.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.2.1...v0.3.0) (2025-06-18)


### Bug Fixes

* semantic release ([0dd3fc3](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/0dd3fc3f871d671bfe184ec166ea45785ae0129c))


### Features

* **ci:** add conventional-pre-commit hook ([1c76974](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/1c7697482e739ed322ddbc4921f9c0898679e512))

## [0.2.1](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.2.0...v0.2.1) (2025-06-17)


### Bug Fixes

* correct tool version configurations after semantic-release ([36db97d](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/36db97d45fe2535fead761c9d2584017eb415a9e))
* correct tool version configurations after semantic-release ([eb1161f](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/eb1161f6bb997ea94d714e63ee54c9d20cc091fd))

## [0.2.0](https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.6...v0.2.0) (2025-06-17)
### Bug Fixes
* replace PyPI badge with shields.io for better reliability ([41f0645](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/41f0645dbcb6277814fbcd7ebc3765c90957d7dd))
* update Codecov configuration to match official documentation ([b1f6552](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/b1f6552cfca5b2fc288b279150e81cc85a12e42e))

### Features

* add official Python 3.13 support ([5fa4074](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/5fa4074abf159110ed4e8f2c9823b922af30185b))
* modernize release process with semantic-release automation ([0c018ce](https://github.com/bjornvandijkman1993/pyspark-analyzer/commit/0c018ce12345678901234567890123456789abcd))

### BREAKING CHANGES

* Manual version bumping workflows have been removed in favor of automated semantic-release

## [Unreleased]

## [0.1.6] - 2025-01-17

### Added

* Intelligent sampling with quality monitoring for datasets over 10M rows
* Statistical quality scores and confidence reporting for sampling accuracy
* Performance monitoring with sampling time and estimated speedup tracking
* SamplingConfig class for flexible sampling configuration
* Automatic sampling thresholds based on dataset size

### Changed

* Enhanced performance with configurable sampling strategies
* Improved sampling with reproducible random sampling using seed control

## [0.1.5] - 2025-01-17

### Fixed

* Critical bug fixes for division by zero errors in statistical computations
* Empty DataFrame handling to prevent runtime errors
* Performance issues with large datasets
* Corrected --of flag for cyclonedx-py output format in SBOM generation

## [0.1.4] - 2025-01-16

### Added

* Path filters to CI workflows to prevent duplicate runs

### Changed

* Updated dependencies and removed downloads badge
* Minor code cleanup and improvements

## [0.1.3] - 2025-01-16

### Fixed

* SBOM generation command in CI workflow

## [0.1.2] - 2025-01-16

### Added

* Enhanced security scanning with comprehensive security measures
* Automated version management with bump2version
* Cyclonedx-bom for SBOM generation

### Changed

* Renamed package from spark-profiler to pyspark-analyzer
* Replaced flake8 with ruff for linting
* Optimized CI/CD workflows to reduce duplication

### Fixed

* Version mismatch issues
* Minimum sample size in examples
* Data type issues in sampling example
* Black formatting and pre-commit configuration
* Examples to use dict output format

## [0.1.1] - 2025-01-16

### Added

* Pandas DataFrame output format as default for better data analysis
* Advanced statistics including skewness, kurtosis, and outlier detection
* Intelligent sampling with quality metrics for large datasets
* Comprehensive documentation and examples
* CI/CD pipeline with automated testing
* Pre-commit hooks including markdown linting

### Changed

* Default output format changed from dictionary to pandas DataFrame
* Improved performance optimizations for large datasets

### Fixed

* Installation verification script for pandas output format
* Markdown linting issues in documentation

## [0.1.0] - 2025-01-16

### Added

* Initial release of pyspark-analyzer
* Basic statistics computation (count, nulls, data types)
* Numeric statistics (min, max, mean, std, median, quartiles)
* String statistics (length metrics, empty counts)
* Temporal statistics (date ranges)
* Performance optimizations for large DataFrames
* Sampling capabilities with configurable options
* Multiple output formats (dict, JSON, summary, pandas)
* Comprehensive test suite
* Example scripts and documentation

[Unreleased]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.2.0...HEAD
[0.1.6]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/bjornvandijkman1993/pyspark-analyzer/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/bjornvandijkman1993/pyspark-analyzer/releases/tag/v0.1.0
