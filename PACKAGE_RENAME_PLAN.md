# Package Rename Plan: spark-profiler → pyspark-analyzer

This document outlines all the changes required to rename the package from `spark-profiler` to `pyspark-analyzer`.

## Overview

The package needs to be renamed due to name similarity concerns. This involves updating:
- Package name (hyphenated): `spark-profiler` → `pyspark-analyzer`
- Module name (underscored): `spark_profiler` → `pyspark_analyzer`

## Required Changes

### 1. Core Package Structure

- [ ] Rename directory: `spark_profiler/` → `pyspark_analyzer/`
- [ ] Update `__init__.py` files if they contain package references

### 2. Configuration Files

#### pyproject.toml
- [ ] Update `name = "spark-profiler"` → `name = "pyspark-analyzer"`
- [ ] Update any references in dependencies or scripts

#### Other Config Files
- [ ] Check and update `setup.cfg` (if exists)
- [ ] Update `Makefile` commands
- [ ] Regenerate `uv.lock` after changes

### 3. Import Statements

Update all Python files (39 files identified) that import the package:

#### Test Files (`tests/`)
- [ ] `test_advanced_statistics.py`
- [ ] `test_integration.py`
- [ ] `test_pandas_output.py`
- [ ] `test_performance.py`
- [ ] `test_profiler.py`
- [ ] `test_sampling.py`
- [ ] `test_statistics.py`
- [ ] `test_utils.py`
- [ ] `conftest.py`
- [ ] `__init__.py`

#### Example Scripts (`examples/`)
- [ ] `advanced_statistics_demo.py`
- [ ] `basic_usage.py`
- [ ] `installation_verification.py`
- [ ] `pandas_output_example.py`
- [ ] `sampling_example.py`

#### Scripts (`scripts/`)
- [ ] `verify_fixes.py`
- [ ] `setup-dev.sh`
- [ ] `bump_version.py`
- [ ] Other shell scripts with package references

#### Root Directory
- [ ] `verify_pandas_output.py`

### 4. Documentation Updates

#### Main Documentation (39 files identified)
- [ ] `README.md`
- [ ] `CLAUDE.md`
- [ ] `PACKAGING_PLAN.md`
- [ ] `PACKAGING_SUMMARY.md`
- [ ] `RELEASING.md`
- [ ] `CHANGELOG.md`
- [ ] `PYPI_SETUP.md`
- [ ] `ADVANCED_STATISTICS.md`
- [ ] `PANDAS_OUTPUT_DESIGN.md`
- [ ] `TROUBLESHOOTING.md`
- [ ] `CI_CD_IMPROVEMENTS.md`
- [ ] `SPARK_SETUP_FIXES.md`

#### Sphinx Documentation (`docs/source/`)
- [ ] `conf.py`
- [ ] `index.rst`
- [ ] `api_reference.rst`
- [ ] `installation.md`
- [ ] `quickstart.md`
- [ ] `user_guide.md`
- [ ] `examples.md`
- [ ] `contributing.md`
- [ ] `changelog.md`

#### Other Documentation
- [ ] `docs/README.md`
- [ ] `docs/version-management.md`
- [ ] `LICENSE` (if it contains package name)

### 5. GitHub Actions Workflows

Update all workflow files in `.github/workflows/`:
- [ ] `ci.yml`
- [ ] `release.yml`
- [ ] `docs.yml`
- [ ] `dependency-update.yml`
- [ ] `auto-format.yml`
- [ ] `version-bump.yml`

### 6. Scripts and Tools

- [ ] `scripts/publish_to_testpypi.sh`
- [ ] `scripts/test_package_build.sh`
- [ ] `scripts/setup_test_environment.sh`
- [ ] `scripts/run_tests.sh`
- [ ] `pandas-output-feature.patch`

### 7. Clean Up

- [ ] Remove all `__pycache__` directories
- [ ] Clear any build artifacts (`dist/`, `build/`, `*.egg-info`)
- [ ] Update `.gitignore` if needed

## Execution Order

1. **Preparation**
   - Create a new branch for the rename
   - Ensure all tests pass before starting

2. **Core Changes**
   - Update `pyproject.toml`
   - Rename the package directory
   - Update all import statements

3. **Documentation**
   - Update all markdown files
   - Update Sphinx documentation

4. **Infrastructure**
   - Update GitHub Actions
   - Update scripts

5. **Verification**
   - Run all tests
   - Build the package
   - Test installation
   - Verify documentation builds

## Testing Checklist

After completing the rename:

- [ ] `uv sync` completes successfully
- [ ] `uv run pytest` passes all tests
- [ ] `uv run mypy pyspark_analyzer/` passes
- [ ] `uv run ruff check pyspark_analyzer/` passes
- [ ] Documentation builds: `cd docs && make html`
- [ ] Package builds: `uv run python -m build`
- [ ] Examples run successfully

## Important Notes

1. **Git History**: The rename will be tracked in git history. Consider using `git mv` for the directory rename to preserve history.

2. **PyPI**: If the package is already published to PyPI, you'll need to:
   - Register the new package name
   - Update any existing users about the name change
   - Consider maintaining the old package with a deprecation notice

3. **Dependencies**: Any external projects depending on `spark-profiler` will need to update their dependencies.

4. **URLs**: Update any documentation URLs, repository references, or badges that might include the old name.
