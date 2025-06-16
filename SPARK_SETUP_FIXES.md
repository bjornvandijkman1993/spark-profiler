# Spark and Java Setup Fixes

## Issue Summary
The test environment was experiencing `Py4JJavaError` failures due to:
1. Missing or misconfigured Java installation
2. Python version mismatch between PySpark driver and workers

## Root Causes Identified

### 1. Java Gateway Error
- Java was installed but environment variables were not properly configured
- Tests couldn't connect to the Java gateway process

### 2. Python Version Mismatch
- The `.env` file was pointing to system Python 3.13 (`/opt/homebrew/bin/python3`)
- The virtual environment was using Python 3.12 (`.venv/bin/python`)
- PySpark requires the driver and worker Python versions to match

## Solutions Applied

### 1. Fixed Python Path Configuration
Updated `.env` file to use the virtual environment's Python:
```bash
# Before (incorrect)
export PYSPARK_PYTHON="/opt/homebrew/bin/python3"
export PYSPARK_DRIVER_PYTHON="/opt/homebrew/bin/python3"

# After (correct)
export PYSPARK_PYTHON="/Users/bjornvandijkman/PycharmProjects/pyspark-analyzer/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="/Users/bjornvandijkman/PycharmProjects/pyspark-analyzer/.venv/bin/python"
```

### 2. Enhanced Setup Script
Updated `scripts/setup_test_environment.sh` to automatically detect the correct Python executable:
- First checks for active virtual environment (`$VIRTUAL_ENV`)
- Then checks for `.venv/bin/python`
- Falls back to system Python only if necessary

### 3. Verified Java Installation
- Java 17 is properly installed at `/opt/homebrew/opt/openjdk@17`
- `JAVA_HOME` is correctly set in `.env`

## Test Results
- **Before fixes**: 50 failed, 46 passed
- **After fixes**: 13 failed, 83 passed
- **Coverage**: 92%

## Remaining Test Failures
The remaining 13 test failures appear to be actual code issues rather than environment problems:
- Integration tests
- Performance tests
- Statistics computation tests

## Quick Setup Guide for Future Use

1. **Install Java (if not already installed)**:
   ```bash
   brew install openjdk@17
   ```

2. **Run the setup script**:
   ```bash
   ./scripts/setup_test_environment.sh
   ```

3. **Load environment variables**:
   ```bash
   source .env
   ```

4. **Run tests**:
   ```bash
   make test-cov
   ```

## Key Takeaways
- Always ensure PySpark driver and worker Python versions match
- Use virtual environment Python paths, not system Python
- The Makefile already handles environment loading correctly
- The diagnostic script (`scripts/diagnose_java.py`) is helpful for troubleshooting

## Environment Variables Required
```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export SPARK_LOCAL_IP="127.0.0.1"
export PYSPARK_PYTHON=".venv/bin/python"  # Use absolute path
export PYSPARK_DRIVER_PYTHON=".venv/bin/python"  # Use absolute path
```
