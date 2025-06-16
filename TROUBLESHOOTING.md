# Troubleshooting Guide

## Java Gateway Error

If you encounter a Java gateway error when running tests, follow these steps:

### 1. Check Java Installation

```bash
# Check if Java is installed
java -version

# Should output something like:
# openjdk version "17.0.x" or "11.0.x" or "1.8.0_xxx"
```

If Java is not installed:
- **macOS**: `brew install openjdk@17` (or @11)
- **Ubuntu/Debian**: `sudo apt-get install openjdk-17-jdk`
- **Windows**: Download from [Adoptium](https://adoptium.net/)

### 2. Set Up Environment

Run the setup script:
```bash
# Make the script executable (first time only)
chmod +x scripts/setup_test_environment.sh

# Run the setup script
./scripts/setup_test_environment.sh

# Load environment variables
source .env
```

### 3. Common Issues and Solutions

#### Issue: "Could not find or load main class"

**Solution**: Set JAVA_HOME properly

```bash
# macOS
export JAVA_HOME=$(/usr/libexec/java_home)

# Linux
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64  # Adjust path as needed

# Windows
set JAVA_HOME=C:\Program Files\Java\jdk-17  # Adjust path as needed
```

#### Issue: "Connection refused" or "Cannot connect to Java gateway"

**Solution**: Set Spark local IP

```bash
export SPARK_LOCAL_IP=127.0.0.1
```

#### Issue: Tests fail with PySpark errors

**Solution**: Ensure Python and PySpark versions are compatible

```bash
# Check Python version
python --version

# Install compatible PySpark
uv add "pyspark>=3.0.0,<4.0.0"
```

### 4. Running Tests with Proper Environment

```bash
# Option 1: Using environment variables
export $(cat .env | xargs) && uv run pytest

# Option 2: Running specific test file
export $(cat .env | xargs) && uv run pytest tests/test_profiler.py -v

# Option 3: With coverage
export $(cat .env | xargs) && uv run pytest --cov=pyspark_analyzer
```

### 5. Debugging PySpark Issues

If tests still fail, enable PySpark debug logging:

```bash
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)
export SPARK_CONF_DIR=/tmp
export SPARK_LOG_LEVEL=DEBUG

uv run pytest tests/test_profiler.py -v -s
```

### 6. Verify Installation

Run the installation verification script:

```bash
uv run python examples/installation_verification.py
```

This will check:

- Java installation and version
- PySpark installation
- Basic Spark functionality

### 7. Clean Environment

If all else fails, try a clean environment:

```bash
# Remove existing virtual environment
rm -rf .venv

# Create new environment
uv sync --all-extras

# Run setup again
./scripts/setup_test_environment.sh
source .env

# Try tests again
uv run pytest
```

## Other Common Issues

### ImportError: No module named 'pyspark_analyzer'

Make sure the package is installed in development mode:

```bash
uv sync
```

### Type checking errors with mypy

PySpark types are not fully supported by mypy. This is expected and handled in the configuration.

### Performance test timeouts

Large dataset tests may take longer. Increase timeout if needed:

```bash
uv run pytest tests/test_performance.py -v --timeout=300
```
