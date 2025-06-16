#!/bin/bash
# Script to run tests with proper environment setup

echo "🧪 Running pyspark-analyzer tests..."

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

# Check if .env exists, create it if not
if [ ! -f .env ]; then
    echo "📍 .env file not found. Running setup..."
    ./scripts/setup_test_environment.sh
fi

# Load environment variables
if [ -f .env ]; then
    echo "📍 Loading environment variables from .env..."
    export $(cat .env | grep -v '^#' | xargs)
fi

# Additional safety settings for tests
export SPARK_LOCAL_IP=127.0.0.1
export PYARROW_IGNORE_TIMEZONE=1
export SPARK_TESTING=1

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo "❌ Error: Java is not installed or not in PATH"
    echo "   Please install Java 8, 11, or 17 and try again"
    exit 1
fi

# Run tests based on arguments
if [ $# -eq 0 ]; then
    # No arguments - run all tests
    echo "📍 Running all tests..."
    uv run pytest tests/ -v
else
    # Pass through arguments to pytest
    echo "📍 Running tests with arguments: $@"
    uv run pytest "$@"
fi
