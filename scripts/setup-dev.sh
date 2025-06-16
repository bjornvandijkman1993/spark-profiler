#!/usr/bin/env bash
# Development setup script for pyspark-analyzer

set -e

echo "🚀 Setting up pyspark-analyzer development environment..."

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
required_version="3.8"

if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)"; then
    echo "❌ Python 3.8 or higher is required. Found: $python_version"
    exit 1
fi

echo "✅ Python version: $python_version"

# Check Java installation
if ! command -v java &> /dev/null; then
    echo "⚠️  Java is not installed. PySpark requires Java 8 or 11."
    echo "   Please install Java before running tests:"
    echo "   - macOS: brew install openjdk@11"
    echo "   - Ubuntu/Debian: sudo apt-get install openjdk-11-jdk"
    echo "   - Windows: Download from https://adoptium.net/"
    echo ""
    echo "   After installation, you may need to set JAVA_HOME:"
    echo "   export JAVA_HOME=$(/usr/libexec/java_home -v 11)"
else
    java_version=$(java -version 2>&1 | head -n 1)
    echo "✅ Java found: $java_version"
fi

# Install uv if not already installed
if ! command -v uv &> /dev/null; then
    echo "📦 Installing uv package manager..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
fi

echo "✅ uv is installed"

# Create virtual environment and install dependencies
echo "📦 Installing dependencies..."
uv sync --all-extras

echo ""
echo "✨ Development environment setup complete!"
echo ""
echo "To activate the virtual environment:"
echo "  source .venv/bin/activate"
echo ""
echo "To run tests:"
echo "  uv run pytest"
echo ""
echo "To run tests with coverage:"
echo "  uv run pytest --cov=pyspark_analyzer"
