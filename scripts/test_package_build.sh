#!/bin/bash
# Script to test package building and installation

set -e  # Exit on error

echo "🧹 Cleaning previous builds..."
rm -rf dist/ build/ *.egg-info

echo "📦 Building package..."
uv build

echo "📋 Checking package contents..."
echo "Contents of source distribution:"
tar -tzf dist/*.tar.gz | head -20
echo ""
echo "Contents of wheel:"
unzip -l dist/*.whl | head -20

echo "✅ Checking package with twine..."
uv run twine check dist/*

echo "🧪 Testing installation in isolated environment..."
# Create temporary directory
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

# Create virtual environment
python -m venv test-env
source test-env/bin/activate || source test-env/Scripts/activate

# Install the package
pip install "$OLDPWD"/dist/*.whl

# Test import
echo "Testing import..."
python -c "
from pyspark_analyzer import DataFrameProfiler
from pyspark_analyzer import SamplingConfig
print('✅ Import successful!')
print(f'Package location: {DataFrameProfiler.__module__}')
"

# Cleanup
deactivate
cd "$OLDPWD"
rm -rf "$TEMP_DIR"

echo "🎉 Package build and installation test completed successfully!"
