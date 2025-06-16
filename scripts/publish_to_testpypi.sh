#!/bin/bash
# Script to publish package to TestPyPI for testing

set -e  # Exit on error

echo "📦 Publishing to TestPyPI..."
echo ""
echo "Prerequisites:"
echo "1. Create account at https://test.pypi.org/account/register/"
echo "2. Generate API token at https://test.pypi.org/manage/account/token/"
echo "3. Save token securely"
echo ""
echo "Press Enter to continue or Ctrl+C to cancel..."
read

# Build fresh packages
echo "🧹 Cleaning previous builds..."
rm -rf dist/ build/ *.egg-info

echo "📦 Building package..."
uv build

echo "✅ Checking package with twine..."
uv run twine check dist/*

echo "📤 Uploading to TestPyPI..."
echo "You will be prompted for your TestPyPI username and password/token."
echo "Username: __token__"
echo "Password: <your-test-pypi-token>"
echo ""

uv run twine upload --repository testpypi dist/*

echo ""
echo "🎉 Package uploaded to TestPyPI!"
echo ""
echo "To test installation:"
echo "pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ pyspark-analyzer"
echo ""
echo "Note: --extra-index-url is needed to install dependencies from regular PyPI"
