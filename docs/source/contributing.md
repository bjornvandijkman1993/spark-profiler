# Contributing to pyspark-analyzer

We welcome contributions to pyspark-analyzer! This guide will help you get started.

## Development Setup

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR_USERNAME/pyspark-analyzer.git
cd pyspark-analyzer
```

### 2. Install Development Dependencies

We use `uv` for fast dependency management:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install all dependencies including dev
uv sync --all-extras
```

### 3. Set Up Pre-commit Hooks

```bash
uv run pre-commit install
```

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

Follow these guidelines:
- Write clear, self-documenting code
- Add type hints to all functions
- Include docstrings (Google style)
- Write tests for new functionality

### 3. Run Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=pyspark_analyzer

# Run specific test file
uv run pytest tests/test_profiler.py
```

### 4. Check Code Quality

```bash
# Run all pre-commit hooks
uv run pre-commit run --all-files

# Or individually:
uv run black pyspark_analyzer/ tests/
uv run ruff pyspark_analyzer/ tests/
uv run mypy pyspark_analyzer/
```

### 5. Update Documentation

If your changes affect the API:

```bash
# Build docs locally
cd docs
uv run make html
# View at docs/build/html/index.html
```

## Code Style Guidelines

### Python Style

We follow PEP 8 with these modifications:
- Line length: 120 characters
- Use Black for formatting
- Use type hints everywhere

### Docstring Format

Use Google style docstrings:

```python
def compute_statistics(df: DataFrame, columns: List[str]) -> Dict[str, Any]:
    """
    Compute statistics for specified columns.

    Args:
        df: The input DataFrame
        columns: List of column names to analyze

    Returns:
        Dictionary mapping column names to their statistics

    Raises:
        ValueError: If columns don't exist in DataFrame

    Example:
        >>> stats = compute_statistics(df, ["age", "salary"])
        >>> print(stats["age"]["mean"])
    """
```

### Import Order

1. Standard library imports
2. Third-party imports
3. Local imports

Each group separated by a blank line.

## Testing Guidelines

### Test Structure

```python
class TestDataFrameProfiler:
    """Test cases for DataFrameProfiler class."""

    def test_basic_profiling(self, spark_session, sample_df):
        """Test basic profiling functionality."""
        # Arrange
        profiler = DataFrameProfiler(sample_df)

        # Act
        profile = profiler.profile()

        # Assert
        assert "overview" in profile
        assert profile["overview"]["row_count"] == 100
```

### Test Coverage

- Aim for >90% test coverage
- Test edge cases and error conditions
- Include integration tests for Spark functionality

## Submitting Pull Requests

### 1. Commit Messages

Follow conventional commits:

```
feat: add support for decimal type profiling
fix: handle null values in median calculation
docs: update installation instructions
test: add tests for sampling module
refactor: optimize batch processing logic
```

### 2. Pull Request Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Tests pass locally
- [ ] Added new tests
- [ ] Updated documentation

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex logic
```

### 3. Review Process

1. Submit PR against `main` branch
2. Ensure CI passes
3. Address review feedback
4. Squash commits if requested

## Adding New Features

### 1. Discuss First

For major features:
- Open an issue for discussion
- Get feedback on approach
- Consider backward compatibility

### 2. Feature Structure

```
pyspark_analyzer/
├── new_feature.py      # Core implementation
tests/
├── test_new_feature.py # Comprehensive tests
docs/source/
├── new_feature.md      # User documentation
examples/
├── new_feature_demo.py # Usage example
```

### 3. Performance Considerations

- Profile performance impact
- Add benchmarks for significant features
- Consider memory usage
- Test with large datasets

## Reporting Issues

### Bug Reports

Include:
- Spark version
- Python version
- Minimal reproducible example
- Error messages
- Expected vs actual behavior

### Feature Requests

Include:
- Use case description
- Proposed API
- Alternative solutions considered
- Potential impact

## Community

### Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Provide constructive feedback
- Focus on the issue, not the person

### Getting Help

- Check existing issues/PRs
- Read the documentation
- Ask in discussions
- Tag maintainers for urgent issues

## Release Process

Maintainers handle releases:

1. Update version in `pyproject.toml`
2. Update CHANGELOG.md
3. Create release tag
4. GitHub Actions publishes to PyPI

## License

By contributing, you agree that your contributions will be licensed under the same license as the project (Apache 2.0).
