# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is "pyspark-analyzer" - a PySpark DataFrame profiler for generating detailed statistics and data quality reports with intelligent sampling capabilities.

## Claude-Specific Instructions

### When asked to run tests or lint/typecheck:
- Use `make test` for running tests (handles Java setup automatically)
- Use `uv run ruff check pyspark_analyzer/` for linting (not flake8)
- Use `uv run black pyspark_analyzer/ tests/ examples/` for formatting
- Use `uv run mypy pyspark_analyzer/` for type checking

### Important Java Environment Notes:
- Tests require Java 17+ which is handled by `./scripts/setup_test_environment.sh`
- If running tests directly with pytest, source the .env file first: `source .env && uv run pytest`
- The Makefile commands automatically handle the Java environment setup

### Key Architecture Points for Code Modifications:
- **`api.py`**: Contains the main `analyze()` function - the primary user interface
- **`profiler.py`**: Contains `DataFrameProfiler` class (internal implementation)
- **`sampling.py`**: Contains `SamplingConfig` - use this for sampling configuration
- **Use existing patterns**: Check neighboring files before adding new dependencies

### Release Process:
- This project uses semantic-release - versions are managed automatically
- Use conventional commits: `feat:`, `fix:`, `docs:`, `chore:`, etc.
- DO NOT manually bump versions in pyproject.toml
- See RELEASE_PROCESS.md for details

### Code Style Guidelines:
- DO NOT add comments unless explicitly requested
- Follow existing code patterns and conventions
- Use type hints consistently
- Keep imports organized (standard library, third-party, local)

### Common Tasks Reference:
```bash
# Add new dependency
uv add <package-name>

# Add dev dependency
uv add --dev <package-name>

# Run example scripts
uv run python examples/installation_verification.py

# Build package
uv run python -m build
```

## Important Reminders:
- Always check if a library is already used before importing it
- Never commit secrets or sensitive information
- Prefer editing existing files over creating new ones
- Only create documentation files when explicitly requested
