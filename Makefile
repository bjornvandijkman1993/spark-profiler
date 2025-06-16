.PHONY: help install install-dev test test-cov lint format typecheck security clean build docs

# Default target
help:
	@echo "Available commands:"
	@echo "  make install      Install package in production mode"
	@echo "  make install-dev  Install package with all development dependencies"
	@echo "  make test         Run all tests"
	@echo "  make test-cov     Run tests with coverage report"
	@echo "  make test-quick   Run tests quickly (stop on first failure)"
	@echo "  make test-integration  Run integration tests only"
	@echo "  make lint         Run linting checks (flake8)"
	@echo "  make format       Format code with black and isort"
	@echo "  make typecheck    Run type checking with mypy"
	@echo "  make security     Run security checks with bandit"
	@echo "  make clean        Clean build artifacts and cache files"
	@echo "  make build        Build distribution packages"
	@echo "  make docs         Generate documentation"
	@echo "  make all          Run all checks (lint, typecheck, security, test)"

# Installation targets
install:
	uv sync

install-dev:
	uv sync --all-extras

# Testing targets
test:
	@if [ -f .env ]; then \
		echo "Loading existing Java environment..."; \
		set -a && . ./.env && set +a && \
		echo "JAVA_HOME: $$JAVA_HOME" && \
		uv run pytest; \
	else \
		echo "Setting up test environment..."; \
		./scripts/setup_test_environment.sh && \
		set -a && . ./.env && set +a && \
		echo "JAVA_HOME: $$JAVA_HOME" && \
		uv run pytest; \
	fi

test-cov:
	@if [ -f .env ]; then \
		echo "Loading existing Java environment..."; \
		set -a && . ./.env && set +a && \
		echo "JAVA_HOME: $$JAVA_HOME" && \
		uv run pytest --cov=pyspark_analyzer --cov-report=term-missing --cov-report=html; \
	else \
		echo "Setting up test environment..."; \
		./scripts/setup_test_environment.sh && \
		set -a && . ./.env && set +a && \
		echo "JAVA_HOME: $$JAVA_HOME" && \
		uv run pytest --cov=pyspark_analyzer --cov-report=term-missing --cov-report=html; \
	fi

test-integration:
	@if [ -f .env ]; then \
		echo "Loading existing Java environment..."; \
		set -a && . ./.env && set +a && \
		echo "JAVA_HOME: $$JAVA_HOME" && \
		uv run pytest tests/test_integration.py -v; \
	else \
		echo "Setting up test environment..."; \
		./scripts/setup_test_environment.sh && \
		set -a && . ./.env && set +a && \
		echo "JAVA_HOME: $$JAVA_HOME" && \
		uv run pytest tests/test_integration.py -v; \
	fi

test-quick:
	@if [ -f .env ]; then \
		echo "Loading existing Java environment..."; \
		set -a && . ./.env && set +a && \
		echo "JAVA_HOME: $$JAVA_HOME" && \
		uv run pytest -x --tb=short; \
	else \
		echo "Setting up test environment..."; \
		./scripts/setup_test_environment.sh && \
		set -a && . ./.env && set +a && \
		echo "JAVA_HOME: $$JAVA_HOME" && \
		uv run pytest -x --tb=short; \
	fi

# Code quality targets
lint:
	uv run flake8 pyspark_analyzer/ tests/

format:
	uv run black pyspark_analyzer/ tests/ examples/
	uv run isort pyspark_analyzer/ tests/ examples/

typecheck:
	uv run mypy pyspark_analyzer/

security:
	uv run bandit -r pyspark_analyzer/ -ll

# Build targets
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	uv run python -m build

# Documentation targets
docs:
	@echo "Documentation generation not yet configured"
	@echo "TODO: Add Sphinx or mkdocs configuration"

# Combined targets
all: lint typecheck security test

# Development workflow shortcuts
check: format lint typecheck security

dev: install-dev
	@echo "Development environment ready!"
	@echo "Run 'source .venv/bin/activate' to activate the virtual environment"
