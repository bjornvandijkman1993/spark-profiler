.PHONY: help install install-dev test test-cov test-quick test-integration test-unit test-watch test-parallel coverage-html \
        lint format format-check typecheck security security-scan secrets-scan clean clean-all build build-check dist-check \
        docs docs-build docs-serve docs-clean diagnose setup-java pre-commit-install pre-commit-run pre-commit-update \
        deps-check deps-update deps-licenses sbom examples example-basic example-verify example-progress version changelog \
        lint-all quality check dev all ci release-dry-run env reinstall

# Default target
help:
	@echo "Available commands:"
	@echo "  make install      Install package in production mode"
	@echo "  make install-dev  Install package with all development dependencies"
	@echo "  make test         Run all tests"
	@echo "  make test-cov     Run tests with coverage report"
	@echo "  make test-quick   Run tests quickly (stop on first failure)"
	@echo "  make test-integration  Run integration tests only"
	@echo "  make lint         Run linting checks (ruff)"
	@echo "  make format       Format code with black and isort"
	@echo "  make typecheck    Run type checking with mypy"
	@echo "  make security     Run security checks with bandit"
	@echo "  make clean        Clean build artifacts and cache files"
	@echo "  make build        Build distribution packages"
	@echo "  make docs-build   Build HTML documentation"
	@echo "  make docs-serve   Build and serve documentation locally"
	@echo "  make diagnose     Run Java diagnostic script"
	@echo "  make pre-commit-install  Install pre-commit hooks"
	@echo "  make deps-check   Check for outdated dependencies"
	@echo "  make version      Display current version"
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

test-unit:
	@if [ -f .env ]; then \
		echo "Loading existing Java environment..."; \
		set -a && . ./.env && set +a && \
		uv run pytest tests/ -v --ignore=tests/test_integration.py; \
	else \
		echo "Setting up test environment..."; \
		./scripts/setup_test_environment.sh && \
		set -a && . ./.env && set +a && \
		uv run pytest tests/ -v --ignore=tests/test_integration.py; \
	fi

test-watch:
	@if [ -f .env ]; then \
		echo "Loading existing Java environment..."; \
		set -a && . ./.env && set +a && \
		uv run pytest-watch; \
	else \
		echo "Setting up test environment..."; \
		./scripts/setup_test_environment.sh && \
		set -a && . ./.env && set +a && \
		uv run pytest-watch; \
	fi

test-parallel:
	@if [ -f .env ]; then \
		echo "Loading existing Java environment..."; \
		set -a && . ./.env && set +a && \
		uv run pytest -n auto; \
	else \
		echo "Setting up test environment..."; \
		./scripts/setup_test_environment.sh && \
		set -a && . ./.env && set +a && \
		uv run pytest -n auto; \
	fi

coverage-html: test-cov
	@echo "Opening coverage report in browser..."
	@python -m webbrowser htmlcov/index.html

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
	uv run ruff check pyspark_analyzer/

format:
	uv run black pyspark_analyzer/ tests/ examples/
	uv run isort pyspark_analyzer/ tests/ examples/

format-check:
	uv run black --check pyspark_analyzer/ tests/ examples/
	uv run isort --check-only pyspark_analyzer/ tests/ examples/

typecheck:
	uv run mypy pyspark_analyzer/

security:
	uv run bandit -r pyspark_analyzer/ -ll

security-scan: security
	uv run safety check || true
	uv run pip-audit || true

secrets-scan:
	uv run detect-secrets scan --baseline .secrets.baseline

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

clean-all: clean
	rm -rf .venv/
	rm -rf .env
	rm -rf .ruff_cache/
	rm -rf .hypothesis/
	rm -rf .tox/

build: clean
	uv run python -m build

build-check:
	./scripts/test_package_build.sh

dist-check: build
	uv run twine check dist/*

# Documentation targets
docs: docs-build

docs-build:
	cd docs && uv run make html

docs-serve: docs-build
	@echo "Serving documentation at http://localhost:8000"
	cd docs/_build/html && python -m http.server 8000

docs-clean:
	cd docs && uv run make clean


diagnose:
	./scripts/diagnose_java.sh

setup-java:
	./scripts/setup_test_environment.sh

# Pre-commit targets
pre-commit-install:
	uv run pre-commit install

pre-commit-run:
	uv run pre-commit run --all-files

pre-commit-update:
	uv run pre-commit autoupdate

# Dependency management
deps-check:
	uv pip list --outdated

deps-update:
	uv sync --upgrade

deps-licenses:
	uv run pip-licenses --with-authors --with-urls

sbom:
	uv run cyclonedx-py -p -o sbom.json

# Example targets
examples:
	uv run python examples/basic_usage.py
	uv run python examples/installation_verification.py

example-basic:
	uv run python examples/basic_usage.py

example-verify:
	uv run python examples/installation_verification.py

example-progress:
	uv run python examples/progress_bar_demo.py

# Version and changelog
version:
	@grep -E "^version" pyproject.toml | cut -d'"' -f2

changelog:
	@head -n 20 CHANGELOG.md

# Quality checks
lint-all: lint security typecheck

quality: format-check lint-all

# Combined targets
all: lint typecheck security test

check: format lint typecheck security

dev: install-dev
	@echo "Development environment ready!"
	@echo "Run 'source .venv/bin/activate' to activate the virtual environment"

# CI/CD helpers
ci: clean quality test-cov build-check

release-dry-run:
	@echo "Simulating release process..."
	uv run semantic-release version --no-commit --no-tag --no-push --no-changelog

# Utility targets
env:
	@echo "Current environment variables:"
	@echo "JAVA_HOME: $$JAVA_HOME"
	@echo "PYTHON: $$(which python)"
	@echo "UV: $$(which uv)"
	@echo "Virtual env: $$VIRTUAL_ENV"

reinstall: clean-all
	uv sync --all-extras
	./scripts/setup_test_environment.sh
