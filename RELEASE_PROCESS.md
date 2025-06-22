# Release Process

This document describes the automated release process using semantic-release.

## Overview

The project uses **semantic-release** to automate version bumping, changelog generation, and releases based on conventional commit messages.

## How It Works

1. **Commits to main branch** trigger the semantic-release workflow
2. **Semantic-release analyzes commit messages** to determine version bump type
3. **Automatic version bumping** based on commit types:
   - `fix:` → patch version (0.1.6 → 0.1.7)
   - `feat:` → minor version (0.1.6 → 0.2.0)
   - `BREAKING CHANGE:` or `feat!:` → major version (0.1.6 → 1.0.0)
4. **Automatic changelog generation** from commit messages
5. **Automatic GitHub release** with release notes
6. **Automatic PyPI publication** with package verification

## Commit Message Format

Use **conventional commits** format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types
- `feat:` - New feature (minor version bump)
- `fix:` - Bug fix (patch version bump)
- `docs:` - Documentation changes (no version bump)
- `style:` - Code style changes (no version bump)
- `refactor:` - Code refactoring (no version bump)
- `test:` - Test changes (no version bump)
- `chore:` - Build/maintenance changes (no version bump)
- `perf:` - Performance improvements (patch version bump)
- `ci:` - CI/CD changes (no version bump)

### Examples

```bash
# Patch release (0.1.6 → 0.1.7)
git commit -m "fix(profiler): resolve division by zero in sampling"

# Minor release (0.1.6 → 0.2.0)
git commit -m "feat(statistics): add support for array columns"

# Major release (0.1.6 → 1.0.0)
git commit -m "feat!: remove deprecated sample_fraction parameter

BREAKING CHANGE: The sample_fraction parameter has been removed. Use SamplingConfig instead."

# No release
git commit -m "docs: update README with new examples"
git commit -m "chore: update dependencies"
git commit -m "test: add more edge case tests"
```

## Manual Release

If you need to trigger a release manually:

1. Go to **Actions** → **Semantic Release**
2. Click **Run workflow**
3. Select the **main** branch
4. Click **Run workflow**

## What Gets Updated Automatically

- `pyproject.toml` version
- `pyspark_analyzer/__init__.py` version
- `CHANGELOG.md` with release notes
- Git tag (e.g., `v0.2.0`)
- GitHub release with notes
- PyPI package publication

## Troubleshooting

### No Release Triggered
- Check if your commit messages follow conventional format
- Only `feat:`, `fix:`, `perf:`, and breaking changes trigger releases
- Commits with `[skip ci]` are ignored

### Release Failed
- Check the GitHub Actions logs
- Ensure all tests pass
- Verify PyPI credentials are set up correctly

### Version Mismatch
- Semantic-release manages versions automatically
- Don't manually edit version numbers in files
- If needed, use semantic-release's `--dry-run` to preview changes
