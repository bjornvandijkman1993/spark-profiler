# Release Process

This document describes the process for releasing new versions of spark-profiler.

## Before You Begin

Ensure you have:
- Maintainer access to the GitHub repository
- PyPI API token configured as GitHub Secret (`PYPI_API_TOKEN`)
- All tests passing on main branch
- No uncommitted changes

## Release Steps

### 1. Prepare the Release

```bash
# Ensure you're on main branch with latest changes
git checkout main
git pull origin main

# Run tests to ensure everything works
uv run pytest
uv run mypy spark_profiler/
```

### 2. Update Version

Edit `pyproject.toml` and update the version number:
```toml
version = "X.Y.Z"  # Follow semantic versioning
```

### 3. Update CHANGELOG

Edit `CHANGELOG.md`:
1. Change "Unreleased" to the new version and date
2. Add a new "Unreleased" section at the top
3. Update comparison links at the bottom

Example:
```markdown
## [Unreleased]

## [0.2.0] - 2025-01-20

### Added
- New feature descriptions...
```

### 4. Commit Changes

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "Release version X.Y.Z"
git push origin main
```

### 5. Create and Push Tag

```bash
# Create annotated tag
git tag -a vX.Y.Z -m "Release version X.Y.Z"

# Push tag to GitHub
git push origin vX.Y.Z
```

### 6. Monitor Release

1. Go to [GitHub Actions](https://github.com/bjornvandijkman1993/spark-profiler/actions)
2. Watch the "Release" workflow
3. Ensure all steps complete successfully

### 7. Verify Release

1. Check [PyPI page](https://pypi.org/project/spark-profiler/)
2. Check [GitHub Releases](https://github.com/bjornvandijkman1993/spark-profiler/releases)
3. Test installation:
   ```bash
   pip install spark-profiler==X.Y.Z
   ```

## Release Types

### Patch Release (X.Y.Z+1)
- Bug fixes
- Documentation updates
- Dependency updates (non-breaking)

### Minor Release (X.Y+1.0)
- New features
- Performance improvements
- Backward-compatible changes

### Major Release (X+1.0.0)
- Breaking API changes
- Major refactoring
- Incompatible changes

## Emergency Hotfix Process

For critical bugs:

1. Create hotfix branch from tag:
   ```bash
   git checkout -b hotfix/X.Y.Z+1 vX.Y.Z
   ```

2. Apply fix and test thoroughly

3. Update version and CHANGELOG

4. Merge to main and tag:
   ```bash
   git checkout main
   git merge hotfix/X.Y.Z+1
   git tag -a vX.Y.Z+1 -m "Hotfix version X.Y.Z+1"
   git push origin main --tags
   ```

## Troubleshooting

### Release Workflow Fails

1. Check GitHub Actions logs
2. Common issues:
   - API token expired or incorrect
   - Network issues
   - PyPI service downtime

### Package Not Appearing on PyPI

1. Wait a few minutes (indexing delay)
2. Check if version already exists
3. Verify workflow completed successfully

### Installation Issues

1. Clear pip cache: `pip cache purge`
2. Try with `--no-cache-dir` flag
3. Check for dependency conflicts

## Post-Release Checklist

- [ ] Announce release (if major version)
- [ ] Update documentation if needed
- [ ] Monitor issue tracker for problems
- [ ] Plan next release features
