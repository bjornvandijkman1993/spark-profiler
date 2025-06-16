# Version Management

This document describes how version management works in the pyspark-analyzer project.

## Current Setup

The project uses `bump2version` for automated version management. Version numbers are synchronized across:
- `pyproject.toml` - Package metadata
- `pyspark_analyzer/__init__.py` - Runtime version
- `.bumpversion.cfg` - Version management configuration

## Manual Version Bumping

### Using the Script (Recommended)
```bash
# Bump patch version (0.1.1 -> 0.1.2)
python scripts/bump_version.py patch

# Bump minor version (0.1.1 -> 0.2.0)
python scripts/bump_version.py minor

# Bump major version (0.1.1 -> 1.0.0)
python scripts/bump_version.py major
```

### Using bump2version Directly
```bash
# Make sure you have bump2version installed
uv sync --frozen

# Bump version
uv run bump2version patch  # or minor, major
```

## Automated Version Bumping

Use the GitHub Actions workflow:

1. Go to Actions → Version Bump
2. Click "Run workflow"
3. Select bump type (patch/minor/major)
4. The workflow will create a PR with the version changes

## Release Process

1. **Bump Version**: Use one of the methods above
2. **Push Changes**:
   ```bash
   git push && git push --tags
   ```
3. **Automatic Release**: The release workflow triggers on new tags
4. **Verification**: The workflow includes post-publish verification

## Version Numbering

We follow [Semantic Versioning](https://semver.org/):
- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality (backwards compatible)
- **PATCH**: Bug fixes (backwards compatible)

## Configuration

The `.bumpversion.cfg` file controls:
- Current version tracking
- Files to update
- Commit and tag behavior
- Tag naming format (v{version})

## Troubleshooting

### Version Mismatch
If versions get out of sync:
1. Check current git tag: `git describe --tags`
2. Update `.bumpversion.cfg` to match
3. Manually sync other files if needed

### Failed Release
If a release fails:
1. Check GitHub Actions logs
2. Verify PyPI token is valid
3. Ensure version wasn't already published
