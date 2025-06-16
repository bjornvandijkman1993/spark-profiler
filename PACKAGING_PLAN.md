# Packaging and Publishing Plan for spark-profiler

## Overview

This document outlines the complete plan for packaging and publishing the spark-profiler package to PyPI (Python Package Index).

## Prerequisites

- [ ] PyPI account
- [ ] TestPyPI account (for testing)
- [ ] GitHub repository with proper permissions
- [ ] Local development environment set up

## Step-by-Step Plan

### 1. Package Metadata Updates (pyproject.toml)

**Status**: ✅ Completed

- [x] Add author email information
- [x] Add maintainer information
- [x] Add keywords for better discoverability
- [x] Verify classifiers are accurate
- [ ] Consider adding Python version badge

### 2. Version Management Strategy

**Status**: 🔄 In Progress

- [ ] Implement semantic versioning (MAJOR.MINOR.PATCH)
- [ ] Current version: 0.1.0 (ready for initial release)
- [ ] Set up version bumping tool (options):
  - `bump2version` - Simple version bumping
  - `hatch version` - Built into hatch
  - Manual updates with git tags

**Versioning Guidelines**:
- MAJOR: Breaking API changes
- MINOR: New features, backward compatible
- PATCH: Bug fixes, backward compatible

### 3. Build System Configuration

**Status**: ✅ Ready

- [x] Using Hatchling as build backend
- [x] Package discovery configured
- [ ] Create MANIFEST.in if needed for additional files
- [ ] Verify all necessary files are included:
  - Python source files
  - README.md
  - LICENSE
  - CHANGELOG.md

### 4. PyPI Account Setup

**Status**: ⏳ Pending

**Steps**:
1. Create account at https://pypi.org/account/register/
2. Verify email address
3. Enable 2FA (recommended)
4. Generate API token:
   - Go to Account Settings → API tokens
   - Create token with scope "Entire account" or project-specific
   - Save token securely

### 5. GitHub Secrets Configuration

**Status**: ⏳ Pending

**Required Secrets**:
- `PYPI_API_TOKEN`: For publishing to PyPI
- `TEST_PYPI_API_TOKEN`: For testing releases (optional)

**Setup**:
1. Go to GitHub repository → Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Add PYPI_API_TOKEN with the token from PyPI

### 6. Release Workflow Configuration

**Status**: ✅ Already Configured

The `.github/workflows/release.yml` is already set up with:
- Automated testing before release
- Package building
- PyPI publishing
- GitHub release creation

### 7. Documentation Updates

**Status**: 🔄 In Progress

- [x] Create CHANGELOG.md
- [ ] Update README.md with:
  - PyPI installation instructions
  - Version badge
  - Downloads badge
- [ ] Create RELEASING.md with release process
- [ ] Update documentation site with installation from PyPI

### 8. Testing Package Build

**Status**: ⏳ Pending

**Local Testing Steps**:
```bash
# Clean previous builds
rm -rf dist/ build/ *.egg-info

# Build package
uv build

# Check package contents
tar -tvf dist/*.tar.gz
unzip -l dist/*.whl

# Check package with twine
uv run twine check dist/*

# Test installation in new environment
python -m venv test-env
source test-env/bin/activate  # or test-env\Scripts\activate on Windows
pip install dist/*.whl
python -c "from spark_profiler import DataFrameProfiler; print('Success!')"
deactivate
rm -rf test-env
```

### 9. TestPyPI Release (Recommended)

**Status**: ⏳ Pending

**Steps**:
1. Create account at https://test.pypi.org/
2. Generate API token for TestPyPI
3. Upload to TestPyPI:
   ```bash
   uv run twine upload --repository testpypi dist/*
   ```
4. Test installation:
   ```bash
   pip install --index-url https://test.pypi.org/simple/ spark-profiler
   ```

### 10. Production Release Process

**Status**: ⏳ Pending

**Release Checklist**:
1. [ ] Ensure all tests pass
2. [ ] Update version in `pyproject.toml`
3. [ ] Update `CHANGELOG.md` with release notes
4. [ ] Commit changes: `git commit -am "Release version X.Y.Z"`
5. [ ] Create git tag: `git tag -a vX.Y.Z -m "Release version X.Y.Z"`
6. [ ] Push commits: `git push origin main`
7. [ ] Push tag: `git push origin vX.Y.Z`
8. [ ] Monitor GitHub Actions for successful deployment
9. [ ] Verify package on PyPI
10. [ ] Test installation: `pip install spark-profiler==X.Y.Z`

### 11. Post-Release Tasks

**Status**: ⏳ Pending

- [ ] Update documentation with new version
- [ ] Announce release (if applicable)
- [ ] Monitor for issues
- [ ] Plan next release features

## Automation Benefits

With this setup, releases are fully automated:
1. Push a tag → GitHub Actions triggered
2. Tests run automatically
3. Package built and checked
4. Published to PyPI
5. GitHub release created

## Security Considerations

- Never commit API tokens
- Use GitHub Secrets for sensitive data
- Enable 2FA on PyPI account
- Use project-specific tokens when possible
- Regularly rotate API tokens

## Troubleshooting

### Common Issues

1. **Token Authentication Failed**
   - Verify token is correctly set in GitHub Secrets
   - Check token permissions
   - Ensure token hasn't expired

2. **Package Build Fails**
   - Check all files are included
   - Verify version format
   - Run `uv build` locally first

3. **Import Errors After Installation**
   - Ensure all dependencies are listed
   - Check package structure
   - Verify `__init__.py` files exist

## Next Steps

1. Create PyPI account
2. Generate and configure API token
3. Make a test release to TestPyPI
4. Perform first production release (v0.1.0)

## Resources

- [PyPI Publishing Guide](https://packaging.python.org/tutorials/packaging-projects/)
- [GitHub Actions PyPI Publish](https://github.com/pypa/gh-action-pypi-publish)
- [Semantic Versioning](https://semver.org/)
- [Keep a Changelog](https://keepachangelog.com/)
