# PyPI Setup Instructions

This document provides step-by-step instructions for setting up PyPI publishing for pyspark-analyzer.

## 1. Create PyPI Account

### Production PyPI
1. Go to https://pypi.org/account/register/
2. Fill in the registration form
3. Verify your email address
4. Enable 2FA (strongly recommended):
   - Go to Account Settings → Security
   - Enable Two-factor authentication

### Test PyPI (Recommended for first-time publishers)
1. Go to https://test.pypi.org/account/register/
2. Follow the same process as above
3. Note: TestPyPI is a separate service with different accounts

## 2. Generate API Tokens

### PyPI Token
1. Log in to https://pypi.org
2. Go to Account Settings → API tokens
3. Click "Add API token"
4. Token name: "pyspark-analyzer-github-actions"
5. Scope: "Project: pyspark-analyzer" (or "Entire account" for first release)
6. Click "Generate token"
7. **IMPORTANT**: Copy the token immediately (shown only once!)
8. Save it securely (password manager recommended)

### TestPyPI Token (Optional but recommended)
1. Log in to https://test.pypi.org
2. Follow the same process as above
3. Keep this token separate from production

## 3. Configure GitHub Secrets

1. Go to your GitHub repository
2. Navigate to Settings → Secrets and variables → Actions
3. Click "New repository secret"
4. Add the following secrets:

### Required Secret
- **Name**: `PYPI_API_TOKEN`
- **Value**: Your PyPI token (starts with `pypi-`)

### Optional Secret (for testing)
- **Name**: `TEST_PYPI_API_TOKEN`
- **Value**: Your TestPyPI token

## 4. Local Testing (Optional)

### Install Twine
```bash
pip install twine
```

### Configure .pypirc (Alternative to tokens)
Create `~/.pypirc` file:
```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-YOUR-TOKEN-HERE

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-YOUR-TEST-TOKEN-HERE
```

**Security Note**: Ensure `.pypirc` has restricted permissions:
```bash
chmod 600 ~/.pypirc
```

## 5. First Release Checklist

### Pre-release
- [ ] All tests passing
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped in pyproject.toml
- [ ] Changes committed and pushed

### Test Release (Recommended)
1. Build package:
   ```bash
   uv build
   ```

2. Upload to TestPyPI:
   ```bash
   twine upload --repository testpypi dist/*
   ```

3. Test installation:
   ```bash
   pip install --index-url https://test.pypi.org/simple/ \
               --extra-index-url https://pypi.org/simple/ \
               pyspark-analyzer
   ```

### Production Release
1. Create and push tag:
   ```bash
   git tag -a v0.1.0 -m "Initial release"
   git push origin v0.1.0
   ```

2. GitHub Actions will automatically:
   - Run tests
   - Build package
   - Upload to PyPI
   - Create GitHub release

## 6. Post-Release Verification

1. Check PyPI page: https://pypi.org/project/pyspark-analyzer/
2. Test installation:
   ```bash
   pip install pyspark-analyzer
   ```
3. Verify functionality:
   ```python
   from pyspark_analyzer import DataFrameProfiler
   print("Success!")
   ```

## 7. Troubleshooting

### "Invalid or non-existent authentication"
- Verify token is correct in GitHub secrets
- Ensure token hasn't expired
- Check token scope includes the project

### "Package already exists"
- Version already published (versions are immutable)
- Increment version and try again

### "Invalid distribution file"
- Run `twine check dist/*` locally
- Ensure all metadata is correct in pyproject.toml

## 8. Security Best Practices

1. **Never commit tokens** to version control
2. **Use project-scoped tokens** when possible
3. **Rotate tokens periodically**
4. **Enable 2FA** on PyPI account
5. **Use GitHub Secrets** for CI/CD
6. **Restrict .pypirc permissions** if used locally

## 9. Additional Resources

- [Python Packaging User Guide](https://packaging.python.org/)
- [Twine Documentation](https://twine.readthedocs.io/)
- [PyPI Help](https://pypi.org/help/)
- [GitHub Actions PyPI Publish](https://github.com/pypa/gh-action-pypi-publish)
