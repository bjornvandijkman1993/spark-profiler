# Documentation

This directory contains the source files for pyspark-analyzer's documentation, built with Sphinx.

## Building Documentation Locally

### Prerequisites

Install the documentation dependencies:

```bash
uv sync --all-extras
# or
pip install -e .[docs]
```

### Build HTML Documentation

```bash
cd docs
make html
```

The built documentation will be in `docs/build/html/`. Open `index.html` in your browser to view.

### Build Other Formats

```bash
# PDF (requires LaTeX)
make latexpdf

# ePub
make epub

# Plain text
make text
```

### Live Development Server

For development with auto-reload:

```bash
pip install sphinx-autobuild
sphinx-autobuild source build/html
```

Then visit http://127.0.0.1:8000

## Documentation Structure

```
docs/
├── source/
│   ├── _static/        # Custom CSS and static files
│   ├── _templates/     # Custom templates (if needed)
│   ├── conf.py         # Sphinx configuration
│   ├── index.rst       # Main documentation index
│   ├── installation.md # Installation guide
│   ├── quickstart.md   # Quick start guide
│   ├── user_guide.md   # Detailed user guide
│   ├── api_reference.rst # API documentation
│   ├── examples.md     # Usage examples
│   ├── contributing.md # Contributing guidelines
│   └── changelog.md    # Version history
├── build/              # Built documentation (git ignored)
├── Makefile           # Build commands
└── README.md          # This file
```

## Writing Documentation

### Style Guidelines

1. **Use clear, concise language**
2. **Include code examples** for all features
3. **Add type hints** in code examples
4. **Test all code examples** before committing
5. **Use semantic headings** (one H1 per page)
6. **Cross-reference** related sections

### Markdown vs reStructuredText

- Use Markdown (`.md`) for narrative documentation
- Use reStructuredText (`.rst`) for API documentation that needs Sphinx directives
- Both formats are supported via MyST parser

### Adding Code Examples

```markdown
```python
from pyspark_analyzer import DataFrameProfiler

profiler = DataFrameProfiler(df)
profile = profiler.profile()
```
```

### Cross-References

In Markdown:
```markdown
See the [API Reference](api_reference.rst) for details.
```

In reStructuredText:
```rst
See :doc:`api_reference` for details.
```

## Deployment

Documentation is automatically built and deployed via GitHub Actions:

1. **Pull Requests**: Documentation is built to check for errors
2. **Main Branch**: Documentation is deployed to GitHub Pages
3. **Read the Docs**: Alternative hosting with version support

### GitHub Pages

Accessible at: https://bjornvandijkman1993.github.io/pyspark-analyzer/

### Read the Docs

Accessible at: https://pyspark-analyzer.readthedocs.io/

## Troubleshooting

### Import Errors

If Sphinx can't import the module:
1. Ensure pyspark-analyzer is installed: `pip install -e .`
2. Check the Python path in `conf.py`
3. Verify Java is installed (required by PySpark)

### Build Warnings

Common warnings and solutions:
- **"document isn't included in any toctree"**: Add the document to a toctree
- **"Duplicate object description"**: Remove duplicate autodoc directives
- **"Unknown target name"**: Check cross-reference syntax

### Missing Dependencies

Install all dependencies:
```bash
uv sync --all-extras
```

## Contributing to Documentation

1. **Fork and clone** the repository
2. **Create a branch** for your changes
3. **Build locally** to test changes
4. **Submit a PR** with clear description

See [Contributing Guidelines](source/contributing.md) for more details.
