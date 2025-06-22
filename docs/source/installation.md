# Installation

## Requirements

- Python >= 3.8
- Apache Spark >= 3.0 (native median function available in 3.4.0+, fallback for older versions)
- Java 8 or 11 (required by Spark)

## Install from PyPI

```bash
pip install pyspark-analyzer
```

## Install from Source

### Using pip

```bash
git clone https://github.com/bjornvandijkman1993/pyspark-analyzer.git
cd pyspark-analyzer
pip install -e .
```

### Using uv (recommended for development)

```bash
git clone https://github.com/bjornvandijkman1993/pyspark-analyzer.git
cd pyspark-analyzer
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

## Verify Installation

```python
import pyspark_analyzer
print(pyspark_analyzer.__version__)

# Run the verification script
python examples/installation_verification.py
```

## Troubleshooting

### Java Not Found

If you encounter Java-related errors, ensure Java is properly installed:

```bash
java -version
```

### Spark Configuration Issues

Set the following environment variables if needed:

```bash
export JAVA_HOME=/path/to/java
export SPARK_HOME=/path/to/spark
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
```

For more detailed troubleshooting, see our [Troubleshooting Guide](https://github.com/bjornvandijkman1993/pyspark-analyzer/blob/main/TROUBLESHOOTING.md).
