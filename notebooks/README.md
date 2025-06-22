# PySpark Analyzer Notebooks

This directory contains Jupyter notebooks for testing and demonstrating the pyspark-analyzer package.

## Setup Instructions

### 1. Install Dependencies

First, ensure you have Java 17+ installed (required for PySpark):

**macOS:**
```bash
brew install openjdk@17
```

**Ubuntu/Debian:**
```bash
sudo apt-get install openjdk-17-jdk
```

**Windows:**
Download from [Oracle](https://www.oracle.com/java/technologies/downloads/) or [OpenJDK](https://adoptium.net/)

### 2. Install Jupyter and Required Packages

Using uv (recommended):
```bash
# From the project root directory
uv add --dev jupyter notebook ipykernel
uv sync
```

Or using pip:
```bash
pip install jupyter notebook ipykernel pyspark pandas numpy
```

### 3. Install the Package in Development Mode

From the project root directory:
```bash
# Using uv
uv pip install -e .

# Or using pip
pip install -e .
```

### 4. Launch Jupyter Notebook

```bash
# Using uv
uv run jupyter notebook notebooks/

# Or directly
jupyter notebook notebooks/
```

## Available Notebooks

- **test_pyspark_analyzer.ipynb**: Comprehensive testing notebook that demonstrates:
  - Setting up a local Spark session
  - Creating sample datasets
  - Running analysis with different configurations
  - Testing sampling features
  - Advanced statistics and data quality analysis
  - Performance comparisons
  - Exporting results in various formats

## Running Spark Locally

The notebook is configured to run Spark in local mode with these settings:
- `master("local[*]")`: Uses all available CPU cores
- `spark.driver.memory`: 4GB (adjust based on your system)
- `spark.executor.memory`: 4GB (adjust based on your system)
- Adaptive Query Execution enabled for better performance

## Troubleshooting

### Java Issues
If you encounter Java-related errors:
1. Verify Java installation: `java -version` (should show version 17+)
2. Set JAVA_HOME environment variable:
   ```bash
   # macOS with Homebrew
   export JAVA_HOME=$(/usr/libexec/java_home -v 17)

   # Linux
   export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
   ```

### Memory Issues
If you run out of memory:
1. Reduce the sample data size in the notebook
2. Adjust Spark memory settings in the SparkSession configuration
3. Close other applications to free up memory

### Import Errors
If you can't import pyspark_analyzer:
1. Ensure you're in the correct directory
2. Verify the package is installed: `pip list | grep pyspark-analyzer`
3. The notebook adds the parent directory to sys.path automatically

## Tips for Local Development

1. **Start Small**: Begin with small datasets (1,000-10,000 rows) to test functionality
2. **Monitor Resources**: Keep an eye on memory usage, especially with larger datasets
3. **Use Sampling**: The analyzer's sampling features are especially useful for local development
4. **Check Spark UI**: Access the Spark UI (usually at http://localhost:4040) to monitor job progress

## Next Steps

After running the test notebook:
1. Try analyzing your own datasets
2. Experiment with different sampling configurations
3. Explore the advanced statistics features
4. Test the data quality analysis capabilities
5. Integrate the analyzer into your data processing pipelines
