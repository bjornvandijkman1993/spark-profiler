.. pyspark-analyzer documentation master file

Welcome to pyspark-analyzer's documentation!
==========================================

.. image:: https://img.shields.io/pypi/v/pyspark-analyzer.svg
   :target: https://pypi.python.org/pypi/pyspark-analyzer
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/pyspark-analyzer.svg
   :target: https://pypi.python.org/pypi/pyspark-analyzer
   :alt: Python versions

.. image:: https://github.com/yourusername/pyspark-analyzer/workflows/CI/badge.svg
   :target: https://github.com/yourusername/pyspark-analyzer/actions
   :alt: CI Status

**pyspark-analyzer** is a comprehensive profiling library for Apache Spark DataFrames, designed to help data engineers and scientists understand their data quickly and efficiently.

Key Features
------------

* **Comprehensive Statistics**: Automatic computation of data type-specific statistics
* **Performance Optimized**: Intelligent sampling and batch processing for large datasets
* **Type-Aware**: Different statistics for numeric, string, and temporal columns
* **Flexible Output**: Multiple output formats (dict, JSON, summary report)
* **Easy Integration**: Simple API that works with any PySpark DataFrame

Installation
------------

.. code-block:: bash

   pip install pyspark-analyzer

Quick Start
-----------

.. code-block:: python

   from pyspark.sql import SparkSession
   from pyspark_analyzer import DataFrameProfiler

   # Create a Spark session
   spark = SparkSession.builder.appName("ProfilerExample").getOrCreate()

   # Load your DataFrame
   df = spark.read.csv("data.csv", header=True, inferSchema=True)

   # Create profiler and generate profile
   profiler = DataFrameProfiler(df)
   profile = profiler.profile()

   # Print summary report
   print(profiler.get_profile("summary"))

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   user_guide
   api_reference
   examples
   contributing
   changelog

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
