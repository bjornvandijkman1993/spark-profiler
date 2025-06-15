.. spark-profiler documentation master file

Welcome to spark-profiler's documentation!
==========================================

.. image:: https://img.shields.io/pypi/v/spark-profiler.svg
   :target: https://pypi.python.org/pypi/spark-profiler
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/spark-profiler.svg
   :target: https://pypi.python.org/pypi/spark-profiler
   :alt: Python versions

.. image:: https://github.com/yourusername/spark-profiler/workflows/CI/badge.svg
   :target: https://github.com/yourusername/spark-profiler/actions
   :alt: CI Status

**spark-profiler** is a comprehensive profiling library for Apache Spark DataFrames, designed to help data engineers and scientists understand their data quickly and efficiently.

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

   pip install spark-profiler

Quick Start
-----------

.. code-block:: python

   from pyspark.sql import SparkSession
   from spark_profiler import DataFrameProfiler

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
