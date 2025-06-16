API Reference
=============

This section provides detailed API documentation for all public classes and functions in pyspark-analyzer.

Core Classes
------------

DataFrameProfiler
~~~~~~~~~~~~~~~~~

.. autoclass:: pyspark_analyzer.DataFrameProfiler
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__


Sampling
--------

SamplingConfig
~~~~~~~~~~~~~~

.. autoclass:: pyspark_analyzer.SamplingConfig
   :members:
   :undoc-members:
   :show-inheritance:

SamplingMetadata
~~~~~~~~~~~~~~~~

.. autoclass:: pyspark_analyzer.sampling.SamplingMetadata
   :members:
   :undoc-members:
   :show-inheritance:

SamplingDecisionEngine
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyspark_analyzer.sampling.SamplingDecisionEngine
   :members:
   :undoc-members:
   :show-inheritance:

Statistics
----------

StatisticsComputer
~~~~~~~~~~~~~~~~~~

.. autoclass:: pyspark_analyzer.statistics.StatisticsComputer
   :members:
   :undoc-members:
   :show-inheritance:

Performance
-----------

BatchStatisticsComputer
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: pyspark_analyzer.performance.BatchStatisticsComputer
   :members:
   :undoc-members:
   :show-inheritance:

Utility Functions
-----------------

.. automodule:: pyspark_analyzer.utils
   :members:
   :undoc-members:
   :show-inheritance:

Examples
--------

Basic profiling::

    from pyspark_analyzer import DataFrameProfiler

    profiler = DataFrameProfiler(df)
    profile = profiler.profile()

With sampling configuration::

    from pyspark_analyzer import DataFrameProfiler, SamplingConfig

    config = SamplingConfig(target_size=100_000)
    profiler = DataFrameProfiler(df, sampling_config=config)
    profile = profiler.profile()

Optimized for large datasets::

    profiler = DataFrameProfiler(df, optimize_for_large_datasets=True)
    profile = profiler.profile()
