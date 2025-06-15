API Reference
=============

This section provides detailed API documentation for all public classes and functions in spark-profiler.

Core Classes
------------

DataFrameProfiler
~~~~~~~~~~~~~~~~~

.. autoclass:: spark_profiler.DataFrameProfiler
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__


Sampling
--------

SamplingConfig
~~~~~~~~~~~~~~

.. autoclass:: spark_profiler.SamplingConfig
   :members:
   :undoc-members:
   :show-inheritance:

SamplingMetadata
~~~~~~~~~~~~~~~~

.. autoclass:: spark_profiler.sampling.SamplingMetadata
   :members:
   :undoc-members:
   :show-inheritance:

SamplingDecisionEngine
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: spark_profiler.sampling.SamplingDecisionEngine
   :members:
   :undoc-members:
   :show-inheritance:

Statistics
----------

StatisticsComputer
~~~~~~~~~~~~~~~~~~

.. autoclass:: spark_profiler.statistics.StatisticsComputer
   :members:
   :undoc-members:
   :show-inheritance:

Performance
-----------

BatchStatisticsComputer
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: spark_profiler.performance.BatchStatisticsComputer
   :members:
   :undoc-members:
   :show-inheritance:

Utility Functions
-----------------

.. automodule:: spark_profiler.utils
   :members:
   :undoc-members:
   :show-inheritance:

Examples
--------

Basic profiling::

    from spark_profiler import DataFrameProfiler

    profiler = DataFrameProfiler(df)
    profile = profiler.profile()

With sampling configuration::

    from spark_profiler import DataFrameProfiler, SamplingConfig

    config = SamplingConfig(target_size=100_000)
    profiler = DataFrameProfiler(df, sampling_config=config)
    profile = profiler.profile()

Optimized for large datasets::

    profiler = DataFrameProfiler(df, optimize_for_large_datasets=True)
    profile = profiler.profile()
