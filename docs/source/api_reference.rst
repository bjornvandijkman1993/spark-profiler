API Reference
=============

This section provides detailed API documentation for all public classes and functions in pyspark-analyzer.

Main Function
-------------

analyze
~~~~~~~

.. autofunction:: pyspark_analyzer.analyze


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

    from pyspark_analyzer import analyze

    # Get profile as pandas DataFrame
    profile = analyze(df)

With sampling configuration::

    from pyspark_analyzer import analyze

    # Sample to 100,000 rows
    profile = analyze(df, target_rows=100_000)

    # Or sample 10% of data
    profile = analyze(df, fraction=0.1)

With automatic sampling for large datasets::

    profile = analyze(df, sampling=True)

Different output formats::

    # Get as dictionary
    profile_dict = analyze(df, output_format="dict")

    # Get as JSON
    profile_json = analyze(df, output_format="json")

    # Get human-readable summary
    summary = analyze(df, output_format="summary")
