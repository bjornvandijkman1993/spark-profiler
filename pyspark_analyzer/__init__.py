"""
PySpark DataFrame Profiler

A library for generating comprehensive profiles of PySpark DataFrames with statistics
for all columns including null counts, data type specific metrics, and performance optimizations.
"""

from .profiler import DataFrameProfiler
from .sampling import SamplingConfig
from .statistics import LazyRowCount

__version__ = "0.3.2"
__all__ = [
    "DataFrameProfiler",
    "SamplingConfig",
    "LazyRowCount",
]
