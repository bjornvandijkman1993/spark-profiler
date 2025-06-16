"""
PySpark DataFrame Profiler

A library for generating comprehensive profiles of PySpark DataFrames with statistics
for all columns including null counts, data type specific metrics, and performance optimizations.
"""

from .profiler import DataFrameProfiler
from .sampling import SamplingConfig, create_sampling_config

__version__ = "0.1.4"
__all__ = ["DataFrameProfiler", "SamplingConfig", "create_sampling_config"]
