"""
PySpark DataFrame Profiler

A library for generating comprehensive profiles of PySpark DataFrames with statistics
for all columns including null counts, data type specific metrics, and performance optimizations.
"""

from .api import analyze

__version__ = "2.0.0"
__all__ = [
    "analyze",
]
