"""
Main DataFrame profiler class for PySpark DataFrames.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, min as spark_min, max as spark_max, mean, stddev, expr
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

from .statistics import StatisticsComputer
from .utils import get_column_data_types
from .performance import BatchStatisticsComputer, optimize_dataframe_for_profiling


class DataFrameProfiler:
    """
    Main profiler class for generating comprehensive statistics for PySpark DataFrames.
    
    This class analyzes a PySpark DataFrame and computes various statistics for each column
    including basic counts, data type specific metrics, and null value analysis.
    """
    
    def __init__(self, dataframe: DataFrame, optimize_for_large_datasets: bool = False, 
                 sample_fraction: Optional[float] = None):
        """
        Initialize the profiler with a PySpark DataFrame.
        
        Args:
            dataframe: PySpark DataFrame to profile
            optimize_for_large_datasets: If True, use optimized batch processing for better performance
            sample_fraction: If provided, sample the DataFrame to this fraction for faster profiling
        """
        if not isinstance(dataframe, DataFrame):
            raise TypeError("Input must be a PySpark DataFrame")
        
        # Optimize DataFrame if requested
        if optimize_for_large_datasets or sample_fraction:
            self.df = optimize_dataframe_for_profiling(dataframe, sample_fraction)
        else:
            self.df = dataframe
            
        self.column_types = get_column_data_types(self.df)
        self.stats_computer = StatisticsComputer(self.df)
        self.batch_computer = BatchStatisticsComputer(self.df) if optimize_for_large_datasets else None
        self.optimize_for_large_datasets = optimize_for_large_datasets
    
    def profile(self, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Generate a comprehensive profile of the DataFrame.
        
        Args:
            columns: List of specific columns to profile. If None, profiles all columns.
            
        Returns:
            Dictionary containing profile information with column names as keys
        """
        if columns is None:
            columns = self.df.columns
        
        # Validate columns exist
        invalid_columns = set(columns) - set(self.df.columns)
        if invalid_columns:
            raise ValueError(f"Columns not found in DataFrame: {invalid_columns}")
        
        profile_result = {
            'overview': self._get_overview(),
            'columns': {}
        }
        
        # Use batch processing for large datasets if enabled
        if self.optimize_for_large_datasets and self.batch_computer:
            profile_result['columns'] = self.batch_computer.compute_all_columns_batch(columns)
        else:
            # Standard column-by-column processing
            for column in columns:
                profile_result['columns'][column] = self._profile_column(column)
        
        return profile_result
    
    def _get_overview(self) -> Dict[str, Any]:
        """Get overview statistics for the entire DataFrame."""
        total_rows = self.df.count()
        total_columns = len(self.df.columns)
        
        return {
            'total_rows': total_rows,
            'total_columns': total_columns,
            'column_types': self.column_types
        }
    
    def _profile_column(self, column_name: str) -> Dict[str, Any]:
        """
        Profile a single column.
        
        Args:
            column_name: Name of the column to profile
            
        Returns:
            Dictionary containing column statistics
        """
        column_type = self.column_types[column_name]
        
        # Basic statistics for all columns
        basic_stats = self.stats_computer.compute_basic_stats(column_name)
        
        column_profile = {
            'data_type': str(column_type),
            **basic_stats
        }
        
        # Add type-specific statistics
        if isinstance(column_type, NumericType):
            numeric_stats = self.stats_computer.compute_numeric_stats(column_name)
            column_profile.update(numeric_stats)
        elif isinstance(column_type, StringType):
            string_stats = self.stats_computer.compute_string_stats(column_name)
            column_profile.update(string_stats)
        elif isinstance(column_type, (TimestampType, DateType)):
            temporal_stats = self.stats_computer.compute_temporal_stats(column_name)
            column_profile.update(temporal_stats)
        
        return column_profile