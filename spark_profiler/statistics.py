"""
Statistics computation functions for DataFrame profiling.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, when, isnan, isnull, min as spark_min, max as spark_max, 
    mean, stddev, expr, length, approx_count_distinct, collect_list, size
)


class StatisticsComputer:
    """Handles computation of various statistics for DataFrame columns."""
    
    def __init__(self, dataframe: DataFrame):
        """
        Initialize with a PySpark DataFrame.
        
        Args:
            dataframe: PySpark DataFrame to compute statistics for
        """
        self.df = dataframe
        self.total_rows = None  # Lazy evaluation
    
    def _get_total_rows(self) -> int:
        """Get total row count (cached for performance)."""
        if self.total_rows is None:
            self.total_rows = self.df.count()
        return self.total_rows
    
    def compute_basic_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Compute basic statistics for any column type.
        
        Args:
            column_name: Name of the column
            
        Returns:
            Dictionary with basic statistics
        """
        total_rows = self._get_total_rows()
        
        # Single aggregation for efficiency - optimized for large datasets
        result = self.df.agg(
            count(col(column_name)).alias("non_null_count"),
            count(when(col(column_name).isNull(), 1)).alias("null_count"),
            approx_count_distinct(col(column_name), rsd=0.05).alias("distinct_count")  # 5% relative error for speed
        ).collect()[0]
        
        non_null_count = result["non_null_count"]
        null_count = result["null_count"]
        distinct_count = result["distinct_count"]
        
        return {
            'total_count': total_rows,
            'non_null_count': non_null_count,
            'null_count': null_count,
            'null_percentage': (null_count / total_rows * 100) if total_rows > 0 else 0.0,
            'distinct_count': distinct_count,
            'distinct_percentage': (distinct_count / non_null_count * 100) if non_null_count > 0 else 0.0
        }
    
    def compute_numeric_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Compute statistics specific to numeric columns.
        
        Args:
            column_name: Name of the numeric column
            
        Returns:
            Dictionary with numeric statistics
        """
        # Single aggregation for all numeric stats
        result = self.df.agg(
            spark_min(col(column_name)).alias("min_value"),
            spark_max(col(column_name)).alias("max_value"),
            mean(col(column_name)).alias("mean_value"),
            stddev(col(column_name)).alias("std_value"),
            expr(f"percentile_approx({column_name}, 0.5)").alias("median_value"),
            expr(f"percentile_approx({column_name}, 0.25)").alias("q1_value"),
            expr(f"percentile_approx({column_name}, 0.75)").alias("q3_value")
        ).collect()[0]
        
        return {
            'min': result["min_value"],
            'max': result["max_value"],
            'mean': result["mean_value"],
            'std': result["std_value"],
            'median': result["median_value"],
            'q1': result["q1_value"],
            'q3': result["q3_value"]
        }
    
    def compute_string_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Compute statistics specific to string columns.
        
        Args:
            column_name: Name of the string column
            
        Returns:
            Dictionary with string statistics
        """
        # Compute string length statistics
        result = self.df.agg(
            spark_min(length(col(column_name))).alias("min_length"),
            spark_max(length(col(column_name))).alias("max_length"),
            mean(length(col(column_name))).alias("avg_length"),
            count(when(col(column_name) == "", 1)).alias("empty_count")
        ).collect()[0]
        
        return {
            'min_length': result["min_length"],
            'max_length': result["max_length"],
            'avg_length': result["avg_length"],
            'empty_count': result["empty_count"]
        }
    
    def compute_temporal_stats(self, column_name: str) -> Dict[str, Any]:
        """
        Compute statistics specific to temporal columns (date/timestamp).
        
        Args:
            column_name: Name of the temporal column
            
        Returns:
            Dictionary with temporal statistics
        """
        result = self.df.agg(
            spark_min(col(column_name)).alias("min_date"),
            spark_max(col(column_name)).alias("max_date")
        ).collect()[0]
        
        min_date = result["min_date"]
        max_date = result["max_date"]
        
        # Calculate date range in days if both dates are present
        date_range_days = None
        if min_date and max_date:
            try:
                date_range_days = (max_date - min_date).days
            except (AttributeError, TypeError):
                # Handle different datetime types
                pass
        
        return {
            'min_date': min_date,
            'max_date': max_date,
            'date_range_days': date_range_days
        }