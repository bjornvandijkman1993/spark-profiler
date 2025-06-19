"""
Internal DataFrame profiler implementation for PySpark DataFrames.

This module is for internal use only. Use the `analyze()` function from the main package instead.
"""

import warnings

import pandas as pd
from typing import Dict, Any, List, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError, Py4JJavaError

from .statistics import StatisticsComputer
from .utils import get_column_data_types, format_profile_output
from .performance import optimize_dataframe_for_profiling
from .sampling import SamplingConfig, SamplingMetadata, apply_sampling
from .exceptions import (
    DataTypeError,
    ColumnNotFoundError,
    SparkOperationError,
    StatisticsError,
)
from .logging import get_logger

logger = get_logger(__name__)


def profile_dataframe(
    dataframe: DataFrame,
    columns: Optional[List[str]] = None,
    output_format: str = "pandas",
    include_advanced: bool = True,
    include_quality: bool = True,
    sampling_config: Optional[SamplingConfig] = None,
) -> Union[pd.DataFrame, Dict[str, Any], str]:
    """
    Generate a comprehensive profile of a PySpark DataFrame.

    Args:
        dataframe: PySpark DataFrame to profile
        columns: List of specific columns to profile. If None, profiles all columns.
        output_format: Output format ("pandas", "dict", "json", "summary").
                      Defaults to "pandas" for easy analysis.
        include_advanced: Include advanced statistics (skewness, kurtosis, outliers, etc.)
        include_quality: Include data quality metrics
        sampling_config: Sampling configuration. If None, auto-sampling is enabled for large datasets.

    Returns:
        Profile results in requested format
    """
    if not isinstance(dataframe, DataFrame):
        logger.error("Input must be a PySpark DataFrame")
        raise DataTypeError("Input must be a PySpark DataFrame")

    logger.info(f"Starting profile_dataframe with {len(dataframe.columns)} columns")

    # Set up sampling with default config if not provided
    if sampling_config is None:
        sampling_config = SamplingConfig()

    # Apply sampling
    logger.debug("Applying sampling configuration")
    sampled_df, sampling_metadata = apply_sampling(dataframe, sampling_config)

    if sampling_metadata.is_sampled:
        logger.info(
            f"Sampling applied: {sampling_metadata.original_size} rows -> "
            f"{sampling_metadata.sample_size} rows (fraction: {sampling_metadata.sampling_fraction:.4f})"
        )
    else:
        logger.debug(
            f"No sampling applied, using full dataset with {sampling_metadata.sample_size} rows"
        )

    # Always optimize DataFrame for better performance
    logger.debug("Optimizing DataFrame for profiling")
    sampled_df = optimize_dataframe_for_profiling(
        sampled_df, row_count=sampling_metadata.sample_size
    )

    # Get column types
    column_types = get_column_data_types(sampled_df)

    # Select columns to profile
    if columns is None:
        columns = sampled_df.columns

    # Validate columns exist
    invalid_columns = set(columns) - set(sampled_df.columns)
    if invalid_columns:
        logger.error(f"Columns not found in DataFrame: {invalid_columns}")
        raise ColumnNotFoundError(list(invalid_columns), sampled_df.columns)

    logger.info(
        f"Profiling {len(columns)} columns: {columns[:5]}{'...' if len(columns) > 5 else ''}"
    )

    # Create profile result
    profile_result: Dict[str, Any] = {
        "overview": _get_overview(sampled_df, column_types, sampling_metadata),
        "columns": {},
        "sampling": _get_sampling_info(sampling_metadata),
    }

    # Initialize stats computer
    stats_computer = StatisticsComputer(
        sampled_df, total_rows=sampling_metadata.sample_size
    )

    # Always use batch processing for optimal performance
    logger.debug("Starting batch column profiling")
    try:
        profile_result["columns"] = stats_computer.compute_all_columns_batch(
            columns, include_advanced=include_advanced, include_quality=include_quality
        )
        logger.info("Column profiling completed")
    except (AnalysisException, Py4JError, Py4JJavaError) as e:
        logger.error(f"Spark error during batch profiling: {str(e)}")
        raise SparkOperationError(
            f"Failed to profile DataFrame due to Spark error: {str(e)}", e
        )
    except Exception as e:
        logger.error(f"Unexpected error during batch profiling: {str(e)}")
        raise StatisticsError(
            f"Failed to compute statistics during batch profiling: {str(e)}"
        )

    logger.debug(f"Formatting output as {output_format}")
    return format_profile_output(profile_result, output_format)


def _get_overview(
    df: DataFrame,
    column_types: Dict[str, Any],
    sampling_metadata: SamplingMetadata,
) -> Dict[str, Any]:
    """Get overview statistics for the entire DataFrame."""
    total_rows = sampling_metadata.sample_size
    total_columns = len(df.columns)

    return {
        "total_rows": total_rows,
        "total_columns": total_columns,
        "column_types": {col: str(dtype) for col, dtype in column_types.items()},
    }


def _profile_column(
    df: DataFrame,
    column_name: str,
    column_types: Dict[str, Any],
    stats_computer: StatisticsComputer,
    sampling_metadata: SamplingMetadata,
    include_advanced: bool = True,
    include_quality: bool = True,
) -> Dict[str, Any]:
    """
    Profile a single column.

    Args:
        df: DataFrame to profile
        column_name: Name of the column to profile
        column_types: Dictionary of column types
        stats_computer: Statistics computer instance
        sampling_metadata: Sampling metadata
        include_advanced: Include advanced statistics
        include_quality: Include data quality metrics

    Returns:
        Dictionary containing column statistics
    """
    column_type = column_types[column_name]

    # Handle empty DataFrame case
    if sampling_metadata.sample_size == 0:
        return {
            "data_type": str(column_type),
            "total_count": 0,
            "non_null_count": 0,
            "null_count": 0,
            "null_percentage": 0.0,
            "distinct_count": 0,
            "distinct_percentage": 0.0,
        }

    # Basic statistics for all columns
    basic_stats = stats_computer.compute_basic_stats(column_name)

    column_profile = {"data_type": str(column_type), **basic_stats}

    # Add type-specific statistics
    if isinstance(column_type, NumericType):
        numeric_stats = stats_computer.compute_numeric_stats(
            column_name, advanced=include_advanced
        )
        column_profile.update(numeric_stats)

        # Add outlier statistics for numeric columns
        if include_advanced:
            outlier_stats = stats_computer.compute_outlier_stats(column_name)
            column_profile["outliers"] = outlier_stats

    elif isinstance(column_type, StringType):
        string_stats = stats_computer.compute_string_stats(
            column_name,
            top_n=10 if include_advanced else 0,
            pattern_detection=include_advanced,
        )
        column_profile.update(string_stats)

    elif isinstance(column_type, (TimestampType, DateType)):
        temporal_stats = stats_computer.compute_temporal_stats(column_name)
        column_profile.update(temporal_stats)

    # Add data quality metrics if requested
    if include_quality:
        # Determine quality check type based on column type
        if isinstance(column_type, NumericType):
            quality_type = "numeric"
        elif isinstance(column_type, StringType):
            quality_type = "string"
        else:
            # For complex types (arrays, structs, etc.), skip type-specific quality checks
            quality_type = "other"

        quality_stats = stats_computer.compute_data_quality_stats(
            column_name,
            column_type=quality_type,
        )
        column_profile["quality"] = quality_stats

    return column_profile


def _get_sampling_info(sampling_metadata: Optional[SamplingMetadata]) -> Dict[str, Any]:
    """Get sampling information for the profile."""
    if not sampling_metadata:
        return {"is_sampled": False}

    return {
        "is_sampled": sampling_metadata.is_sampled,
        "original_size": sampling_metadata.original_size,
        "sample_size": sampling_metadata.sample_size,
        "sampling_fraction": sampling_metadata.sampling_fraction,
        "sampling_time": sampling_metadata.sampling_time,
        "estimated_speedup": sampling_metadata.speedup_estimate,
    }


# Backwards compatibility: Keep the DataFrameProfiler class but add deprecation warning
class DataFrameProfiler:
    """
    Main profiler class for generating comprehensive statistics for PySpark DataFrames.

    .. deprecated:: 3.0.0
        DataFrameProfiler is deprecated. Use the `analyze()` function instead.

    This class analyzes a PySpark DataFrame and computes various statistics for each column,
    including basic counts, data type specific metrics, and null value analysis.
    """

    def __init__(
        self,
        dataframe: DataFrame,
        sampling_config: Optional[SamplingConfig] = None,
    ):
        """
        Initialize the profiler with a PySpark DataFrame.

        Args:
            dataframe: PySpark DataFrame to profile
            sampling_config: Sampling configuration. If None, auto-sampling is enabled for large datasets.
        """
        warnings.warn(
            "DataFrameProfiler is deprecated and will be removed in a future version. "
            "Use the analyze() function instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        if not isinstance(dataframe, DataFrame):
            logger.error("Input must be a PySpark DataFrame")
            raise DataTypeError("Input must be a PySpark DataFrame")

        # Set up sampling with default config if not provided
        if sampling_config is None:
            sampling_config = SamplingConfig()

        self.sampling_config = sampling_config
        self.sampling_metadata: Optional[SamplingMetadata] = None

        # Store original DataFrame
        self._original_dataframe = dataframe
        self.df = dataframe
        self._sampling_applied = False

        # Initialize with lazy evaluation - defer heavy operations
        self.column_types = get_column_data_types(self.df)
        self.stats_computer: Optional[StatisticsComputer] = None

    def _apply_sampling(self) -> None:
        """Apply sampling if not already applied."""
        if self._sampling_applied:
            return

        # Apply sampling
        self.df, self.sampling_metadata = apply_sampling(
            self._original_dataframe, self.sampling_config
        )

        # Always optimize DataFrame for better performance
        self.df = optimize_dataframe_for_profiling(
            self.df, row_count=self.sampling_metadata.sample_size
        )

        # Update column types if DataFrame changed
        self.column_types = get_column_data_types(self.df)
        self._sampling_applied = True

    def _ensure_stats_computer(self) -> StatisticsComputer:
        """Initialize stats computer with lazy evaluation."""
        if self.stats_computer is None:
            self._apply_sampling()
            # Ensure sampling_metadata is available after _apply_sampling
            assert self.sampling_metadata is not None
            # Pass the sample size to avoid redundant count operations
            self.stats_computer = StatisticsComputer(
                self.df, total_rows=self.sampling_metadata.sample_size
            )
        return self.stats_computer

    def profile(
        self,
        columns: Optional[List[str]] = None,
        output_format: str = "pandas",
        include_advanced: bool = True,
        include_quality: bool = True,
    ) -> Union[pd.DataFrame, Dict[str, Any], str]:
        """
        Generate a comprehensive profile of the DataFrame.

        Args:
            columns: List of specific columns to profile. If None, profiles all columns.
            output_format: Output format ("pandas", "dict", "json", "summary")
                          Defaults to "pandas" for easy analysis.
            include_advanced: Include advanced statistics (skewness, kurtosis, outliers, etc.)
            include_quality: Include data quality metrics

        Returns:
            Profile results in requested format
        """
        # Use the new standalone function
        return profile_dataframe(
            dataframe=self._original_dataframe,
            columns=columns,
            output_format=output_format,
            include_advanced=include_advanced,
            include_quality=include_quality,
            sampling_config=self.sampling_config,
        )
