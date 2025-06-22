"""
Internal DataFrame profiler implementation for PySpark DataFrames.

This module is for internal use only. Use the `analyze()` function from the main package instead.
"""

import pandas as pd
from typing import Dict, Any, List, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError, Py4JJavaError

from .statistics import StatisticsComputer
from .utils import format_profile_output
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
    column_types = {field.name: field.dataType for field in sampled_df.schema.fields}

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
    logger.debug("Starting batch computation")
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
