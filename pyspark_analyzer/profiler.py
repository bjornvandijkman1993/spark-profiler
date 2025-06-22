"""
Internal DataFrame profiler implementation for PySpark DataFrames.

This module is for internal use only. Use the `analyze()` function from the main package instead.
"""

from typing import Any

import pandas as pd
from py4j.protocol import Py4JError, Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from .exceptions import (
    ColumnNotFoundError,
    DataTypeError,
    SparkOperationError,
    StatisticsError,
)
from .logging import get_logger
from .performance import optimize_dataframe_for_profiling
from .progress import ProgressStage
from .sampling import SamplingConfig, SamplingMetadata, apply_sampling
from .statistics import StatisticsComputer
from .utils import format_profile_output

logger = get_logger(__name__)


def profile_dataframe(
    dataframe: DataFrame,
    columns: list[str] | None = None,
    output_format: str = "pandas",
    include_advanced: bool = True,
    include_quality: bool = True,
    sampling_config: SamplingConfig | None = None,
    show_progress: bool | None = None,
) -> pd.DataFrame | dict[str, Any] | str:
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
        show_progress: Show progress indicators during profiling. If None, auto-detects based on environment.

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

    # Set up progress tracking
    progress_stage = ProgressStage(
        [
            ("Counting rows", 1),
            ("Applying sampling", 1),
            ("Computing statistics", 5),
            ("Formatting output", 1),
        ],
        show_progress=show_progress,
    )
    progress_stage.start()

    # Apply sampling
    logger.debug("Applying sampling configuration")
    sampled_df, sampling_metadata = apply_sampling(dataframe, sampling_config)

    # Move to next stage after sampling
    progress_stage.next_stage()

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
    profile_result: dict[str, Any] = {
        "overview": _get_overview(sampled_df, column_types, sampling_metadata),
        "columns": {},
        "sampling": _get_sampling_info(sampling_metadata),
    }

    # Initialize stats computer
    stats_computer = StatisticsComputer(
        sampled_df, total_rows=sampling_metadata.sample_size
    )

    # Move to statistics computation stage
    stats_tracker = progress_stage.next_stage()

    # Always use batch processing for optimal performance
    logger.debug("Starting batch column profiling")
    logger.debug("Starting batch computation")
    try:
        profile_result["columns"] = stats_computer.compute_all_columns_batch(
            columns,
            include_advanced=include_advanced,
            include_quality=include_quality,
            progress_tracker=stats_tracker,
        )
        logger.info("Column profiling completed")
    except (AnalysisException, Py4JError, Py4JJavaError) as e:
        logger.error(f"Spark error during batch profiling: {e!s}")
        raise SparkOperationError(
            f"Failed to profile DataFrame due to Spark error: {e!s}", e
        ) from e
    except Exception as e:
        logger.error(f"Unexpected error during batch profiling: {e!s}")
        raise StatisticsError(
            f"Failed to compute statistics during batch profiling: {e!s}"
        ) from e

    # Move to formatting stage
    progress_stage.next_stage()

    # Move to formatting stage
    progress_stage.next_stage()

    logger.debug(f"Formatting output as {output_format}")
    result = format_profile_output(profile_result, output_format)

    # Finish progress tracking
    progress_stage.finish()

    return result


def _get_overview(
    df: DataFrame,
    column_types: dict[str, Any],
    sampling_metadata: SamplingMetadata,
) -> dict[str, Any]:
    """Get overview statistics for the entire DataFrame."""
    total_rows = sampling_metadata.sample_size
    total_columns = len(df.columns)

    return {
        "total_rows": total_rows,
        "total_columns": total_columns,
        "column_types": {col: str(dtype) for col, dtype in column_types.items()},
    }


def _get_sampling_info(sampling_metadata: SamplingMetadata | None) -> dict[str, Any]:
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
