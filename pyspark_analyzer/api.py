from typing import Optional, List, Union, Any
import pandas as pd
from pyspark.sql import DataFrame

from .profiler import DataFrameProfiler
from .sampling import SamplingConfig


def analyze(
    df: DataFrame,
    *,
    sampling: Optional[bool] = None,
    target_rows: Optional[int] = None,
    fraction: Optional[float] = None,
    columns: Optional[List[str]] = None,
    output_format: str = "pandas",
    include_advanced: bool = True,
    include_quality: bool = True,
    optimize_for_large_datasets: bool = False,
    auto_threshold: Optional[int] = None,
    seed: Optional[int] = None,
) -> Union[pd.DataFrame, dict, str]:
    """
    Analyze a PySpark DataFrame and generate comprehensive statistics.

    This is the simplified entry point for profiling DataFrames. It automatically
    handles sampling configuration based on the provided parameters.

    Args:
        df: PySpark DataFrame to analyze
        sampling: Whether to enable sampling. If None, auto-sampling is enabled for large datasets.
                 If False, no sampling. If True, uses default sampling.
        target_rows: Sample to approximately this many rows. Mutually exclusive with fraction.
        fraction: Sample this fraction of the data (0.0-1.0). Mutually exclusive with target_rows.
        columns: List of specific columns to profile. If None, profiles all columns.
        output_format: Output format ("pandas", "dict", "json", "summary"). Default is "pandas".
        include_advanced: Include advanced statistics (skewness, kurtosis, outliers, etc.)
        include_quality: Include data quality metrics
        optimize_for_large_datasets: Use optimized batch processing for better performance
        auto_threshold: Custom threshold for auto-sampling (default: 10,000,000 rows)
        seed: Random seed for reproducible sampling

    Returns:
        Profile results in the requested format:
        - "pandas": pandas DataFrame with statistics
        - "dict": Python dictionary
        - "json": JSON string
        - "summary": Human-readable summary string

    Examples:
        >>> # Basic usage with auto-sampling
        >>> profile = analyze(df)

        >>> # Disable sampling
        >>> profile = analyze(df, sampling=False)

        >>> # Sample to 100,000 rows
        >>> profile = analyze(df, target_rows=100_000)

        >>> # Sample 10% of data
        >>> profile = analyze(df, fraction=0.1)

        >>> # Profile specific columns only
        >>> profile = analyze(df, columns=["age", "salary"])

        >>> # Get results as dictionary
        >>> profile = analyze(df, output_format="dict")
    """
    # Build sampling configuration based on parameters
    sampling_config = _build_sampling_config(
        sampling=sampling,
        target_rows=target_rows,
        fraction=fraction,
        auto_threshold=auto_threshold,
        seed=seed,
    )

    # Create profiler and generate profile
    profiler = DataFrameProfiler(
        df,
        optimize_for_large_datasets=optimize_for_large_datasets,
        sampling_config=sampling_config,
    )

    return profiler.profile(
        columns=columns,
        output_format=output_format,
        include_advanced=include_advanced,
        include_quality=include_quality,
    )


def _build_sampling_config(
    sampling: Optional[bool],
    target_rows: Optional[int],
    fraction: Optional[float],
    auto_threshold: Optional[int],
    seed: Optional[int],
) -> SamplingConfig:
    """
    Build SamplingConfig from simplified parameters.

    Args:
        sampling: Whether to enable sampling
        target_rows: Target number of rows to sample
        fraction: Fraction of data to sample
        auto_threshold: Custom auto-sampling threshold
        seed: Random seed

    Returns:
        SamplingConfig instance

    Raises:
        ValueError: If both target_rows and fraction are specified
    """
    if target_rows is not None and fraction is not None:
        raise ValueError("Cannot specify both target_rows and fraction")

    # If sampling is explicitly disabled
    if sampling is False:
        return SamplingConfig(enabled=False)

    # Build config with specified parameters
    config_params: dict[str, Any] = {}

    if sampling is not None:
        config_params["enabled"] = sampling

    if target_rows is not None:
        config_params["target_rows"] = target_rows

    if fraction is not None:
        config_params["fraction"] = fraction

    if auto_threshold is not None:
        config_params["auto_threshold"] = auto_threshold

    if seed is not None:
        config_params["seed"] = seed

    return SamplingConfig(**config_params)
