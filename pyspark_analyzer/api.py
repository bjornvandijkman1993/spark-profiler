from typing import Optional, List, Union
import pandas as pd
from pyspark.sql import DataFrame

from .profiler import profile_dataframe
from .sampling import SamplingConfig
from .logging import get_logger

logger = get_logger(__name__)


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
    seed: Optional[int] = None,
) -> Union[pd.DataFrame, dict, str]:
    """
    Analyze a PySpark DataFrame and generate comprehensive statistics.

    This is the simplified entry point for profiling DataFrames. It automatically
    handles sampling configuration based on the provided parameters.

    Note: Compatible with PySpark 3.0.0+. Uses native median() function when available
    (PySpark 3.4.0+) for better performance, with automatic fallback to percentile_approx
    for older versions.

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
    logger.info(f"Starting DataFrame analysis with output_format={output_format}")

    # Build sampling configuration
    sampling_config = _build_sampling_config(
        sampling=sampling,
        target_rows=target_rows,
        fraction=fraction,
        seed=seed,
    )

    # Delegate to profiler
    try:
        result = profile_dataframe(
            dataframe=df,
            columns=columns,
            output_format=output_format,
            include_advanced=include_advanced,
            include_quality=include_quality,
            sampling_config=sampling_config,
        )
        logger.info("DataFrame analysis completed successfully")
        return result
    except Exception as e:
        logger.error(f"Error during DataFrame analysis: {str(e)}", exc_info=True)
        raise


def _build_sampling_config(
    sampling: Optional[bool],
    target_rows: Optional[int],
    fraction: Optional[float],
    seed: Optional[int],
) -> SamplingConfig:
    """
    Build SamplingConfig from simplified parameters.

    Args:
        sampling: Whether to enable sampling
        target_rows: Target number of rows to sample
        fraction: Fraction of data to sample
        seed: Random seed

    Returns:
        SamplingConfig instance
    """
    # If sampling is explicitly disabled
    if sampling is False:
        return SamplingConfig(enabled=False)

    # Let SamplingConfig handle validation
    return SamplingConfig(
        enabled=sampling if sampling is not None else True,
        target_rows=target_rows,
        fraction=fraction,
        seed=seed if seed is not None else 42,
    )
