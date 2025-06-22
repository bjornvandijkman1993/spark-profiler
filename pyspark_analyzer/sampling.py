"""
Simplified sampling configuration for DataFrame profiling.
"""

import time
from typing import Tuple, Optional
from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError, Py4JJavaError

from .exceptions import ConfigurationError, SamplingError, SparkOperationError
from .logging import get_logger

logger = get_logger(__name__)


@dataclass
class SamplingConfig:
    """
    Configuration for sampling operations.

    Attributes:
        enabled: Whether to enable sampling. Set to False to disable sampling completely.
        target_rows: Target number of rows to sample. Takes precedence over fraction.
        fraction: Fraction of data to sample (0-1). Only used if target_rows is not set.
        seed: Random seed for reproducible sampling.
    """

    enabled: bool = True
    target_rows: Optional[int] = None
    fraction: Optional[float] = None
    seed: int = 42

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.target_rows is not None and self.target_rows <= 0:
            raise ConfigurationError("target_rows must be positive")

        if self.fraction is not None:
            if not (0 < self.fraction <= 1.0):
                raise ConfigurationError("fraction must be between 0 and 1")

        if self.target_rows is not None and self.fraction is not None:
            raise ConfigurationError("Cannot specify both target_rows and fraction")


@dataclass
class SamplingMetadata:
    """Metadata about a sampling operation."""

    original_size: int
    sample_size: int  # Estimated when sampling is applied
    sampling_fraction: float
    sampling_time: float
    is_sampled: bool

    @property
    def speedup_estimate(self) -> float:
        """Estimate processing speedup from sampling."""
        if self.is_sampled and self.sampling_fraction > 0:
            return 1.0 / self.sampling_fraction
        return 1.0


def apply_sampling(
    df: DataFrame, config: SamplingConfig, row_count: Optional[int] = None
) -> Tuple[DataFrame, SamplingMetadata]:
    """
    Apply sampling to a DataFrame based on configuration.

    Args:
        df: DataFrame to potentially sample
        config: Sampling configuration
        row_count: Pre-computed row count (optional, to avoid redundant counts)

    Returns:
        Tuple of (sampled DataFrame, sampling metadata)
    """
    start_time = time.time()

    # Get row count if not provided
    if row_count is None:
        logger.debug("Computing row count for sampling decision")
        try:
            row_count = df.count()
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(f"Failed to count DataFrame rows: {str(e)}")
            raise SparkOperationError(
                f"Failed to count DataFrame rows during sampling: {str(e)}", e
            )

    logger.debug(f"DataFrame has {row_count:,} rows")

    # Handle empty DataFrame
    if row_count == 0:
        logger.warning("DataFrame is empty, no sampling needed")
        return df, SamplingMetadata(
            original_size=0,
            sample_size=0,
            sampling_fraction=1.0,
            sampling_time=time.time() - start_time,
            is_sampled=False,
        )

    # Determine if sampling should be applied
    if not config.enabled:
        # Sampling disabled
        logger.info("Sampling is disabled by configuration")
        sampling_fraction = 1.0
        should_sample = False
    elif config.target_rows is not None:
        # Explicit target rows specified
        sampling_fraction = min(1.0, config.target_rows / row_count)
        should_sample = sampling_fraction < 1.0
        logger.info(
            f"Target rows sampling: {config.target_rows:,} rows (fraction: {sampling_fraction:.4f})"
        )
    elif config.fraction is not None:
        # Explicit fraction specified
        sampling_fraction = config.fraction
        should_sample = sampling_fraction < 1.0
        logger.info(f"Fraction-based sampling: {sampling_fraction:.4f}")
    else:
        # Auto-sampling for large datasets (over 10M rows)
        if row_count > 10_000_000:
            # For large datasets, sample to the smaller of:
            # - 1M rows
            # - 10% of the original size
            target_rows = min(1_000_000, int(row_count * 0.1))
            sampling_fraction = min(1.0, target_rows / row_count)
            should_sample = sampling_fraction < 1.0
            logger.info(
                f"Auto-sampling triggered for large dataset: {row_count:,} rows -> "
                f"{target_rows:,} rows (fraction: {sampling_fraction:.4f})"
            )
        else:
            sampling_fraction = 1.0
            should_sample = False
            logger.debug(f"No auto-sampling needed for {row_count:,} rows")

    # Apply sampling if needed
    if should_sample:
        logger.debug(
            f"Applying sampling with fraction {sampling_fraction:.4f} and seed {config.seed}"
        )
        try:
            sample_df = df.sample(fraction=sampling_fraction, seed=config.seed)
            # Estimate sample size instead of counting to avoid extra collection
            sample_size = int(row_count * sampling_fraction)
            is_sampled = True
            logger.info(
                f"Sampling completed: {row_count:,} -> ~{sample_size:,} rows (estimated)"
            )
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(f"Failed to sample DataFrame: {str(e)}")
            raise SamplingError(
                f"Failed to sample DataFrame with fraction {sampling_fraction}: {str(e)}"
            )
    else:
        sample_df = df
        sample_size = row_count
        is_sampled = False
        logger.debug("No sampling applied, using full dataset")

    sampling_time = time.time() - start_time
    metadata = SamplingMetadata(
        original_size=row_count,
        sample_size=sample_size,
        sampling_fraction=sampling_fraction,
        sampling_time=sampling_time,
        is_sampled=is_sampled,
    )

    if is_sampled:
        logger.debug(
            f"Sampling metadata: speedup estimate = {metadata.speedup_estimate:.2f}x, "
            f"time = {sampling_time:.2f}s"
        )

    return sample_df, metadata
