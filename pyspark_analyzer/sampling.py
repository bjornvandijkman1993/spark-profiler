"""
Simplified sampling configuration for DataFrame profiling.
"""

import time
import logging
from typing import Tuple, Optional
from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError, Py4JJavaError

from .exceptions import ConfigurationError, SamplingError, SparkOperationError

logger = logging.getLogger(__name__)


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
    sample_size: int
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
        try:
            row_count = df.count()
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(f"Failed to count DataFrame rows: {str(e)}")
            raise SparkOperationError(
                f"Failed to count DataFrame rows during sampling: {str(e)}", e
            )

    # Handle empty DataFrame
    if row_count == 0:
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
        sampling_fraction = 1.0
        should_sample = False
    elif config.target_rows is not None:
        # Explicit target rows specified
        sampling_fraction = min(1.0, config.target_rows / row_count)
        should_sample = sampling_fraction < 1.0
    elif config.fraction is not None:
        # Explicit fraction specified
        sampling_fraction = config.fraction
        should_sample = sampling_fraction < 1.0
    else:
        # Auto-sampling for large datasets (over 10M rows)
        if row_count > 10_000_000:
            # For large datasets, sample to the smaller of:
            # - 1M rows
            # - 10% of the original size
            target_rows = min(1_000_000, int(row_count * 0.1))
            sampling_fraction = min(1.0, target_rows / row_count)
            should_sample = sampling_fraction < 1.0
        else:
            sampling_fraction = 1.0
            should_sample = False

    # Apply sampling if needed
    if should_sample:
        try:
            sample_df = df.sample(fraction=sampling_fraction, seed=config.seed)
            sample_size = sample_df.count()
            is_sampled = True
        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(f"Failed to sample DataFrame: {str(e)}")
            raise SamplingError(
                f"Failed to sample DataFrame with fraction {sampling_fraction}: {str(e)}"
            )
    else:
        sample_df = df
        sample_size = row_count
        is_sampled = False

    metadata = SamplingMetadata(
        original_size=row_count,
        sample_size=sample_size,
        sampling_fraction=sampling_fraction,
        sampling_time=time.time() - start_time,
        is_sampled=is_sampled,
    )

    return sample_df, metadata
