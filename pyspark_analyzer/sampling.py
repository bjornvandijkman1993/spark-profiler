"""
Simplified sampling configuration for DataFrame profiling.
"""

import time
from typing import Tuple, Optional
from dataclasses import dataclass
from pyspark.sql import DataFrame


@dataclass
class SamplingConfig:
    """
    Configuration for sampling operations.

    Attributes:
        enabled: Whether to enable sampling. Set to False to disable sampling completely.
        target_rows: Target number of rows to sample. Takes precedence over fraction.
        fraction: Fraction of data to sample (0-1). Only used if target_rows is not set.
        seed: Random seed for reproducible sampling.
        auto_threshold: Row count threshold above which auto-sampling kicks in (if enabled=True).
    """

    enabled: bool = True
    target_rows: Optional[int] = None
    fraction: Optional[float] = None
    seed: int = 42
    auto_threshold: int = 10_000_000

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.target_rows is not None and self.target_rows <= 0:
            raise ValueError("target_rows must be positive")

        if self.fraction is not None:
            if not (0 < self.fraction <= 1.0):
                raise ValueError("fraction must be between 0 and 1")

        if self.target_rows is not None and self.fraction is not None:
            raise ValueError("Cannot specify both target_rows and fraction")


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
        row_count = df.count()

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
        # Auto-sampling based on threshold
        if row_count > config.auto_threshold:
            # For datasets over the threshold, sample to the smaller of:
            # - 1M rows
            # - 10% of the original size
            # - The auto_threshold itself
            target_rows = min(1_000_000, int(row_count * 0.1), config.auto_threshold)
            sampling_fraction = min(1.0, target_rows / row_count)
            should_sample = sampling_fraction < 1.0
        else:
            sampling_fraction = 1.0
            should_sample = False

    # Apply sampling if needed
    if should_sample:
        sample_df = df.sample(fraction=sampling_fraction, seed=config.seed)
        sample_size = sample_df.count()
        is_sampled = True
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
