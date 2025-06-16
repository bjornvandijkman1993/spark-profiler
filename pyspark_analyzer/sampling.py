"""
Sampling strategies and configuration for DataFrame profiling.
"""

import time
from abc import ABC, abstractmethod
from typing import Tuple
from typing import Optional
from dataclasses import dataclass
from pyspark.sql import DataFrame


@dataclass
class SamplingConfig:
    """Configuration for sampling operations."""

    strategy: str = "random"
    target_size: Optional[int] = None
    target_fraction: Optional[float] = None
    max_sample_size: int = 1_000_000
    min_sample_size: int = 10_000
    seed: int = 42
    auto_sample: bool = True
    performance_threshold: int = 10_000_000

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.target_size and self.target_fraction:
            raise ValueError("Cannot specify both target_size and target_fraction")

        if self.target_size and self.target_size < self.min_sample_size:
            raise ValueError(f"target_size must be >= {self.min_sample_size}")

        if self.target_fraction is not None and not (0 < self.target_fraction <= 1.0):
            raise ValueError("target_fraction must be between 0 and 1")


@dataclass
class SamplingMetadata:
    """Metadata about sampling operation."""

    original_size: int
    sample_size: int
    sampling_fraction: float
    strategy_used: str
    sampling_time: float
    quality_score: float
    is_sampled: bool

    @property
    def reduction_ratio(self) -> float:
        """Calculate data reduction ratio."""
        if self.original_size > 0:
            return self.sample_size / self.original_size
        return 0.0 if self.sample_size == 0 else 1.0

    @property
    def speedup_estimate(self) -> float:
        """Estimate processing speedup from sampling."""
        if self.reduction_ratio > 0 and self.reduction_ratio < 1.0:
            return 1.0 / self.reduction_ratio
        return 1.0


class SamplingStrategy(ABC):
    """Abstract base class for sampling strategies."""

    @abstractmethod
    def sample(
        self, df: DataFrame, config: SamplingConfig, original_size: Optional[int] = None
    ) -> Tuple[DataFrame, SamplingMetadata]:
        """Sample the DataFrame and return sample with metadata."""
        pass

    @abstractmethod
    def estimate_quality(self, original_df: DataFrame, sample_df: DataFrame) -> float:
        """Estimate how well the sample represents the original data."""
        pass


class RandomSamplingStrategy(SamplingStrategy):
    """Random sampling strategy with quality estimation."""

    def sample(
        self, df: DataFrame, config: SamplingConfig, original_size: Optional[int] = None
    ) -> Tuple[DataFrame, SamplingMetadata]:
        """Perform random sampling on the DataFrame."""
        start_time = time.time()

        # Get original size
        if original_size is None:
            original_size = df.count()

        # Determine sampling fraction
        sampling_fraction = self._calculate_sampling_fraction(original_size, config)

        # Perform sampling if needed
        if sampling_fraction >= 1.0:
            # No sampling needed
            sample_df = df
            sample_size = original_size
            is_sampled = False
            quality_score = 1.0
        else:
            # Sample the data
            sample_df = df.sample(fraction=sampling_fraction, seed=config.seed)
            sample_size = sample_df.count()
            is_sampled = True

            # Estimate quality (simplified for performance)
            quality_score = self._estimate_quality_fast(
                original_size, sample_size, sampling_fraction
            )

        sampling_time = time.time() - start_time

        metadata = SamplingMetadata(
            original_size=original_size,
            sample_size=sample_size,
            sampling_fraction=sampling_fraction,
            strategy_used="random",
            sampling_time=sampling_time,
            quality_score=quality_score,
            is_sampled=is_sampled,
        )

        return sample_df, metadata

    def estimate_quality(self, original_df: DataFrame, sample_df: DataFrame) -> float:
        """Estimate sampling quality using statistical measures."""
        # For random sampling, quality is primarily based on sample size
        # More sophisticated quality estimation could be added here
        original_size = original_df.count()
        sample_size = sample_df.count()

        if sample_size >= original_size:
            return 1.0

        # Simple quality estimate based on sample size
        # Real implementation could include distribution comparisons
        sample_ratio = sample_size / original_size

        # Quality score based on statistical power
        # Larger samples generally provide better representation
        if sample_size >= 100_000:
            return float(min(0.98, 0.7 + sample_ratio * 0.3))
        elif sample_size >= 10_000:
            return float(min(0.90, 0.6 + sample_ratio * 0.3))
        else:
            return float(min(0.80, 0.4 + sample_ratio * 0.4))

    def _calculate_sampling_fraction(
        self, original_size: int, config: SamplingConfig
    ) -> float:
        """Calculate the sampling fraction based on configuration."""
        if config.target_fraction:
            return config.target_fraction

        if config.target_size and original_size > 0:
            return min(1.0, config.target_size / original_size)

        # Auto-determine sampling fraction
        if not config.auto_sample or original_size <= config.performance_threshold:
            return 1.0  # No sampling needed

        # Calculate fraction to get reasonable sample size
        target_size = min(
            config.max_sample_size,
            max(config.min_sample_size, int(original_size * 0.01)),
        )
        return target_size / original_size if original_size > 0 else 1.0

    def _estimate_quality_fast(
        self, original_size: int, sample_size: int, fraction: float
    ) -> float:
        """Fast quality estimation without additional data scans."""
        if sample_size >= original_size:
            return 1.0

        # Quality estimate based on sample size and fraction
        # This is a simplified heuristic that avoids expensive computations
        size_score = min(1.0, sample_size / 100_000)  # Reward larger samples
        fraction_score = min(1.0, fraction * 10)  # Reward higher fractions

        # Combine scores with weights
        quality = 0.6 * size_score + 0.4 * fraction_score

        # Ensure minimum quality for reasonable sample sizes
        if sample_size >= 10_000:
            quality = max(0.85, quality)
        elif sample_size >= 1_000:
            quality = max(0.70, quality)

        return min(0.98, quality)


class SamplingDecisionEngine:
    """Engine to decide whether and how to sample data."""

    def __init__(self, config: SamplingConfig):
        self.config = config
        self.strategy = RandomSamplingStrategy()

    def should_sample(self, df: DataFrame, row_count: Optional[int] = None) -> bool:
        """Determine if sampling is beneficial for the given DataFrame."""
        # Always sample if explicit targets are set
        if (
            self.config.target_size is not None
            or self.config.target_fraction is not None
        ):
            return True

        # If auto-sampling is disabled and no explicit targets, don't sample
        if not self.config.auto_sample:
            return False

        # Get DataFrame size for auto-sampling decision
        if row_count is None:
            row_count = df.count()

        # Sample if above performance threshold
        return bool(row_count > self.config.performance_threshold)

    def create_sample(
        self, df: DataFrame, original_size: Optional[int] = None
    ) -> Tuple[DataFrame, SamplingMetadata]:
        """Create a sample of the DataFrame with metadata."""
        return self.strategy.sample(df, self.config, original_size=original_size)

    def recommend_config(
        self, df: DataFrame, use_case: str = "balanced"
    ) -> SamplingConfig:
        """Recommend sampling configuration based on use case."""
        row_count = df.count()

        if use_case == "fast":
            # Fast exploration - smaller samples
            return SamplingConfig(
                target_fraction=0.001 if row_count > 1_000_000 else 0.01,
                auto_sample=True,
                performance_threshold=100_000,
            )
        elif use_case == "accurate":
            # High accuracy - larger samples
            return SamplingConfig(
                max_sample_size=5_000_000,
                min_sample_size=100_000,
                auto_sample=True,
                performance_threshold=5_000_000,
            )
        else:  # balanced
            # Default balanced approach
            return SamplingConfig()


def create_sampling_config(
    strategy: str = "random",
    target_size: Optional[int] = None,
    target_fraction: Optional[float] = None,
    auto_sample: bool = True,
    seed: int = 42,
) -> SamplingConfig:
    """Convenience function to create sampling configuration."""
    return SamplingConfig(
        strategy=strategy,
        target_size=target_size,
        target_fraction=target_fraction,
        auto_sample=auto_sample,
        seed=seed,
    )
