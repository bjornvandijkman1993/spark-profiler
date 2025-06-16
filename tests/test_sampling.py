"""
Test cases for sampling functionality.
"""

import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from pyspark_analyzer import DataFrameProfiler, SamplingConfig, create_sampling_config
from pyspark_analyzer.sampling import (
    RandomSamplingStrategy,
    SamplingDecisionEngine,
    SamplingMetadata,
)


@pytest.fixture
def small_dataframe(spark_session):
    """Create a small DataFrame that shouldn't trigger sampling."""
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )

    data = [(i, f"name_{i}", float(i * 1.5)) for i in range(100)]
    return spark_session.createDataFrame(data, schema)


class TestSamplingConfig:
    """Test cases for SamplingConfig class."""

    def test_default_config(self):
        """Test default sampling configuration."""
        config = SamplingConfig()
        assert config.strategy == "random"
        assert config.target_size is None
        assert config.target_fraction is None
        assert config.auto_sample is True
        assert config.seed == 42

    def test_custom_config(self):
        """Test custom sampling configuration."""
        config = SamplingConfig(target_size=50000, seed=123, auto_sample=False)
        assert config.target_size == 50000
        assert config.seed == 123
        assert config.auto_sample is False

    def test_config_validation_both_size_and_fraction(self):
        """Test validation when both target_size and target_fraction are specified."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            SamplingConfig(target_size=1000, target_fraction=0.1)

    def test_config_validation_invalid_fraction(self):
        """Test validation of invalid target_fraction."""
        with pytest.raises(ValueError, match="target_fraction must be between"):
            SamplingConfig(target_fraction=1.5)

        with pytest.raises(ValueError, match="target_fraction must be between"):
            SamplingConfig(target_fraction=-0.1)

    def test_config_validation_small_target_size(self):
        """Test validation of too small target_size."""
        with pytest.raises(ValueError, match="target_size must be"):
            SamplingConfig(target_size=100)  # Below min_sample_size

    def test_create_sampling_config_helper(self):
        """Test the convenience function for creating sampling config."""
        config = create_sampling_config(target_size=25000, seed=456)
        assert config.target_size == 25000
        assert config.seed == 456


class TestRandomSamplingStrategy:
    """Test cases for RandomSamplingStrategy class."""

    def test_no_sampling_needed(self, small_dataframe):
        """Test when no sampling is needed."""
        strategy = RandomSamplingStrategy()
        config = SamplingConfig(
            performance_threshold=1000
        )  # Higher than DataFrame size

        sample_df, metadata = strategy.sample(small_dataframe, config)

        assert metadata.is_sampled is False
        assert metadata.sampling_fraction == 1.0
        assert metadata.quality_score == 1.0
        assert sample_df.count() == small_dataframe.count()

    def test_fraction_based_sampling(self, large_dataframe):
        """Test sampling with target fraction."""
        strategy = RandomSamplingStrategy()
        config = SamplingConfig(target_fraction=0.1, seed=42)

        sample_df, metadata = strategy.sample(large_dataframe, config)

        assert metadata.is_sampled is True
        assert metadata.sampling_fraction == 0.1
        assert metadata.sample_size < metadata.original_size
        assert metadata.quality_score > 0

    def test_size_based_sampling(self, large_dataframe):
        """Test sampling with target size."""
        strategy = RandomSamplingStrategy()
        config = SamplingConfig(target_size=10000, seed=42)

        sample_df, metadata = strategy.sample(large_dataframe, config)

        assert metadata.is_sampled is True
        assert metadata.sample_size <= 10000
        assert metadata.sampling_fraction < 1.0

    def test_auto_sampling(self, large_dataframe):
        """Test automatic sampling decision."""
        strategy = RandomSamplingStrategy()
        config = SamplingConfig(auto_sample=True, performance_threshold=50000)

        sample_df, metadata = strategy.sample(large_dataframe, config)

        assert metadata.is_sampled is True
        assert metadata.sample_size < metadata.original_size

    def test_quality_estimation(self, large_dataframe):
        """Test quality estimation for samples."""
        strategy = RandomSamplingStrategy()

        # Large sample should have high quality
        config_large = SamplingConfig(target_size=50000)
        _, metadata_large = strategy.sample(large_dataframe, config_large)

        # Small sample should have lower quality
        config_small = SamplingConfig(target_size=10000)
        _, metadata_small = strategy.sample(large_dataframe, config_small)

        assert metadata_large.quality_score > metadata_small.quality_score

    def test_reproducible_sampling(self, large_dataframe):
        """Test that sampling is reproducible with same seed."""
        strategy = RandomSamplingStrategy()
        config = SamplingConfig(target_fraction=0.1, seed=42)

        sample1, _ = strategy.sample(large_dataframe, config)
        sample2, _ = strategy.sample(large_dataframe, config)

        # Should get same number of rows (though content might vary due to Spark's sampling)
        assert sample1.count() == sample2.count()


class TestSamplingDecisionEngine:
    """Test cases for SamplingDecisionEngine class."""

    def test_should_sample_with_auto_enabled(self, large_dataframe):
        """Test sampling decision with auto sampling enabled."""
        config = SamplingConfig(auto_sample=True, performance_threshold=50000)
        engine = SamplingDecisionEngine(config)

        assert engine.should_sample(large_dataframe) is True

    def test_should_not_sample_small_dataset(self, small_dataframe):
        """Test that small datasets are not sampled automatically."""
        config = SamplingConfig(auto_sample=True, performance_threshold=1000)
        engine = SamplingDecisionEngine(config)

        assert engine.should_sample(small_dataframe) is False

    def test_should_sample_with_explicit_config(self, small_dataframe):
        """Test that explicit sampling config overrides auto decision."""
        config = SamplingConfig(target_size=10000, auto_sample=False)
        engine = SamplingDecisionEngine(config)

        assert engine.should_sample(small_dataframe) is True

    def test_create_sample(self, large_dataframe):
        """Test sample creation through engine."""
        config = SamplingConfig(target_size=10000)
        engine = SamplingDecisionEngine(config)

        sample_df, metadata = engine.create_sample(large_dataframe)

        assert isinstance(metadata, SamplingMetadata)
        assert sample_df.count() <= 10000


class TestDataFrameProfilerSampling:
    """Test cases for DataFrameProfiler sampling integration."""

    def test_auto_sampling_large_dataset(self, large_dataframe):
        """Test auto-sampling with large dataset."""
        # Use a lower threshold to trigger sampling
        config = SamplingConfig(performance_threshold=50000)
        profiler = DataFrameProfiler(large_dataframe, sampling_config=config)
        profile = profiler.profile(output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is True
        assert sampling_info["sample_size"] < sampling_info["original_size"]
        assert "quality_score" in sampling_info

    def test_no_sampling_small_dataset(self, small_dataframe):
        """Test no sampling with small dataset."""
        profiler = DataFrameProfiler(small_dataframe)
        profile = profiler.profile(output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is False
        assert sampling_info["sample_size"] == sampling_info["original_size"]

    def test_custom_sampling_config(self, large_dataframe):
        """Test DataFrameProfiler with custom sampling config."""
        config = SamplingConfig(target_size=15000, seed=123, auto_sample=False)
        profiler = DataFrameProfiler(large_dataframe, sampling_config=config)
        profile = profiler.profile(output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is True
        # Allow some variance in random sampling
        assert sampling_info["sample_size"] <= 16000

    def test_legacy_sample_fraction(self, large_dataframe):
        """Test legacy sample_fraction parameter."""
        profiler = DataFrameProfiler(large_dataframe, sample_fraction=0.05)
        profile = profiler.profile(output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is True
        assert sampling_info["sampling_fraction"] == 0.05

    def test_conflicting_parameters(self, large_dataframe):
        """Test error when both legacy and new parameters are provided."""
        config = SamplingConfig(target_size=10000)

        with pytest.raises(ValueError, match="Cannot specify both"):
            DataFrameProfiler(
                large_dataframe, sample_fraction=0.1, sampling_config=config
            )

    def test_sampling_with_optimization(self, large_dataframe):
        """Test sampling combined with performance optimization."""
        config = SamplingConfig(target_size=10000, auto_sample=False)
        profiler = DataFrameProfiler(
            large_dataframe, sampling_config=config, optimize_for_large_datasets=True
        )
        profile = profiler.profile(output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is True
        assert "columns" in profile
        assert len(profile["columns"]) > 0

    def test_profile_structure_with_sampling(self, large_dataframe):
        """Test that profile structure includes sampling information."""
        profiler = DataFrameProfiler(large_dataframe)
        profile = profiler.profile(output_format="dict")

        # Check required keys
        assert "overview" in profile
        assert "columns" in profile
        assert "sampling" in profile

        # Check sampling info structure
        sampling = profile["sampling"]
        required_keys = [
            "is_sampled",
            "original_size",
            "sample_size",
            "sampling_fraction",
            "strategy_used",
            "quality_score",
        ]
        for key in required_keys:
            assert key in sampling


class TestSamplingMetadata:
    """Test cases for SamplingMetadata class."""

    def test_metadata_properties(self):
        """Test SamplingMetadata properties."""
        metadata = SamplingMetadata(
            original_size=100000,
            sample_size=10000,
            sampling_fraction=0.1,
            strategy_used="random",
            sampling_time=1.5,
            quality_score=0.85,
            is_sampled=True,
        )

        assert metadata.reduction_ratio == 0.1
        assert metadata.speedup_estimate == 10.0

    def test_metadata_edge_cases(self):
        """Test SamplingMetadata edge cases."""
        # Zero original size
        metadata = SamplingMetadata(
            original_size=0,
            sample_size=0,
            sampling_fraction=0.0,
            strategy_used="none",
            sampling_time=0.0,
            quality_score=1.0,
            is_sampled=False,
        )

        assert metadata.reduction_ratio == 0.0
        assert metadata.speedup_estimate == 1.0
