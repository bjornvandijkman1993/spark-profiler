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

from pyspark_analyzer import ConfigurationError, analyze
from pyspark_analyzer.sampling import SamplingConfig
from pyspark_analyzer.sampling import SamplingMetadata, apply_sampling


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

    data = [
        (i, f"name_{i}", float(i * 1.5)) for i in range(50)
    ]  # Reduced from 100 to 50
    return spark_session.createDataFrame(data, schema)


class TestSamplingConfig:
    """Test cases for SamplingConfig class."""

    def test_default_config(self):
        """Test default sampling configuration."""
        config = SamplingConfig()
        assert config.enabled is True
        assert config.target_rows is None
        assert config.fraction is None
        assert config.seed == 42

    def test_custom_config(self):
        """Test custom sampling configuration."""
        config = SamplingConfig(enabled=False, target_rows=50000, seed=123)
        assert config.enabled is False
        assert config.target_rows == 50000
        assert config.seed == 123

    def test_config_validation_both_size_and_fraction(self):
        """Test validation when both target_rows and fraction are specified."""
        with pytest.raises(ConfigurationError, match="Cannot specify both"):
            SamplingConfig(target_rows=1000, fraction=0.1)

    def test_config_validation_invalid_fraction(self):
        """Test validation of invalid fraction."""
        with pytest.raises(ConfigurationError, match="fraction must be between"):
            SamplingConfig(fraction=1.5)

        with pytest.raises(ConfigurationError, match="fraction must be between"):
            SamplingConfig(fraction=-0.1)

    def test_config_validation_invalid_target_rows(self):
        """Test validation of invalid target_rows."""
        with pytest.raises(ConfigurationError, match="target_rows must be positive"):
            SamplingConfig(target_rows=-100)

        with pytest.raises(ConfigurationError, match="target_rows must be positive"):
            SamplingConfig(target_rows=0)


class TestApplySampling:
    """Test cases for apply_sampling function."""

    def test_no_sampling_needed(self, small_dataframe):
        """Test when no sampling is needed."""
        config = SamplingConfig()  # Default config - won't sample small datasets

        sample_df, metadata = apply_sampling(small_dataframe, config)

        assert metadata.is_sampled is False
        assert metadata.sampling_fraction == 1.0
        assert sample_df.count() == small_dataframe.count()

    def test_fraction_based_sampling(self, large_dataframe):
        """Test sampling with fraction."""
        config = SamplingConfig(fraction=0.1, seed=42)

        sample_df, metadata = apply_sampling(large_dataframe, config)

        assert metadata.is_sampled is True
        assert metadata.sampling_fraction == 0.1
        assert metadata.sample_size < metadata.original_size

    def test_size_based_sampling(self, large_dataframe):
        """Test sampling with target rows."""
        config = SamplingConfig(target_rows=5000, seed=42)

        sample_df, metadata = apply_sampling(large_dataframe, config)

        assert metadata.is_sampled is True
        assert metadata.sample_size <= 5500  # Allow variance in sampling
        assert metadata.sampling_fraction == 0.5  # 5000/10000

    def test_auto_sampling(self, large_dataframe):
        """Test automatic sampling decision."""
        # Note: auto-sampling only kicks in for datasets >10M rows
        # Since large_dataframe has 10k rows, it won't auto-sample
        config = SamplingConfig(enabled=True)

        sample_df, metadata = apply_sampling(large_dataframe, config)

        # Should not be sampled since it's under 10M rows
        assert metadata.is_sampled is False
        assert metadata.sample_size == metadata.original_size

    def test_sampling_disabled(self, large_dataframe):
        """Test when sampling is disabled."""
        config = SamplingConfig(enabled=False)

        sample_df, metadata = apply_sampling(large_dataframe, config)

        assert metadata.is_sampled is False
        assert metadata.sampling_fraction == 1.0
        assert metadata.sample_size == metadata.original_size

    def test_reproducible_sampling(self, large_dataframe):
        """Test that sampling is reproducible with same seed."""
        config = SamplingConfig(fraction=0.1, seed=42)

        sample1, metadata1 = apply_sampling(large_dataframe, config)
        sample2, metadata2 = apply_sampling(large_dataframe, config)

        # With same seed, should get identical results
        assert sample1.count() == sample2.count()

        # Both should be sampled
        assert metadata1.is_sampled is True
        assert metadata2.is_sampled is True

        # Both should use same fraction
        assert metadata1.sampling_fraction == 0.1
        assert metadata2.sampling_fraction == 0.1

    def test_empty_dataframe(self, spark_session):
        """Test sampling with empty DataFrame."""
        schema = StructType([StructField("id", IntegerType(), True)])
        empty_df = spark_session.createDataFrame([], schema)

        config = SamplingConfig(target_rows=100)
        sample_df, metadata = apply_sampling(empty_df, config)

        assert metadata.is_sampled is False
        assert metadata.original_size == 0
        assert metadata.sample_size == 0
        assert metadata.sampling_fraction == 1.0


class TestAnalyzeSampling:
    """Test cases for analyze function sampling integration."""

    def test_auto_sampling_large_dataset(self, large_dataframe):
        """Test auto-sampling with large dataset."""
        # Since large_dataframe has 10k rows and auto-sampling only kicks in at 10M rows,
        # we need to explicitly use fraction or target_rows to test sampling
        profile = analyze(large_dataframe, fraction=0.5, output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is True
        assert sampling_info["sample_size"] < sampling_info["original_size"]

    def test_no_sampling_small_dataset(self, small_dataframe):
        """Test no sampling with small dataset."""
        profile = analyze(small_dataframe, output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is False
        assert sampling_info["sample_size"] == sampling_info["original_size"]

    def test_custom_sampling_config(self, large_dataframe):
        """Test analyze with custom sampling config."""
        profile = analyze(large_dataframe, fraction=0.5, seed=123, output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is True
        # Allow some variance in random sampling
        assert sampling_info["sample_size"] <= 6000

    def test_disable_sampling(self, large_dataframe):
        """Test disabling sampling."""
        profile = analyze(large_dataframe, sampling=False, output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is False
        assert sampling_info["sample_size"] == sampling_info["original_size"]

    def test_sampling_with_optimization(self, large_dataframe):
        """Test sampling combined with performance optimization."""
        profile = analyze(large_dataframe, fraction=0.5, output_format="dict")

        sampling_info = profile["sampling"]
        assert sampling_info["is_sampled"] is True
        assert "columns" in profile
        assert len(profile["columns"]) > 0

    def test_profile_structure_with_sampling(self, large_dataframe):
        """Test that profile structure includes sampling information."""
        profile = analyze(large_dataframe, output_format="dict")

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
            sampling_time=1.5,
            is_sampled=True,
        )

        assert metadata.speedup_estimate == 10.0

    def test_metadata_edge_cases(self):
        """Test SamplingMetadata edge cases."""
        # Zero original size
        metadata = SamplingMetadata(
            original_size=0,
            sample_size=0,
            sampling_fraction=1.0,
            sampling_time=0.0,
            is_sampled=False,
        )

        assert metadata.speedup_estimate == 1.0

        # Not sampled
        metadata = SamplingMetadata(
            original_size=1000,
            sample_size=1000,
            sampling_fraction=1.0,
            sampling_time=0.0,
            is_sampled=False,
        )

        assert metadata.speedup_estimate == 1.0
