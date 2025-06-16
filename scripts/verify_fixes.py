#!/usr/bin/env python3
"""
Verify the fixes made to the pyspark-analyzer without requiring Java/Spark.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_sampling_config():
    """Test SamplingConfig doesn't have confidence_level."""
    import pyspark_analyzer.sampling

    SamplingConfig = pyspark_analyzer.sampling.SamplingConfig

    config = SamplingConfig(target_size=50_000, min_sample_size=10_000, seed=42)

    # Verify attributes exist
    assert hasattr(config, "target_size")
    assert hasattr(config, "min_sample_size")
    assert hasattr(config, "seed")

    # Verify removed attributes don't exist
    assert not hasattr(config, "confidence_level")
    assert not hasattr(config, "relative_error")

    print("✓ SamplingConfig test passed")


def test_profiler_methods():
    """Test DataFrameProfiler has profile method."""
    import pyspark_analyzer.profiler

    DataFrameProfiler = pyspark_analyzer.profiler.DataFrameProfiler

    # Check method exists
    assert hasattr(DataFrameProfiler, "profile")

    print("✓ DataFrameProfiler methods test passed")


def test_imports():
    """Test all modules can be imported."""
    try:
        import pyspark_analyzer
        import pyspark_analyzer.profiler  # noqa: F401
        import pyspark_analyzer.statistics  # noqa: F401
        import pyspark_analyzer.performance  # noqa: F401
        import pyspark_analyzer.utils  # noqa: F401
        import pyspark_analyzer.sampling  # noqa: F401

        print("✓ All imports successful")
    except ImportError as e:
        print(f"✗ Import error: {e}")
        return False
    return True


def test_sampling_metadata():
    """Test SamplingMetadata structure."""
    import pyspark_analyzer.sampling

    SamplingMetadata = pyspark_analyzer.sampling.SamplingMetadata

    metadata = SamplingMetadata(
        original_size=100000,
        sample_size=10000,
        sampling_fraction=0.1,
        strategy_used="random",
        sampling_time=1.5,
        quality_score=0.85,
        is_sampled=True,
    )

    # Test properties
    assert metadata.reduction_ratio == 0.1
    assert metadata.speedup_estimate == 10.0
    assert metadata.is_sampled is True  # Not was_sampled

    print("✓ SamplingMetadata test passed")


def main():
    """Run all verification tests."""
    print("Verifying pyspark-analyzer fixes...")
    print("-" * 50)

    tests = [
        test_imports,
        test_sampling_config,
        test_profiler_methods,
        test_sampling_metadata,
    ]

    all_passed = True
    for test in tests:
        try:
            result = test()
            if result is False:
                all_passed = False
        except Exception as e:
            print(f"✗ {test.__name__} failed: {e}")
            all_passed = False

    print("-" * 50)
    if all_passed:
        print("✅ All verification tests passed!")
    else:
        print("❌ Some tests failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
