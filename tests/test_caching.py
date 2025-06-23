"""
Test cases for the caching functionality.
"""

import time
from unittest.mock import Mock, patch

import pytest
from pyspark import StorageLevel
from pyspark.sql.utils import AnalysisException

from pyspark_analyzer.caching import (
    CacheEntry,
    CacheManager,
    CacheStatistics,
)
from pyspark_analyzer.exceptions import SparkOperationError
from pyspark_analyzer.sampling import SamplingMetadata


class TestCacheStatistics:
    """Test cases for CacheStatistics tracking."""

    def test_initial_statistics(self):
        """Test initial statistics values."""
        stats = CacheStatistics()
        assert stats.total_requests == 0
        assert stats.cache_hits == 0
        assert stats.cache_misses == 0
        assert stats.total_cached_bytes == 0
        assert stats.evictions == 0
        assert stats.hit_rate == 0.0

    def test_record_hit(self):
        """Test recording cache hits."""
        stats = CacheStatistics()
        stats.record_hit()
        assert stats.total_requests == 1
        assert stats.cache_hits == 1
        assert stats.cache_misses == 0
        assert stats.hit_rate == 1.0

    def test_record_miss(self):
        """Test recording cache misses."""
        stats = CacheStatistics()
        stats.record_miss()
        assert stats.total_requests == 1
        assert stats.cache_hits == 0
        assert stats.cache_misses == 1
        assert stats.hit_rate == 0.0

    def test_hit_rate_calculation(self):
        """Test hit rate calculation with mixed hits/misses."""
        stats = CacheStatistics()
        stats.record_hit()
        stats.record_hit()
        stats.record_miss()
        stats.record_hit()
        assert stats.total_requests == 4
        assert stats.cache_hits == 3
        assert stats.cache_misses == 1
        assert stats.hit_rate == 0.75


class TestCacheEntry:
    """Test cases for CacheEntry dataclass."""

    def test_cache_entry_creation(self, sample_dataframe):
        """Test creating a cache entry."""
        entry = CacheEntry(
            dataframe=sample_dataframe,
            cache_key="test_key",
            row_count=1000,
            storage_level=StorageLevel.MEMORY_AND_DISK,
        )
        assert entry.dataframe is sample_dataframe
        assert entry.cache_key == "test_key"
        assert entry.row_count == 1000
        assert entry.storage_level == StorageLevel.MEMORY_AND_DISK
        assert entry.access_count == 0
        assert entry.is_persisted is False

    def test_cache_entry_access(self, sample_dataframe):
        """Test updating access statistics."""
        entry = CacheEntry(
            dataframe=sample_dataframe,
            cache_key="test_key",
            row_count=1000,
            storage_level=StorageLevel.MEMORY_AND_DISK,
        )
        initial_time = entry.last_accessed
        time.sleep(0.01)  # Small delay to ensure time difference

        entry.access()
        assert entry.access_count == 1
        assert entry.last_accessed > initial_time

        entry.access()
        assert entry.access_count == 2


class TestCacheManager:
    """Test cases for CacheManager."""

    def test_cache_manager_initialization(self):
        """Test CacheManager initialization."""
        manager = CacheManager()
        assert len(manager._cache) == 0
        assert manager._stats is not None

    def test_generate_cache_key(self, sample_dataframe):
        """Test cache key generation."""
        manager = CacheManager()

        # Without sampling metadata
        key1 = manager.generate_cache_key(sample_dataframe)
        assert isinstance(key1, str)
        assert len(key1) == 32  # MD5 hash length

        # With sampling metadata
        sampling_meta = SamplingMetadata(
            original_size=1000000,
            sample_size=100000,
            sampling_fraction=0.1,
            sampling_time=1.0,
            is_sampled=True,
        )
        key2 = manager.generate_cache_key(sample_dataframe, sampling_meta)
        assert isinstance(key2, str)
        assert key1 != key2  # Keys should be different with different metadata

    # Removed test_should_cache_disabled since caching is always enabled now

    def test_should_cache_small_dataframe(self, sample_dataframe):
        """Test should_cache with small DataFrame."""
        manager = CacheManager()
        assert manager.should_cache(sample_dataframe, 100) is False

    def test_should_cache_large_dataframe(self, sample_dataframe):
        """Test should_cache with large DataFrame."""
        manager = CacheManager()
        assert manager.should_cache(sample_dataframe, 2_000_000) is True

    def test_should_cache_force(self, sample_dataframe):
        """Test should_cache with force flag."""
        manager = CacheManager()
        assert manager.should_cache(sample_dataframe, 100, force=True) is True

    @patch("pyspark_analyzer.caching.logger")
    def test_cache_dataframe_success(self, mock_logger, sample_dataframe):
        """Test successful DataFrame caching."""
        manager = CacheManager()

        # Mock the persist operation
        sample_dataframe.persist = Mock(return_value=sample_dataframe)
        sample_dataframe.count = Mock(return_value=5)

        cached_df, cache_key = manager.cache_dataframe(sample_dataframe, row_count=5)

        # Verify persist was called
        sample_dataframe.persist.assert_called_once()

        # Verify cache entry was created
        assert cache_key in manager._cache
        entry = manager._cache[cache_key]
        assert entry.dataframe is sample_dataframe
        assert entry.row_count == 5
        assert entry.is_persisted is True

        # Verify statistics were updated
        assert manager._stats.cache_misses == 1

    def test_cache_dataframe_already_cached(self, sample_dataframe):
        """Test caching an already cached DataFrame."""
        manager = CacheManager()

        # Mock the persist operation
        sample_dataframe.persist = Mock(return_value=sample_dataframe)
        sample_dataframe.count = Mock(return_value=5)

        # Cache the DataFrame
        _, cache_key1 = manager.cache_dataframe(sample_dataframe, row_count=5)

        # Try to cache again with same key
        _, cache_key2 = manager.cache_dataframe(
            sample_dataframe, cache_key=cache_key1, row_count=5
        )

        # Should return same key and record a hit
        assert cache_key1 == cache_key2
        assert manager._stats.cache_hits == 1
        assert manager._stats.cache_misses == 1

    def test_cache_dataframe_error(self, sample_dataframe):
        """Test error handling during caching."""
        manager = CacheManager()

        # Mock persist to raise an error
        sample_dataframe.persist = Mock(side_effect=AnalysisException("Test error", []))

        with pytest.raises(SparkOperationError):
            manager.cache_dataframe(sample_dataframe)

    def test_get_cached_hit(self, sample_dataframe):
        """Test retrieving a cached DataFrame."""
        manager = CacheManager()

        # Add entry to cache
        cache_key = "test_key"
        entry = CacheEntry(
            dataframe=sample_dataframe,
            cache_key=cache_key,
            row_count=5,
            storage_level=StorageLevel.MEMORY_AND_DISK,
        )
        manager._cache[cache_key] = entry

        # Retrieve from cache
        cached_df = manager.get_cached(cache_key)
        assert cached_df is sample_dataframe
        assert entry.access_count == 1
        assert manager._stats.cache_hits == 1

    def test_get_cached_miss(self):
        """Test cache miss when retrieving."""
        manager = CacheManager()
        cached_df = manager.get_cached("non_existent_key")
        assert cached_df is None
        assert manager._stats.cache_misses == 1

    def test_unpersist_success(self, sample_dataframe):
        """Test successful unpersist operation."""
        manager = CacheManager()

        # Add entry to cache
        cache_key = "test_key"
        sample_dataframe.unpersist = Mock()
        entry = CacheEntry(
            dataframe=sample_dataframe,
            cache_key=cache_key,
            row_count=5,
            storage_level=StorageLevel.MEMORY_AND_DISK,
            is_persisted=True,
        )
        manager._cache[cache_key] = entry

        # Unpersist
        result = manager.unpersist(cache_key)
        assert result is True
        assert cache_key not in manager._cache
        sample_dataframe.unpersist.assert_called_once_with(blocking=False)

    def test_unpersist_not_found(self):
        """Test unpersist with non-existent key."""
        manager = CacheManager()
        result = manager.unpersist("non_existent_key")
        assert result is False

    def test_unpersist_all(self, sample_dataframe):
        """Test unpersisting all cached DataFrames."""
        manager = CacheManager()

        # Add multiple entries
        for i in range(3):
            cache_key = f"key_{i}"
            df_mock = Mock()
            df_mock.unpersist = Mock()
            entry = CacheEntry(
                dataframe=df_mock,
                cache_key=cache_key,
                row_count=5,
                storage_level=StorageLevel.MEMORY_AND_DISK,
                is_persisted=True,
            )
            manager._cache[cache_key] = entry

        # Unpersist all
        manager.unpersist_all()
        assert len(manager._cache) == 0

    def test_memory_check(self, sample_dataframe):
        """Test memory availability check."""
        manager = CacheManager()

        # Mock spark session and context
        mock_session = Mock()
        mock_sc = Mock()
        mock_status = Mock()
        mock_executor_info = Mock()
        mock_executor_info.totalCores = 4

        mock_status.getExecutorInfos.return_value = [mock_executor_info]
        mock_sc.statusTracker.return_value = mock_status
        mock_session.sparkContext = mock_sc

        # Use a mock DataFrame with the mocked session
        mock_df = Mock()
        mock_df.sparkSession = mock_session

        # Should return True (memory available)
        result = manager._check_memory_available(mock_df)
        assert result is True

    def test_eviction(self, sample_dataframe):
        """Test LRU eviction when cache is full."""
        manager = CacheManager()

        # Add entries up to max cache size (10)
        for i in range(12):
            cache_key = f"key_{i}"
            df_mock = Mock()
            df_mock.unpersist = Mock()
            entry = CacheEntry(
                dataframe=df_mock,
                cache_key=cache_key,
                row_count=5,
                storage_level=StorageLevel.MEMORY_AND_DISK,
                is_persisted=True,
                last_accessed=time.time()
                - (12 - i),  # Older entries have lower access time
            )
            manager._cache[cache_key] = entry
            time.sleep(0.001)  # Small delay to ensure different timestamps

        # Trigger eviction
        manager._evict_if_needed()

        # Should have evicted 2 oldest entries
        assert len(manager._cache) == 10
        assert "key_0" not in manager._cache
        assert "key_1" not in manager._cache
        assert "key_2" in manager._cache
        assert manager._stats.evictions == 2


class TestIntegrationWithProfiler:
    """Integration tests with the profiler module."""

    def test_profile_with_caching_enabled(self, sample_dataframe):
        """Test profiling with caching enabled."""
        from pyspark_analyzer import analyze

        # Mock persist/unpersist
        sample_dataframe.persist = Mock(return_value=sample_dataframe)
        sample_dataframe.unpersist = Mock()
        sample_dataframe.count = Mock(
            return_value=2_000_000
        )  # Large enough to trigger caching

        # Run analysis with patched MIN_ROWS_TO_CACHE to trigger caching
        with patch("pyspark_analyzer.caching.MIN_ROWS_TO_CACHE", 1):
            result = analyze(sample_dataframe, output_format="dict")

        # Verify caching info is in result
        assert "caching" in result
        assert result["caching"]["enabled"] is True

    def test_profile_with_caching_disabled(self, sample_dataframe):
        """Test profiling with caching disabled."""
        from pyspark_analyzer import analyze

        # Mock count to return small row count (won't trigger caching)
        sample_dataframe.count = Mock(return_value=100)

        # Run analysis
        result = analyze(sample_dataframe, output_format="dict")

        # When DataFrame is small and caching is not triggered,
        # caching stats still appear but cache_key should be None
        if "caching" in result:
            assert result["caching"]["cache_key"] is None
            # No actual caching should have occurred
            assert result["caching"]["cache_hits"] == 0

    def test_profile_with_cache_error_handling(self, sample_dataframe):
        """Test that profiling continues even if caching fails."""
        from pyspark_analyzer import analyze

        # Mock persist to raise an error
        sample_dataframe.persist = Mock(side_effect=Exception("Caching failed"))
        sample_dataframe.count = Mock(
            return_value=2_000_000
        )  # Large enough to trigger caching

        # Should complete successfully despite cache error
        with patch("pyspark_analyzer.caching.MIN_ROWS_TO_CACHE", 1):
            result = analyze(sample_dataframe, output_format="dict")

        assert "columns" in result
        assert len(result["columns"]) > 0
