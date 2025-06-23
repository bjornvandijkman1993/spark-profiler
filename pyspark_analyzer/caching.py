"""
Advanced caching strategy for PySpark DataFrame profiling.

This module provides intelligent caching of sampled DataFrames to minimize
repeated computations, with memory-aware decisions and automatic cleanup.
"""

import hashlib
import time
from dataclasses import dataclass, field

from py4j.protocol import Py4JError, Py4JJavaError
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from .exceptions import SparkOperationError
from .logging import get_logger
from .sampling import SamplingMetadata

logger = get_logger(__name__)


# Default caching configuration
MEMORY_THRESHOLD = 0.7  # 70% memory usage
MIN_ROWS_TO_CACHE = 1_000_000
STORAGE_LEVEL = "MEMORY_AND_DISK"


@dataclass
class CacheEntry:
    """Represents a cached DataFrame with metadata."""

    dataframe: DataFrame
    cache_key: str
    row_count: int
    storage_level: StorageLevel
    cached_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    is_persisted: bool = False

    def access(self) -> None:
        """Update access statistics."""
        self.last_accessed = time.time()
        self.access_count += 1


@dataclass
class CacheStatistics:
    """Track cache performance statistics."""

    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    total_cached_bytes: int = 0
    evictions: int = 0

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        if self.total_requests == 0:
            return 0.0
        return self.cache_hits / self.total_requests

    def record_hit(self) -> None:
        """Record a cache hit."""
        self.total_requests += 1
        self.cache_hits += 1

    def record_miss(self) -> None:
        """Record a cache miss."""
        self.total_requests += 1
        self.cache_misses += 1


class CacheManager:
    """
    Manages DataFrame caching with memory-aware decisions and LRU eviction.

    This class provides centralized cache management for PySpark DataFrames,
    including memory monitoring, cache key generation, and automatic cleanup.
    """

    def __init__(self) -> None:
        """
        Initialize the cache manager.
        """
        self._cache: dict[str, CacheEntry] = {}
        self._stats = CacheStatistics()
        logger.info("CacheManager initialized")

    def generate_cache_key(
        self, df: DataFrame, sampling_metadata: SamplingMetadata | None = None
    ) -> str:
        """
        Generate a unique cache key for a DataFrame.

        The key is based on:
        - DataFrame schema
        - Sampling configuration (if applicable)
        - DataFrame ID (for uniqueness)

        Args:
            df: DataFrame to generate key for
            sampling_metadata: Optional sampling metadata

        Returns:
            Unique cache key string
        """
        # Build key components
        key_parts = []

        # Schema representation
        schema_str = str(df.schema)
        key_parts.append(schema_str)

        # Sampling information
        if sampling_metadata:
            key_parts.append(f"sampled_{sampling_metadata.sampling_fraction}")
            key_parts.append(f"size_{sampling_metadata.sample_size}")

        # DataFrame RDD ID for uniqueness
        try:
            rdd_id = df.rdd.id()
            key_parts.append(f"rdd_{rdd_id}")
        except Exception:
            # Fallback if RDD ID is not available
            key_parts.append(f"time_{time.time()}")

        # Generate hash
        key_str = "|".join(key_parts)
        cache_key = hashlib.md5(key_str.encode(), usedforsecurity=False).hexdigest()

        logger.debug(f"Generated cache key: {cache_key[:8]}... for DataFrame")
        return cache_key

    def should_cache(self, df: DataFrame, row_count: int, force: bool = False) -> bool:
        """
        Determine whether a DataFrame should be cached.

        Decision factors:
        - Cache is enabled
        - DataFrame size exceeds minimum threshold
        - Memory usage is below threshold
        - Force flag overrides size checks

        Args:
            df: DataFrame to evaluate
            row_count: Number of rows in the DataFrame
            force: Force caching regardless of size

        Returns:
            True if DataFrame should be cached
        """
        # Caching is always enabled now

        # Check size threshold
        if not force and row_count < MIN_ROWS_TO_CACHE:
            logger.debug(
                f"DataFrame too small to cache: {row_count:,} rows < "
                f"{MIN_ROWS_TO_CACHE:,} threshold"
            )
            return False

        # Check memory availability
        if not self._check_memory_available(df):
            logger.warning("Insufficient memory for caching")
            return False

        logger.debug(f"DataFrame eligible for caching: {row_count:,} rows")
        return True

    def cache_dataframe(
        self,
        df: DataFrame,
        cache_key: str | None = None,
        row_count: int | None = None,
        sampling_metadata: SamplingMetadata | None = None,
    ) -> tuple[DataFrame, str]:
        """
        Cache a DataFrame with the configured storage level.

        Args:
            df: DataFrame to cache
            cache_key: Optional pre-computed cache key
            row_count: Optional row count (avoids recomputation)
            sampling_metadata: Optional sampling metadata

        Returns:
            Tuple of (cached DataFrame, cache key)
        """
        # Generate cache key if not provided
        if cache_key is None:
            cache_key = self.generate_cache_key(df, sampling_metadata)

        # Check if already cached
        if cache_key in self._cache:
            logger.debug(f"DataFrame already cached with key: {cache_key[:8]}...")
            entry = self._cache[cache_key]
            entry.access()
            if self._stats:
                self._stats.record_hit()
            return entry.dataframe, cache_key

        # Record cache miss
        if self._stats:
            self._stats.record_miss()

        # Get storage level
        storage_level = self._get_storage_level()

        try:
            # Cache the DataFrame
            logger.info(f"Caching DataFrame with key: {cache_key[:8]}...")
            df_cached = df.persist(storage_level)

            # Force evaluation to ensure caching happens
            _ = df_cached.count() if row_count is None else row_count

            # Create cache entry
            entry = CacheEntry(
                dataframe=df_cached,
                cache_key=cache_key,
                row_count=row_count or df_cached.count(),
                storage_level=storage_level,
                is_persisted=True,
            )

            # Store in cache
            self._cache[cache_key] = entry

            # Check if eviction is needed
            self._evict_if_needed()

            logger.info(
                f"Successfully cached DataFrame: {entry.row_count:,} rows, "
                f"storage level: {storage_level}"
            )

            return df_cached, cache_key

        except (AnalysisException, Py4JError, Py4JJavaError) as e:
            logger.error(f"Failed to cache DataFrame: {e!s}")
            raise SparkOperationError(f"Failed to cache DataFrame: {e!s}", e) from e

    def get_cached(self, cache_key: str) -> DataFrame | None:
        """
        Retrieve a cached DataFrame by key.

        Args:
            cache_key: Cache key to lookup

        Returns:
            Cached DataFrame if found, None otherwise
        """
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            entry.access()
            if self._stats:
                self._stats.record_hit()
            logger.debug(f"Cache hit for key: {cache_key[:8]}...")
            return entry.dataframe

        if self._stats:
            self._stats.record_miss()
        logger.debug(f"Cache miss for key: {cache_key[:8]}...")
        return None

    def unpersist(self, cache_key: str, blocking: bool = False) -> bool:
        """
        Unpersist a cached DataFrame.

        Args:
            cache_key: Cache key of DataFrame to unpersist
            blocking: Whether to block until unpersisted

        Returns:
            True if successfully unpersisted
        """
        if cache_key not in self._cache:
            logger.debug(f"Cache key not found: {cache_key[:8]}...")
            return False

        entry = self._cache[cache_key]

        if entry.is_persisted:
            try:
                logger.debug(f"Unpersisting DataFrame: {cache_key[:8]}...")
                entry.dataframe.unpersist(blocking=blocking)
                entry.is_persisted = False
                logger.info(f"Unpersisted DataFrame: {cache_key[:8]}...")
            except Exception as e:
                logger.warning(f"Failed to unpersist DataFrame: {e!s}")
                return False

        # Remove from cache
        del self._cache[cache_key]
        return True

    def unpersist_all(self, blocking: bool = False) -> None:
        """
        Unpersist all cached DataFrames.

        Args:
            blocking: Whether to block until all are unpersisted
        """
        logger.info(f"Unpersisting all {len(self._cache)} cached DataFrames")

        cache_keys = list(self._cache.keys())
        for cache_key in cache_keys:
            self.unpersist(cache_key, blocking=blocking)

    def get_statistics(self) -> CacheStatistics | None:
        """Get cache performance statistics."""
        return self._stats

    def _check_memory_available(self, df: DataFrame) -> bool:
        """
        Check if there's sufficient memory to cache a DataFrame.

        Args:
            df: DataFrame to check

        Returns:
            True if memory is available
        """
        try:
            # Get Spark context and status
            sc = df.sparkSession.sparkContext
            status = sc.statusTracker()

            # Get memory info from executors
            # This is a simplified check - in production, you might want
            # to query JMX beans or use more sophisticated monitoring
            executor_infos = status.getExecutorInfos()

            if not executor_infos:
                # No executor info available, assume memory is ok
                logger.debug("No executor info available for memory check")
                return True

            # Simple heuristic: check if we're below threshold
            # In practice, this would need more sophisticated memory tracking
            total_memory = (
                sum(e.totalCores for e in executor_infos) * 2 * 1024 * 1024 * 1024
            )  # Assume 2GB per core
            used_memory = (
                len(self._cache) * 500 * 1024 * 1024
            )  # Rough estimate: 500MB per cached DF

            memory_usage = used_memory / total_memory if total_memory > 0 else 0

            logger.debug(f"Memory usage estimate: {memory_usage:.2%}")
            return bool(memory_usage < MEMORY_THRESHOLD)

        except Exception as e:
            logger.warning(f"Could not check memory availability: {e!s}")
            # If we can't check, be conservative and allow caching
            return True

    def _get_storage_level(self) -> StorageLevel:
        """Get the appropriate Spark StorageLevel from configuration."""
        level_map = {
            "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
            "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
            "DISK_ONLY": StorageLevel.DISK_ONLY,
            "MEMORY_ONLY_2": StorageLevel.MEMORY_ONLY_2,
            "MEMORY_AND_DISK_2": StorageLevel.MEMORY_AND_DISK_2,
        }

        # Handle serialized versions that might not exist in all PySpark versions
        if hasattr(StorageLevel, "MEMORY_ONLY_SER"):
            level_map["MEMORY_ONLY_SER"] = StorageLevel.MEMORY_ONLY_SER
            level_map["MEMORY_AND_DISK_SER"] = StorageLevel.MEMORY_AND_DISK_SER

        if hasattr(StorageLevel, "OFF_HEAP"):
            level_map["OFF_HEAP"] = StorageLevel.OFF_HEAP

        return level_map.get(STORAGE_LEVEL, StorageLevel.MEMORY_AND_DISK)

    def _evict_if_needed(self) -> None:
        """
        Evict least recently used entries if cache is too large.

        Currently uses a simple LRU policy with a fixed size limit.
        """
        max_cache_size = 10  # Maximum number of cached DataFrames

        if len(self._cache) <= max_cache_size:
            return

        # Sort by last accessed time
        sorted_entries = sorted(self._cache.items(), key=lambda x: x[1].last_accessed)

        # Evict oldest entries
        num_to_evict = len(self._cache) - max_cache_size
        logger.info(f"Evicting {num_to_evict} least recently used cache entries")

        for cache_key, _ in sorted_entries[:num_to_evict]:
            self.unpersist(cache_key)
            if self._stats:
                self._stats.evictions += 1
