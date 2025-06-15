"""
Main DataFrame profiler class for PySpark DataFrames.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

from .statistics import StatisticsComputer
from .utils import get_column_data_types
from .performance import BatchStatisticsComputer, optimize_dataframe_for_profiling
from .sampling import SamplingConfig, SamplingDecisionEngine, SamplingMetadata


class DataFrameProfiler:
    """
    Main profiler class for generating comprehensive statistics for PySpark DataFrames.

    This class analyzes a PySpark DataFrame and computes various statistics for each column
    including basic counts, data type specific metrics, and null value analysis.
    """

    def __init__(
        self,
        dataframe: DataFrame,
        optimize_for_large_datasets: bool = False,
        sample_fraction: Optional[float] = None,
        sampling_config: Optional[SamplingConfig] = None,
    ):
        """
        Initialize the profiler with a PySpark DataFrame.

        Args:
            dataframe: PySpark DataFrame to profile
            optimize_for_large_datasets: If True, use optimized batch processing for better performance
            sample_fraction: If provided, sample the DataFrame to this fraction for faster profiling (legacy)
            sampling_config: Advanced sampling configuration (recommended over sample_fraction)
        """
        if not isinstance(dataframe, DataFrame):
            raise TypeError("Input must be a PySpark DataFrame")

        # Handle legacy sample_fraction parameter
        if sample_fraction and sampling_config:
            raise ValueError("Cannot specify both sample_fraction and sampling_config")

        if sample_fraction:
            sampling_config = SamplingConfig(target_fraction=sample_fraction)

        # Set up sampling
        if sampling_config is None:
            sampling_config = SamplingConfig()

        self.sampling_config = sampling_config
        self.sampling_engine = SamplingDecisionEngine(sampling_config)
        self.sampling_metadata: Optional[SamplingMetadata] = None

        # Apply sampling if needed
        if self.sampling_engine.should_sample(dataframe):
            self.df, self.sampling_metadata = self.sampling_engine.create_sample(dataframe)
        else:
            self.df = dataframe
            # Create metadata for non-sampled case
            original_size = dataframe.count()
            self.sampling_metadata = SamplingMetadata(
                original_size=original_size,
                sample_size=original_size,
                sampling_fraction=1.0,
                strategy_used="none",
                sampling_time=0.0,
                quality_score=1.0,
                is_sampled=False,
            )

        # Optimize DataFrame if requested (after sampling)
        if optimize_for_large_datasets:
            self.df = optimize_dataframe_for_profiling(self.df)

        self.column_types = get_column_data_types(self.df)
        self.stats_computer = StatisticsComputer(self.df)
        self.batch_computer = BatchStatisticsComputer(self.df) if optimize_for_large_datasets else None
        self.optimize_for_large_datasets = optimize_for_large_datasets
        self._cached_profile: Optional[Dict[str, Any]] = None  # Cache for profile results

    def profile(self, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Generate a comprehensive profile of the DataFrame.

        Args:
            columns: List of specific columns to profile. If None, profiles all columns.

        Returns:
            Dictionary containing profile information with column names as keys
        """
        # Cache the full profile if no specific columns are requested
        if columns is None and self._cached_profile is not None:
            return self._cached_profile
        if columns is None:
            columns = self.df.columns

        # Validate columns exist
        invalid_columns = set(columns) - set(self.df.columns)
        if invalid_columns:
            raise ValueError(f"Columns not found in DataFrame: {invalid_columns}")

        profile_result: Dict[str, Any] = {
            "overview": self._get_overview(),
            "columns": {},
            "sampling": self._get_sampling_info(),
        }

        # Use batch processing for large datasets if enabled
        if self.optimize_for_large_datasets and self.batch_computer:
            profile_result["columns"] = self.batch_computer.compute_all_columns_batch(columns)
        else:
            # Standard column-by-column processing
            for column in columns:
                profile_result["columns"][column] = self._profile_column(column)

        # Cache the full profile if all columns were profiled
        if columns is None or set(columns) == set(self.df.columns):
            self._cached_profile = profile_result

        return profile_result

    def get_profile(self, format_type: str = "dict") -> Any:
        """
        Get the profile in the specified format.

        Args:
            format_type: Format to return ('dict', 'json', or 'summary')

        Returns:
            Profile in the requested format
        """
        # Generate profile if not cached
        if self._cached_profile is None:
            self._cached_profile = self.profile()

        from .utils import format_profile_output

        return format_profile_output(self._cached_profile, format_type)

    def _get_overview(self) -> Dict[str, Any]:
        """Get overview statistics for the entire DataFrame."""
        # Use original size if sampling was applied
        if self.sampling_metadata and self.sampling_metadata.is_sampled:
            total_rows = self.sampling_metadata.original_size
        else:
            total_rows = self.df.count()
        total_columns = len(self.df.columns)

        # Estimate memory usage (approximate)
        # This is a rough estimate based on row count and column count
        # Actual memory usage depends on data types and content
        avg_bytes_per_cell = 20  # Conservative estimate
        memory_usage = total_rows * total_columns * avg_bytes_per_cell

        return {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "column_types": self.column_types,
            "memory_usage": memory_usage,
        }

    def _profile_column(self, column_name: str) -> Dict[str, Any]:
        """
        Profile a single column.

        Args:
            column_name: Name of the column to profile

        Returns:
            Dictionary containing column statistics
        """
        column_type = self.column_types[column_name]

        # Basic statistics for all columns
        basic_stats = self.stats_computer.compute_basic_stats(column_name)

        column_profile = {"data_type": str(column_type), **basic_stats}

        # Add type-specific statistics
        if isinstance(column_type, NumericType):
            numeric_stats = self.stats_computer.compute_numeric_stats(column_name)
            column_profile.update(numeric_stats)
        elif isinstance(column_type, StringType):
            string_stats = self.stats_computer.compute_string_stats(column_name)
            column_profile.update(string_stats)
        elif isinstance(column_type, (TimestampType, DateType)):
            temporal_stats = self.stats_computer.compute_temporal_stats(column_name)
            column_profile.update(temporal_stats)

        return column_profile

    def _get_sampling_info(self) -> Dict[str, Any]:
        """Get sampling information for the profile."""
        if not self.sampling_metadata:
            return {"is_sampled": False}

        return {
            "is_sampled": self.sampling_metadata.is_sampled,
            "original_size": self.sampling_metadata.original_size,
            "sample_size": self.sampling_metadata.sample_size,
            "sampling_fraction": self.sampling_metadata.sampling_fraction,
            "strategy_used": self.sampling_metadata.strategy_used,
            "sampling_time": self.sampling_metadata.sampling_time,
            "quality_score": self.sampling_metadata.quality_score,
            "reduction_ratio": self.sampling_metadata.reduction_ratio,
            "estimated_speedup": self.sampling_metadata.speedup_estimate,
        }
