"""
Main DataFrame profiler class for PySpark DataFrames.
"""

import pandas as pd
from typing import Dict, Any, List, Optional, Union
from pyspark.sql import DataFrame
from pyspark.sql.types import NumericType, StringType, TimestampType, DateType

from .statistics import StatisticsComputer
from .utils import get_column_data_types, format_profile_output
from .performance import optimize_dataframe_for_profiling
from .sampling import SamplingConfig, SamplingMetadata, apply_sampling


class DataFrameProfiler:
    """
    Main profiler class for generating comprehensive statistics for PySpark DataFrames.

    This class analyzes a PySpark DataFrame and computes various statistics for each column,
    including basic counts, data type specific metrics, and null value analysis.
    """

    def __init__(
        self,
        dataframe: DataFrame,
        optimize_for_large_datasets: bool = False,
        sampling_config: Optional[SamplingConfig] = None,
    ):
        """
        Initialize the profiler with a PySpark DataFrame.

        Args:
            dataframe: PySpark DataFrame to profile
            optimize_for_large_datasets: If True, use optimized batch processing for better performance
            sampling_config: Sampling configuration. If None, auto-sampling is enabled for large datasets.
        """
        if not isinstance(dataframe, DataFrame):
            raise TypeError("Input must be a PySpark DataFrame")

        # Set up sampling with default config if not provided
        if sampling_config is None:
            sampling_config = SamplingConfig()

        self.sampling_config = sampling_config
        self.sampling_metadata: Optional[SamplingMetadata] = None

        # Store original DataFrame
        self._original_dataframe = dataframe
        self.df = dataframe
        self._sampling_applied = False
        self.optimize_for_large_datasets = optimize_for_large_datasets

        # Initialize with lazy evaluation - defer heavy operations
        self.column_types = get_column_data_types(self.df)
        self.stats_computer: Optional[StatisticsComputer] = None

    def _apply_sampling(self) -> None:
        """Apply sampling if not already applied."""
        if self._sampling_applied:
            return

        # Apply sampling
        self.df, self.sampling_metadata = apply_sampling(
            self._original_dataframe, self.sampling_config
        )

        # Optimize DataFrame if requested (after sampling)
        if self.optimize_for_large_datasets:
            self.df = optimize_dataframe_for_profiling(
                self.df, row_count=self.sampling_metadata.sample_size
            )

        # Update column types if DataFrame changed
        self.column_types = get_column_data_types(self.df)
        self._sampling_applied = True

    def _ensure_stats_computer(self) -> StatisticsComputer:
        """Initialize stats computer with lazy evaluation."""
        if self.stats_computer is None:
            self._apply_sampling()
            # Ensure sampling_metadata is available after _apply_sampling
            assert self.sampling_metadata is not None
            # Pass the sample size to avoid redundant count operations
            self.stats_computer = StatisticsComputer(
                self.df, total_rows=self.sampling_metadata.sample_size
            )
        return self.stats_computer

    def profile(
        self,
        columns: Optional[List[str]] = None,
        output_format: str = "pandas",
        include_advanced: bool = True,
        include_quality: bool = True,
    ) -> Union[pd.DataFrame, Dict[str, Any], str]:
        """
        Generate a comprehensive profile of the DataFrame.

        Args:
            columns: List of specific columns to profile. If None, profiles all columns.
            output_format: Output format ("pandas", "dict", "json", "summary")
                          Defaults to "pandas" for easy analysis.
            include_advanced: Include advanced statistics (skewness, kurtosis, outliers, etc.)
            include_quality: Include data quality metrics

        Returns:
            Profile results in requested format
        """
        # Ensure sampling is applied before profiling
        self._apply_sampling()

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

        # Get stats computer with lazy initialization
        stats_computer = self._ensure_stats_computer()

        # Use batch processing for large datasets if enabled
        if self.optimize_for_large_datasets:
            profile_result["columns"] = stats_computer.compute_all_columns_batch(
                columns
            )
        else:
            # Standard column-by-column processing
            for column in columns:
                profile_result["columns"][column] = self._profile_column(
                    column,
                    include_advanced=include_advanced,
                    include_quality=include_quality,
                )

        return format_profile_output(profile_result, output_format)

    def _get_overview(self) -> Dict[str, Any]:
        """Get overview statistics for the entire DataFrame."""
        # Ensure sampling is applied to get accurate metadata
        self._apply_sampling()
        # Ensure sampling_metadata is available after _apply_sampling
        assert self.sampling_metadata is not None

        # Use cached row count from sampling metadata
        total_rows = self.sampling_metadata.sample_size
        total_columns = len(self.df.columns)

        return {
            "total_rows": total_rows,
            "total_columns": total_columns,
            "column_types": {
                col: str(dtype) for col, dtype in self.column_types.items()
            },
        }

    def _profile_column(
        self,
        column_name: str,
        include_advanced: bool = True,
        include_quality: bool = True,
    ) -> Dict[str, Any]:
        """
        Profile a single column.

        Args:
            column_name: Name of the column to profile
            include_advanced: Include advanced statistics
            include_quality: Include data quality metrics

        Returns:
            Dictionary containing column statistics
        """
        # Ensure sampling and stats computer are ready
        self._apply_sampling()
        stats_computer = self._ensure_stats_computer()
        # Ensure sampling_metadata is available after _apply_sampling
        assert self.sampling_metadata is not None

        column_type = self.column_types[column_name]

        # Handle empty DataFrame case
        if self.sampling_metadata.sample_size == 0:
            return {
                "data_type": str(column_type),
                "total_count": 0,
                "non_null_count": 0,
                "null_count": 0,
                "null_percentage": 0.0,
                "distinct_count": 0,
                "distinct_percentage": 0.0,
            }

        # Basic statistics for all columns
        basic_stats = stats_computer.compute_basic_stats(column_name)

        column_profile = {"data_type": str(column_type), **basic_stats}

        # Add type-specific statistics
        if isinstance(column_type, NumericType):
            numeric_stats = stats_computer.compute_numeric_stats(
                column_name, advanced=include_advanced
            )
            column_profile.update(numeric_stats)

            # Add outlier statistics for numeric columns
            if include_advanced:
                outlier_stats = stats_computer.compute_outlier_stats(column_name)
                column_profile["outliers"] = outlier_stats

        elif isinstance(column_type, StringType):
            string_stats = stats_computer.compute_string_stats(
                column_name,
                top_n=10 if include_advanced else 0,
                pattern_detection=include_advanced,
            )
            column_profile.update(string_stats)

        elif isinstance(column_type, (TimestampType, DateType)):
            temporal_stats = stats_computer.compute_temporal_stats(column_name)
            column_profile.update(temporal_stats)

        # Add data quality metrics if requested
        if include_quality:
            # Determine quality check type based on column type
            if isinstance(column_type, NumericType):
                quality_type = "numeric"
            elif isinstance(column_type, StringType):
                quality_type = "string"
            else:
                # For complex types (arrays, structs, etc.), skip type-specific quality checks
                quality_type = "other"

            quality_stats = stats_computer.compute_data_quality_stats(
                column_name,
                column_type=quality_type,
            )
            column_profile["quality"] = quality_stats

        return column_profile

    def _get_sampling_info(self) -> Dict[str, Any]:
        """Get sampling information for the profile."""
        # Ensure sampling is applied to get accurate metadata
        self._apply_sampling()

        if not self.sampling_metadata:
            return {"is_sampled": False}

        return {
            "is_sampled": self.sampling_metadata.is_sampled,
            "original_size": self.sampling_metadata.original_size,
            "sample_size": self.sampling_metadata.sample_size,
            "sampling_fraction": self.sampling_metadata.sampling_fraction,
            "sampling_time": self.sampling_metadata.sampling_time,
            "estimated_speedup": self.sampling_metadata.speedup_estimate,
        }

    def to_csv(self, path: str, **kwargs: Any) -> None:
        """
        Save profile results to CSV file.

        Args:
            path: Path to save the CSV file
            **kwargs: Additional arguments passed to pandas.DataFrame.to_csv()
        """
        df = self.profile(output_format="pandas")
        if hasattr(df, "to_csv"):
            df.to_csv(path, **kwargs)

    def to_parquet(self, path: str, **kwargs: Any) -> None:
        """
        Save profile results to Parquet file.

        Args:
            path: Path to save the Parquet file
            **kwargs: Additional arguments passed to pandas.DataFrame.to_parquet()
        """
        df = self.profile(output_format="pandas")
        if hasattr(df, "to_parquet"):
            df.to_parquet(path, **kwargs)

    def to_sql(self, name: str, con: Any, **kwargs: Any) -> None:
        """
        Save profile results to SQL database table.

        Args:
            name: Name of the SQL table
            con: SQLAlchemy engine or DBAPI2 connection
            **kwargs: Additional arguments passed to pandas.DataFrame.to_sql()
        """
        df = self.profile(output_format="pandas")
        if hasattr(df, "to_sql"):
            df.to_sql(name, con, **kwargs)

    def format_output(
        self, format_type: str = "pandas"
    ) -> Union[pd.DataFrame, Dict[str, Any], str]:
        """
        Get profile output in specified format.

        This is a convenience method that calls profile() with the specified format.

        Args:
            format_type: Output format ("pandas", "dict", "json", "summary")

        Returns:
            Profile results in requested format
        """
        return self.profile(output_format=format_type)

    def quick_profile(self, columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Generate a quick profile without advanced statistics for performance.

        Args:
            columns: List of specific columns to profile. If None, profiles all columns.

        Returns:
            Basic profile results as dictionary
        """
        result = self.profile(
            columns=columns,
            output_format="dict",
            include_advanced=False,
            include_quality=False,
        )
        # Type assertion for mypy
        assert isinstance(result, dict)
        return result

    def quality_report(self, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Generate a data quality report for specified columns.

        Args:
            columns: List of specific columns to analyze. If None, analyzes all columns.

        Returns:
            DataFrame with quality metrics for each column
        """
        profile = self.profile(
            columns=columns,
            output_format="dict",
            include_advanced=False,
            include_quality=True,
        )

        # Type assertion for mypy
        assert isinstance(profile, dict)

        # Extract quality metrics into a summary
        quality_data = []
        for col_name, col_stats in profile["columns"].items():
            if "quality" in col_stats:
                quality_info = {
                    "column": col_name,
                    "data_type": col_stats["data_type"],
                    **col_stats["quality"],
                }
                quality_data.append(quality_info)

        return pd.DataFrame(quality_data)
