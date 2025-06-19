"""
Utility functions for the DataFrame profiler.
"""

from typing import Dict, Any, Union
import pandas as pd

from .exceptions import ConfigurationError
from .logging import get_logger

logger = get_logger(__name__)


def escape_column_name(column_name: str) -> str:
    """
    Escape column name for safe use in PySpark SQL operations.

    Handles special characters and SQL injection attempts by properly
    escaping backticks and wrapping the column name in backticks.

    Args:
        column_name: Raw column name that may contain special characters

    Returns:
        Escaped column name wrapped in backticks

    Examples:
        >>> escape_column_name("normal_column")
        '`normal_column`'
        >>> escape_column_name("column.with.dots")
        '`column.with.dots`'
        >>> escape_column_name("col`with`backticks")
        '`col``with``backticks`'
        >>> escape_column_name("col; DROP TABLE users;--")
        '`col; DROP TABLE users;--`'
    """
    # Escape any backticks in the column name by doubling them
    escaped = column_name.replace("`", "``")
    # Wrap in backticks
    return f"`{escaped}`"


def format_profile_output(
    profile_data: Dict[str, Any], format_type: str = "dict"
) -> Union[pd.DataFrame, Dict[str, Any], str]:
    """
    Format the profile output in different formats.

    Args:
        profile_data: Raw profile data dictionary
        format_type: Output format ("dict", "json", "summary", "pandas")

    Returns:
        Formatted profile data
    """
    if format_type == "dict":
        return profile_data
    elif format_type == "json":
        import json

        return json.dumps(profile_data, indent=2, default=str)
    elif format_type == "summary":
        return _create_summary_report(profile_data)
    elif format_type == "pandas":
        return _create_pandas_dataframe(profile_data)
    else:
        logger.error(f"Unsupported format type: {format_type}")
        raise ConfigurationError(
            f"Unsupported format type: {format_type}. Supported formats are: dict, json, summary, pandas"
        )


def _create_summary_report(profile_data: Dict[str, Any]) -> str:
    """
    Create a human-readable summary report.

    Args:
        profile_data: Profile data dictionary

    Returns:
        Formatted summary string
    """
    overview = profile_data.get("overview", {})
    columns = profile_data.get("columns", {})

    total_rows = overview.get("total_rows", "N/A")
    total_rows_str = (
        f"{total_rows:,}" if isinstance(total_rows, (int, float)) else str(total_rows)
    )

    report_lines = [
        "DataFrame Profile Summary",
        "=" * 50,
        f"Total Rows: {total_rows_str}",
        f"Total Columns: {overview.get('total_columns', 'N/A')}",
        "",
        "Column Details:",
        "-" * 30,
    ]

    for col_name, col_stats in columns.items():
        null_pct = col_stats.get("null_percentage", 0)
        distinct_count = col_stats.get("distinct_count", "N/A")
        data_type = col_stats.get("data_type", "Unknown")

        report_lines.extend(
            [
                f"Column: {col_name}",
                f"  Type: {data_type}",
                f"  Null %: {null_pct:.2f}%",
                f"  Distinct Values: {distinct_count}",
            ]
        )

        # Add type-specific details
        if "min" in col_stats:  # Numeric column
            report_lines.extend(
                [
                    f"  Min: {col_stats.get('min')}",
                    f"  Max: {col_stats.get('max')}",
                    f"  Mean: {col_stats.get('mean', 0):.2f}",
                ]
            )
        elif "min_length" in col_stats:  # String column
            report_lines.extend(
                [
                    f"  Min Length: {col_stats.get('min_length')}",
                    f"  Max Length: {col_stats.get('max_length')}",
                    f"  Avg Length: {col_stats.get('avg_length', 0):.2f}",
                ]
            )
        elif "min_date" in col_stats:  # Temporal column
            report_lines.extend(
                [
                    f"  Date Range: {col_stats.get('min_date')} to {col_stats.get('max_date')}",
                ]
            )

        report_lines.append("")

    return "\n".join(report_lines)


def _create_pandas_dataframe(profile_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Create a pandas DataFrame from profile data.

    Args:
        profile_data: Profile data dictionary

    Returns:
        pandas DataFrame with profile statistics
    """
    # Extract column statistics
    columns_data = profile_data.get("columns", {})

    # Convert to a format suitable for pandas
    records = []
    for col_name, stats in columns_data.items():
        record = {"column_name": col_name}
        record.update(stats)
        records.append(record)

    # Create DataFrame
    df = pd.DataFrame(records)

    # Add metadata to DataFrame.attrs
    df.attrs["overview"] = profile_data.get("overview", {})
    df.attrs["sampling"] = profile_data.get("sampling", {})
    df.attrs["profiling_timestamp"] = pd.Timestamp.now()

    return df
