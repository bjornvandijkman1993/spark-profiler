"""
Utility functions for the DataFrame profiler.
"""

import pandas as pd
from typing import Dict, Any, Union
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType


def get_column_data_types(dataframe: DataFrame) -> Dict[str, DataType]:
    """
    Get data types for all columns in the DataFrame.

    Args:
        dataframe: PySpark DataFrame

    Returns:
        Dictionary mapping column names to their data types
    """
    return {field.name: field.dataType for field in dataframe.schema.fields}


def format_profile_output(
    profile_data: Dict[str, Any], format_type: str = "pandas"
) -> Union[pd.DataFrame, Dict[str, Any], str]:
    """
    Format the profile output in different formats.

    Args:
        profile_data: Raw profile data dictionary
        format_type: Output format ("pandas", "dict", "json", "summary")
                     Defaults to "pandas" for easy analysis.

    Returns:
        Formatted profile data in requested format
    """
    if format_type == "pandas":
        return _create_pandas_dataframe(profile_data)
    elif format_type == "dict":
        return profile_data
    elif format_type == "json":
        import json

        return json.dumps(profile_data, indent=2, default=str)
    elif format_type == "summary":
        return _create_summary_report(profile_data)
    else:
        raise ValueError(f"Unsupported format type: {format_type}")


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

    report_lines = [
        "DataFrame Profile Summary",
        "=" * 50,
        f"Total Rows: {overview.get('total_rows', 'N/A'):,}",
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
    Convert profile data to pandas DataFrame.

    Each row represents a column from the profiled Spark DataFrame.
    Metadata is stored in DataFrame.attrs.

    Args:
        profile_data: Profile data dictionary

    Returns:
        pandas DataFrame with profile statistics
    """
    columns_data = profile_data.get("columns", {})

    # Convert nested dict to list of dicts for DataFrame constructor
    rows = []
    for col_name, col_stats in columns_data.items():
        row = {"column_name": col_name}
        row.update(col_stats)
        rows.append(row)

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Add metadata as attributes
    df.attrs["overview"] = profile_data.get("overview", {})
    df.attrs["sampling"] = profile_data.get("sampling", {})
    df.attrs["profiling_timestamp"] = pd.Timestamp.now()

    # Ensure consistent column order
    column_order = [
        "column_name",
        "data_type",
        "total_count",
        "non_null_count",
        "null_count",
        "null_percentage",
        "distinct_count",
        "distinct_percentage",
        "min",
        "max",
        "mean",
        "std",
        "median",
        "q1",
        "q3",
        "min_length",
        "max_length",
        "avg_length",
        "empty_count",
        "min_date",
        "max_date",
        "date_range_days",
    ]

    # Reorder columns (only include columns that exist)
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]

    return df
