"""
Test cases for utility functions.
"""

import pytest
import json
from datetime import datetime, date

from pyspark_analyzer import ConfigurationError
from pyspark_analyzer.utils import (
    format_profile_output,
    _create_summary_report,
)


class TestFormatProfileOutput:
    """Test cases for format_profile_output function."""

    def test_format_as_dict(self):
        """Test formatting as dictionary (default)."""
        profile_data = {
            "overview": {"total_rows": 100, "total_columns": 3},
            "columns": {"col1": {"min": 1, "max": 10}},
        }

        result = format_profile_output(profile_data, "dict")
        assert result == profile_data

        # Test default format
        result = format_profile_output(profile_data)
        assert result == profile_data

    def test_format_as_json(self):
        """Test formatting as JSON string."""
        profile_data = {
            "overview": {"total_rows": 100, "total_columns": 3},
            "columns": {
                "col1": {"min": 1, "max": 10, "mean": 5.5},
                "col2": {"data_type": "StringType", "null_count": 5},
            },
        }

        result = format_profile_output(profile_data, "json")

        # Check it's a valid JSON string
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed == profile_data

        # Check formatting
        assert "\n" in result  # Should be indented
        assert "  " in result  # Should have indentation

    def test_format_as_json_with_datetime(self):
        """Test JSON formatting with datetime objects."""
        profile_data = {
            "columns": {
                "date_col": {
                    "min_date": datetime(2023, 1, 1),
                    "max_date": date(2023, 12, 31),
                }
            }
        }

        result = format_profile_output(profile_data, "json")

        # Should handle datetime serialization
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "2023-01-01" in parsed["columns"]["date_col"]["min_date"]
        assert "2023-12-31" in parsed["columns"]["date_col"]["max_date"]

    def test_format_as_summary(self):
        """Test formatting as summary report."""
        profile_data = {
            "overview": {"total_rows": 1000, "total_columns": 4},
            "columns": {
                "id": {
                    "data_type": "IntegerType",
                    "null_percentage": 0.0,
                    "distinct_count": 1000,
                    "min": 1,
                    "max": 1000,
                    "mean": 500.5,
                },
                "name": {
                    "data_type": "StringType",
                    "null_percentage": 5.0,
                    "distinct_count": 950,
                    "min_length": 3,
                    "max_length": 20,
                    "avg_length": 8.5,
                },
                "created": {
                    "data_type": "DateType",
                    "null_percentage": 0.0,
                    "distinct_count": 365,
                    "min_date": "2023-01-01",
                    "max_date": "2023-12-31",
                },
            },
        }

        result = format_profile_output(profile_data, "summary")

        # Check it's a string
        assert isinstance(result, str)

        # Check key components are present
        assert "DataFrame Profile Summary" in result
        assert "Total Rows: 1,000" in result
        assert "Total Columns: 4" in result

        # Check column details
        assert "Column: id" in result
        assert "Type: IntegerType" in result
        assert "Null %: 0.00%" in result
        assert "Min: 1" in result
        assert "Max: 1000" in result
        assert "Mean: 500.50" in result

        assert "Column: name" in result
        assert "Min Length: 3" in result
        assert "Max Length: 20" in result
        assert "Avg Length: 8.50" in result

        assert "Column: created" in result
        assert "Date Range: 2023-01-01 to 2023-12-31" in result

    def test_format_invalid_type(self):
        """Test formatting with invalid format type."""
        profile_data = {"test": "data"}

        with pytest.raises(ConfigurationError) as excinfo:
            format_profile_output(profile_data, "invalid")

        assert "Unsupported format type: invalid" in str(excinfo.value)


class TestCreateSummaryReport:
    """Test cases for _create_summary_report function."""

    def test_empty_profile_data(self):
        """Test summary report with empty profile data."""
        profile_data = {}

        result = _create_summary_report(profile_data)

        assert "DataFrame Profile Summary" in result
        assert "Total Rows: N/A" in result
        assert "Total Columns: N/A" in result

    def test_missing_overview(self):
        """Test summary report with missing overview section."""
        profile_data = {
            "columns": {"col1": {"data_type": "IntegerType", "null_percentage": 10.0}}
        }

        result = _create_summary_report(profile_data)

        assert "Total Rows: N/A" in result
        assert "Column: col1" in result

    def test_missing_columns(self):
        """Test summary report with missing columns section."""
        profile_data = {"overview": {"total_rows": 500, "total_columns": 2}}

        result = _create_summary_report(profile_data)

        assert "Total Rows: 500" in result
        assert "Column Details:" in result
        # Should not have any column details

    def test_column_with_missing_stats(self):
        """Test summary report with columns missing some statistics."""
        profile_data = {
            "overview": {"total_rows": 100, "total_columns": 1},
            "columns": {
                "incomplete_col": {
                    "data_type": "UnknownType",
                    # Missing null_percentage and distinct_count
                }
            },
        }

        result = _create_summary_report(profile_data)

        assert "Column: incomplete_col" in result
        assert "Type: UnknownType" in result
        assert "Null %: 0.00%" in result  # Should use default
        assert "Distinct Values: N/A" in result

    def test_numeric_column_formatting(self):
        """Test summary report formatting for numeric columns."""
        profile_data = {
            "columns": {
                "numeric_col": {
                    "data_type": "DoubleType",
                    "null_percentage": 2.5,
                    "distinct_count": 50,
                    "min": 0.0,
                    "max": 100.0,
                    "mean": 50.12345,
                }
            }
        }

        result = _create_summary_report(profile_data)

        # Check mean is formatted to 2 decimal places
        assert "Mean: 50.12" in result
        assert "Null %: 2.50%" in result

    def test_report_structure(self):
        """Test overall structure of the summary report."""
        profile_data = {
            "overview": {"total_rows": 1000, "total_columns": 2},
            "columns": {
                "col1": {"data_type": "IntegerType", "null_percentage": 0.0},
                "col2": {"data_type": "StringType", "null_percentage": 5.0},
            },
        }

        result = _create_summary_report(profile_data)
        lines = result.split("\n")

        # Check structure
        assert lines[0] == "DataFrame Profile Summary"
        assert lines[1] == "=" * 50
        assert "Total Rows:" in lines[2]
        assert "Total Columns:" in lines[3]
        assert lines[4] == ""
        assert lines[5] == "Column Details:"
        assert lines[6] == "-" * 30
