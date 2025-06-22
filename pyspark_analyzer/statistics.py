"""
Statistics computation using type-specific calculators.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JError, Py4JJavaError

from .calculators import create_calculator
from .quality import QualityCalculator
from .exceptions import StatisticsError, SparkOperationError
from .logging import get_logger

logger = get_logger(__name__)


class StatisticsComputer:
    """Computes statistics for DataFrame columns using type-specific calculators."""

    def __init__(self, dataframe: DataFrame, total_rows: Optional[int] = None):
        """
        Initialize with a PySpark DataFrame.

        Args:
            dataframe: PySpark DataFrame to compute statistics for
            total_rows: Cached row count to avoid recomputation
        """
        self.df = dataframe
        self._total_rows = total_rows
        self._column_types = {
            field.name: field.dataType for field in self.df.schema.fields
        }
        logger.debug(
            f"StatisticsComputer initialized with {'cached' if total_rows else 'lazy'} row count"
        )

    def _get_total_rows(self) -> int:
        """Get total row count, computing if not cached."""
        if self._total_rows is None:
            logger.debug("Computing DataFrame row count")
            self._total_rows = self.df.count()
            logger.debug(f"Row count computed: {self._total_rows:,}")
        return self._total_rows

    def compute_all_columns_batch(
        self,
        columns: Optional[List[str]] = None,
        include_advanced: bool = True,
        include_quality: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compute statistics for multiple columns.

        Args:
            columns: List of columns to profile. If None, profiles all columns.
            include_advanced: Included for API compatibility (always True with calculators)
            include_quality: Include data quality metrics

        Returns:
            Dictionary mapping column names to their statistics
        """
        if columns is None:
            columns = self.df.columns

        logger.info(f"Starting computation for {len(columns)} columns")

        results = {}
        total_rows = self._get_total_rows()

        for column in columns:
            if column not in self._column_types:
                logger.warning(f"Column '{column}' not found in DataFrame")
                continue

            try:
                column_type = self._column_types[column]

                # Create appropriate calculator and compute stats
                calculator = create_calculator(self.df, column, column_type, total_rows)
                stats = calculator.calculate()

                # Add data type info
                stats["data_type"] = str(column_type)

                # Add quality metrics if requested
                if include_quality:
                    quality_calc = QualityCalculator(self.df, column, total_rows)
                    stats["quality"] = quality_calc.calculate_quality(
                        stats, column_type
                    )

                results[column] = stats

            except (AnalysisException, Py4JError, Py4JJavaError) as e:
                logger.error(
                    f"Spark error computing stats for column '{column}': {str(e)}"
                )
                raise SparkOperationError(
                    f"Failed to compute statistics for column '{column}': {str(e)}", e
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error computing stats for column '{column}': {str(e)}"
                )
                raise StatisticsError(
                    f"Failed to compute statistics for column '{column}': {str(e)}"
                )

        logger.info(f"Computation completed for {len(results)} columns")
        return results
