# bc_etl/silver/standardize.py
"""
Standardization functions for the silver layer.
Handles name standardization and table reference normalization.
"""

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame

from bc_etl.utils import logging
from bc_etl.utils.config_loader import get_column_schema, get_table_config
from bc_etl.utils.exceptions import ColumnStandardizationError
from bc_etl.utils.naming import standardize_table_reference


def standardize_dataframe_from_config(
    df: DataFrame, table_base_name: str, exclude_columns: list[str] | None = None
) -> DataFrame:
    """
    Standardize all column names in a DataFrame based on loaded configuration.

    Args:
        df: Spark DataFrame with original BC column names
        table_base_name: Base name of the table (e.g., 'GLAccount') - no suffix
        exclude_columns: Optional list of column names to exclude from standardization

    Returns:
        DataFrame with standardized column names according to configuration

    Raises:
        ColumnStandardizationError: If column standardization fails
    """
    exclude_columns = exclude_columns or []

    try:
        # Get column schema from configuration
        column_schema = get_column_schema(table_base_name)

        if not column_schema:
            # If no config is available, return the DataFrame unchanged
            logging.warning(
                f"No column schema found for {table_base_name}, returning DataFrame unchanged",
                table=table_base_name,
            )
            return df

        # Use configuration-based standardization
        column_mapping = {}
        result_df = df

        # Apply standardization based on config
        for standard_name, column_info in column_schema.items():
            if standard_name in exclude_columns:
                continue

            # The source_name is the original column name in the bronze table
            source_name = column_info.get("bronze_name", standard_name)

            # Only rename if the column exists and has a different name
            if source_name in df.columns and source_name != standard_name:
                result_df = result_df.withColumnRenamed(source_name, standard_name)
                column_mapping[source_name] = standard_name

        # Log the column mapping
        if column_mapping:
            logging.info(
                "Standardized column names using configuration",
                table=table_base_name,
                column_count=len(column_mapping),
                mapping=column_mapping,
            )

        return result_df

    except Exception as e:
        error_msg = f"Failed to standardize DataFrame columns for {table_base_name}: {str(e)}"
        logging.error(error_msg)
        raise ColumnStandardizationError(error_msg) from e


def get_silver_table_name(table_base_name: str) -> str:
    """
    Determines the standardized silver table name for a base name.
    Uses proper case with no suffixes as per the naming convention.

    Args:
        table_base_name: Base name of the table (e.g., 'GLAccount', 'glaccount15')

    Returns:
        Standardized silver table name (e.g., 'GLAccount', 'Customer')
    """
    # Use standardize_table_reference to ensure proper case with no suffix
    standard_name = standardize_table_reference(table_base_name, include_suffix=False)

    # Get table config to check for any override
    table_config = get_table_config(standard_name)
    if table_config and "silver_target" in table_config:
        return str(table_config["silver_target"])

    return standard_name


def add_standard_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add standard audit columns to a DataFrame if they don't already exist.

    Args:
        df: Spark DataFrame

    Returns:
        DataFrame with standard audit columns added
    """
    # Check if ETL processing time column exists
    if "$EtlProcessingTime" not in df.columns:
        df = df.withColumn("$EtlProcessingTime", F.current_timestamp())

    return df
