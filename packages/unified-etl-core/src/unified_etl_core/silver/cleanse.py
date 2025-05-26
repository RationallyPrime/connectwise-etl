# unified_etl/silver/cleanse.py

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)
from unified_etl.utils import logging
from unified_etl.utils.config_loader import get_column_schema

# Complete type mapping - no inference needed
TYPE_MAPPING = {
    # String types
    "string": StringType(),
    "varchar": StringType(),
    "nvarchar": StringType(),
    "char": StringType(),
    "text": StringType(),
    # Numeric types
    "int": IntegerType(),
    "integer": IntegerType(),
    "smallint": IntegerType(),
    "tinyint": IntegerType(),
    "bigint": IntegerType(),
    "decimal": DecimalType(18, 6),
    "numeric": DecimalType(18, 6),
    "real": DoubleType(),
    "float": DoubleType(),
    "double": DoubleType(),
    # Boolean type
    "bit": BooleanType(),
    "boolean": BooleanType(),
    # Date/Time types
    "date": DateType(),
    "datetime": TimestampType(),
    "timestamp": TimestampType(),
}


def apply_data_types(
    df: DataFrame,
    table_base_name: str | None = None,
    skip_columns: list[str] | None = None,
) -> DataFrame:
    """
    Apply appropriate data types to DataFrame columns based on configuration.

    Args:
        df: DataFrame to process
        table_base_name: Base name of the table for config lookup (e.g., 'GLAccount')
        skip_columns: Optional list of columns to skip type conversion

    Returns:
        DataFrame with proper data types
    """
    result_df = df
    skip_columns = skip_columns or []
    conversion_failures = []
    column_schema = {}

    # If table_base_name is provided, get column schema from config
    if table_base_name:
        column_schema = get_column_schema(table_base_name)
        logging.debug(f"Loaded column schema for {table_base_name}", schema_size=len(column_schema))

    with logging.span("apply_data_types", column_count=len(df.columns), table=table_base_name):
        # Iterate through the columns *currently* in the DataFrame (these should be standardized)
        for column in df.columns:
            if column in skip_columns:
                logging.debug("Skipping type conversion for column", column=column)
                continue

            data_type_str = None
            # Look up the *current* column name in the loaded schema keys
            if column in column_schema:
                # Get the type definition directly from the config for this standard column name
                data_type_str = column_schema[column].get("data_type")  # Corrected key lookup
                if data_type_str:
                    logging.debug("Using type from YAML config", column=column, type=data_type_str)
                else:
                    logging.warning(
                        "Column found in config, but 'data_type' key missing",
                        column=column,
                        table=table_base_name,
                        config_entry=column_schema[column],
                    )
                    data_type_str = "string"  # Default if type is missing

            # If the column (standardized name) is NOT found in the config keys, default to string
            else:
                # Log a warning only if it's not an internal column like $Company
                if not column.startswith("$"):
                    logging.warning(
                        "Column not found in configuration schema, defaulting to string type",
                        column=column,
                        table=table_base_name,
                    )
                data_type_str = "string"  # Default to string

            # Apply the type casting
            try:
                spark_type = TYPE_MAPPING.get(data_type_str.lower(), StringType())

                if spark_type == DateType():
                    # Use to_date, which returns null on failure
                    result_df = result_df.withColumn(column, F.to_date(F.col(column)))
                elif spark_type == TimestampType():
                    # Use to_timestamp, which returns null on failure
                    result_df = result_df.withColumn(column, F.to_timestamp(F.col(column)))
                else:
                    # Cast other types normally
                    result_df = result_df.withColumn(column, F.col(column).cast(spark_type))

            except Exception as e:
                conversion_failures.append((column, data_type_str, str(e)))
                logging.warning(
                    "Failed to cast column", column=column, target_type=data_type_str, error=str(e)
                )

    # Log summary
    logging.info(
        "Data type conversion complete",
        total_columns=len(df.columns),
        skipped_columns=len(skip_columns),
        failed_columns=len(conversion_failures),
    )

    # If there were failures, include them in the log
    if conversion_failures:
        logging.warning(
            "Some columns failed type conversion",
            failures=conversion_failures,
        )

    return result_df
