# bc_etl/utils/spark_utils.py
"""
Spark utility functions for table operations and safe data access.
"""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from bc_etl.utils import logging


def table_exists(spark: SparkSession, full_table_path: str) -> bool:
    """
    Check if a table exists in the Spark catalog.

    Args:
        spark: SparkSession to use
        full_table_path: Fully qualified table name (e.g., 'database.table')

    Returns:
        True if the table exists, False otherwise
    """
    try:
        # Parse the table name
        parts = full_table_path.split(".")
        if len(parts) == 2:
            database, table = parts
            tables = spark.sql(f"SHOW TABLES IN {database}").collect()
            return any(row.tableName == table for row in tables)
        elif len(parts) == 3:
            catalog, database, table = parts
            tables = spark.sql(f"SHOW TABLES IN {catalog}.{database}").collect()
            return any(row.tableName == table for row in tables)
        else:
            tables = spark.sql("SHOW TABLES").collect()
            return any(row.tableName == full_table_path for row in tables)
    except Exception as e:
        logging.warning(f"Error checking if table {full_table_path} exists: {str(e)}")
        return False


def read_table_safely(
    spark: SparkSession, full_table_path: str, default_value: DataFrame | None = None
) -> Optional[DataFrame]:  # noqa: UP007
    """
    Safely read a table, returning a default value if the table doesn't exist.

    Args:
        spark: SparkSession to use
        full_table_path: Fully qualified table name (e.g., 'database.table')
        default_value: Value to return if table doesn't exist (None by default)

    Returns:
        DataFrame from the table or default_value if table doesn't exist
    """
    try:
        if table_exists(spark, full_table_path):
            return spark.table(full_table_path)
        else:
            logging.warning(f"Table {full_table_path} does not exist")
            return default_value
    except Exception as e:
        logging.warning(f"Error reading table {full_table_path}: {str(e)}")
        return default_value


def create_empty_table_if_not_exists(
    spark: SparkSession, full_table_path: str, schema: DataFrame, save_mode: str = "errorifexists"
) -> bool:
    """
    Create an empty table with the same schema as the provided DataFrame if it doesn't exist.

    Args:
        spark: SparkSession to use
        full_table_path: Fully qualified table name (e.g., 'database.table')
        schema: DataFrame with the desired schema
        save_mode: Save mode to use when creating the table (default: errorifexists)

    Returns:
        True if table was created, False if it already existed or there was an error
    """
    try:
        if not table_exists(spark, full_table_path):
            # Create an empty DataFrame with the same schema
            empty_df = spark.createDataFrame([], schema.schema)

            # Write the empty DataFrame to create the table
            empty_df.write.mode(save_mode).saveAsTable(full_table_path)
            logging.info(f"Created empty table {full_table_path}")
            return True
        else:
            logging.info(f"Table {full_table_path} already exists, skipping creation")
            return False
    except Exception as e:
        logging.error(f"Error creating empty table {full_table_path}: {str(e)}")
        return False
