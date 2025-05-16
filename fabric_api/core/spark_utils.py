#!/usr/bin/env python
"""
Spark utilities optimized for Microsoft Fabric.
"""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """
    Get the active Spark session already initialized in Microsoft Fabric.

    In Fabric, the Spark session is always available and pre-configured.

    Returns:
        The active SparkSession
    """
    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError(
            "No active Spark session found. This code must run in a Microsoft Fabric environment."
        )
    return spark


def table_exists(table_name: str, schema: str | None = None) -> bool:
    """
    Check if a table exists in the Fabric catalog.

    Args:
        table_name: Name of the table
        schema: Optional schema name (database)

    Returns:
        True if the table exists, False otherwise
    """
    spark = get_spark_session()
    try:
        # Construct the full table path if schema is provided
        full_table_path = f"{schema}.{table_name}" if schema else table_name

        # Use Spark catalog API - more reliable than SQL
        if schema:
            return spark.catalog.tableExists(tableName=table_name, dbName=schema)
        else:
            return any(t.name == table_name for t in spark.catalog.listTables())
    except Exception as e:
        logger.warning(f"Error checking if table {table_name} exists: {e!s}")
        return False


def read_delta_table(path: str) -> DataFrame:
    """
    Read a Delta table from the specified path in Fabric.

    Args:
        path: Path to the Delta table in the lakehouse

    Returns:
        DataFrame containing the table data
    """
    spark = get_spark_session()
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        logger.error(f"Error reading Delta table at {path}: {e!s}")
        raise


def create_empty_dataframe(schema) -> DataFrame:
    """
    Create an empty DataFrame with the provided schema.

    Args:
        schema: Schema for the empty DataFrame

    Returns:
        Empty DataFrame with the specified schema
    """
    spark = get_spark_session()
    return spark.createDataFrame([], schema)
