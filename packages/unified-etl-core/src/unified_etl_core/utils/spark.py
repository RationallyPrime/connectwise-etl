"""Minimal Spark utilities for the unified ETL framework."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def get_session(app_name: str = "UnifiedETL", **spark_conf: str) -> SparkSession:
    """
    Get or create a Spark session with standard configuration.

    Args:
        app_name: Spark application name
        **spark_conf: Additional Spark configuration options

    Returns:
        Configured SparkSession
    """
    builder = SparkSession.builder.appName(app_name)

    # Apply any additional configuration
    for key, value in spark_conf.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def read_delta(spark: SparkSession, path: str) -> DataFrame | None:
    """
    Read a Delta table with basic error handling.

    Args:
        spark: SparkSession
        path: Full path to the Delta table

    Returns:
        DataFrame or None if table doesn't exist
    """
    try:
        return spark.read.format("delta").load(path)
    except Exception as e:
        logger.warning(f"Could not read Delta table at {path}: {e}")
        return None


def write_delta(
    df: DataFrame, path: str, mode: str = "append", partition_by: list[str] | None = None
) -> None:
    """
    Write DataFrame to Delta table.

    Args:
        df: DataFrame to write
        path: Destination path
        mode: Write mode (append, overwrite, etc.)
        partition_by: Optional list of columns to partition by
    """
    writer = df.write.mode(mode).format("delta")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)
    logger.info(f"Wrote {df.count()} rows to {path}")


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Check if a table exists in the catalog.

    Args:
        spark: SparkSession
        table_name: Table name (can be qualified with database)

    Returns:
        True if table exists
    """
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False
