# unified_etl/bronze/reader.py
import logging
from pyspark.sql import DataFrame, SparkSession


def read_bronze_table(spark: SparkSession, table_path: str) -> DataFrame:
    """
    Read table from bronze layer using native Spark.

    Args:
        spark: Active SparkSession
        table_path: Full path to table

    Returns:
        DataFrame containing bronze data
    """
    try:
        df = spark.read.format("delta").load(table_path)
        logging.info(f"Successfully read table from {table_path}")
        return df
    except Exception as e:
        logging.error(f"Failed to read table from {table_path}: {e}")
        raise