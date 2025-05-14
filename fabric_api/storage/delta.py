#!/usr/bin/env python
"""
Delta table operations for Microsoft Fabric.
This module provides unified functionality for writing data to Delta tables.
"""

import logging
import os
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from ..core.config import DELTA_WRITE_OPTIONS
from ..core.path_utils import get_table_path

# Initialize logger
logger = logging.getLogger(__name__)

def write_to_delta(
    df: DataFrame,
    entity_name: str,
    base_path: str | None = None,
    mode: str = "append",
    partition_cols: list[str] | None = None,
    add_timestamp: bool = True,
    options: dict[str, str] | None = None,
    register_table: bool = True
) -> tuple[str, int]:
    """
    Write a DataFrame to Delta with optimized settings for Fabric.

    Args:
        df: DataFrame to write
        entity_name: Name of the entity (used for path)
        base_path: Base path to write to (None for default Tables path)
        mode: Write mode (append, overwrite)
        partition_cols: Columns to partition by
        add_timestamp: Whether to add etl_timestamp column
        options: Additional Delta write options
        register_table: Whether to register the table in the catalog

    Returns:
        Tuple of (path, row_count)
    """
    # Check for empty DataFrame
    if df.isEmpty():
        logger.warning(f"DataFrame is empty, skipping write for {entity_name}")
        return get_table_path(entity_name, base_path=base_path), 0

    # Get spark session from DataFrame
    spark = df.sparkSession

    # Add metadata columns
    if add_timestamp and "etl_timestamp" not in df.columns:
        df = df.withColumn("etl_timestamp", lit(datetime.utcnow().isoformat()))

    if "etl_entity" not in df.columns:
        df = df.withColumn("etl_entity", lit(entity_name))

    # Get proper path
    path = get_table_path(entity_name, base_path=base_path)

    # Prepare write options - use defaults plus any provided options
    write_options = dict(DELTA_WRITE_OPTIONS)
    if options:
        write_options.update(options)

    # Force materialization to avoid streaming expressions
    df_to_write = df.cache()
    row_count = df_to_write.count()
    logger.info(f"Writing {row_count} rows to {path} in {mode} mode")

    # Configure writer
    writer = df_to_write.write.format("delta").mode(mode)

    # Add all options
    for key, value in write_options.items():
        writer = writer.option(key, value)

    # Add partitioning if specified
    if partition_cols and len(partition_cols) > 0:
        writer = writer.partitionBy(*partition_cols)

    # Write the data
    writer.save(path)

    # Register table in Spark catalog if requested
    if register_table:
        try:
            # Extract table name from path
            table_name = os.path.basename(path)

            # Create table if it doesn't exist
            spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            LOCATION '{path}'
            """)
            logger.info(f"Registered table {table_name}")
        except Exception as e:
            logger.warning(f"Failed to register table: {e!s}")

    # Unpersist to free memory
    df_to_write.unpersist()

    return path, row_count

def prepare_dataframe(
    df: DataFrame,
    entity_name: str,
    add_timestamp: bool = True
) -> DataFrame:
    """
    Prepare a DataFrame for writing by adding metadata columns.

    Args:
        df: Input DataFrame
        entity_name: Name of the entity
        add_timestamp: Whether to add timestamp column

    Returns:
        DataFrame with added metadata columns
    """
    if df.isEmpty():
        return df

    result_df = df

    # Add timestamp if requested
    if add_timestamp and "etl_timestamp" not in result_df.columns:
        result_df = result_df.withColumn("etl_timestamp", lit(datetime.utcnow().isoformat()))

    # Add entity name
    if "etl_entity" not in result_df.columns:
        result_df = result_df.withColumn("etl_entity", lit(entity_name))

    return result_df

def write_validation_errors(
    spark: SparkSession,
    entity_name: str,
    errors: list[dict[str, Any]],
    base_path: str | None = None
) -> tuple[str, int]:
    """
    Write validation errors to a Delta table.

    Args:
        spark: SparkSession
        entity_name: Name of the entity that produced the errors
        errors: List of error dictionaries
        base_path: Base path for error table

    Returns:
        Tuple of (path, row_count)
    """
    if not errors:
        logger.info(f"No validation errors for {entity_name}")
        return "", 0

    # Create a DataFrame from the errors
    errors_df = spark.createDataFrame(errors)

    # Add entity column if not already present
    if "entity" not in errors_df.columns:
        errors_df = errors_df.withColumn("entity", lit(entity_name))

    # Write to validation_errors table
    return write_to_delta(
        df=errors_df,
        entity_name="validation_errors",
        base_path=base_path,
        mode="append",
        partition_cols=["entity"]
    )

def dataframe_from_models(
    spark: SparkSession,
    models: list[Any],
    entity_name: str
) -> DataFrame:
    """
    Create a DataFrame from a list of Pydantic models.

    Args:
        spark: SparkSession
        models: List of Pydantic models
        entity_name: Name of the entity

    Returns:
        Spark DataFrame
    """
    if not models:
        logger.warning(f"No models provided for {entity_name}")
        # For empty lists, create an empty DataFrame with the schema from the model class if possible
        model_class = models[0].__class__ if models else None
        if model_class and hasattr(model_class, "model_spark_schema"):
            schema = model_class.model_spark_schema()
            return prepare_dataframe(
                spark.createDataFrame([], schema),
                entity_name
            )
        else:
            # If we can't get the schema, return a truly empty DataFrame
            return spark.createDataFrame([])

    try:
        # Get model class and schema
        model_class = models[0].__class__

        # Create DataFrame with schema if possible
        if hasattr(model_class, "model_spark_schema"):
            schema = model_class.model_spark_schema()
            dict_data = [model.model_dump() for model in models]
            df = spark.createDataFrame(dict_data, schema)
        else:
            # Fall back to schema inference
            dict_data = [model.model_dump() for model in models]
            df = spark.createDataFrame(dict_data)

        # Add metadata columns
        return prepare_dataframe(df, entity_name)

    except Exception as e:
        logger.warning(f"Error creating DataFrame with schema: {e!s}")
        logger.info("Falling back to schema inference")

        # Convert models to dictionaries
        dict_data = [model.model_dump() for model in models]
        df = spark.createDataFrame(dict_data)

        # Add metadata columns
        return prepare_dataframe(df, entity_name)
