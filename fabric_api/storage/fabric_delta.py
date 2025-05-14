#!/usr/bin/env python
"""
Delta table operations optimized for Microsoft Fabric.
This module provides streamlined functionality for writing data to Delta tables in OneLake.
"""

import json
import logging
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ..core.fabric_paths import get_error_table_path, get_table_path
from ..core.spark_utils import get_spark_session

# Initialize logger
logger = logging.getLogger(__name__)

# Fabric-optimized Delta write options
FABRIC_DELTA_OPTIONS = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "mergeSchema": "true"
}

def add_metadata_columns(
    df: DataFrame,
    entity_name: str,
    add_timestamp: bool = True
) -> DataFrame:
    """
    Add metadata columns to a DataFrame.

    Args:
        df: Input DataFrame
        entity_name: Name of the entity
        add_timestamp: Whether to add etl_timestamp column

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

def write_to_delta(
    df: DataFrame,
    entity_name: str,
    base_path: str | None = None,
    mode: str = "append",
    partition_cols: list[str] | None = None,
    add_timestamp: bool = True,
    options: dict[str, str] | None = None
) -> tuple[str, int]:
    """
    Write a DataFrame to Delta with Fabric-optimized settings.

    Args:
        df: DataFrame to write
        entity_name: Name of the entity (used for path)
        base_path: Base path to write to (None for default Tables path)
        mode: Write mode (append, overwrite)
        partition_cols: Columns to partition by
        add_timestamp: Whether to add etl_timestamp column
        options: Additional Delta write options

    Returns:
        Tuple of (path, row_count)
    """
    # Check for empty DataFrame
    if df.isEmpty():
        logger.warning(f"DataFrame is empty, skipping write for {entity_name}")
        return get_table_path(entity_name, base_path=base_path), 0

    # Add metadata columns
    df_with_metadata = add_metadata_columns(df, entity_name, add_timestamp)

    # Get proper path
    path = get_table_path(entity_name, base_path=base_path)

    # Prepare write options - combine Fabric options with any provided options
    write_options = dict(FABRIC_DELTA_OPTIONS)
    if options:
        write_options.update(options)

    # Force materialization to avoid streaming expressions
    df_to_write = df_with_metadata.cache()
    row_count = df_to_write.count()
    logger.info(f"Writing {row_count} rows to {path} in {mode} mode")

    # Configure writer with all options in one fluent chain
    writer = df_to_write.write.format("delta").mode(mode).options(**write_options)

    # Add partitioning if specified
    if partition_cols and len(partition_cols) > 0:
        writer = writer.partitionBy(*partition_cols)

    # Write the data - in Fabric this automatically registers the table
    writer.save(path)

    # Clean up
    df_to_write.unpersist()

    return path, row_count

def write_errors(
    errors: list[dict[str, Any]],
    entity_name: str,
    base_path: str | None = None
) -> tuple[str, int]:
    """
    Write validation errors to a Delta table.

    Args:
        errors: List of error dictionaries
        entity_name: Name of the entity that produced the errors
        base_path: Base path for error table

    Returns:
        Tuple of (path, row_count)
    """
    if not errors:
        logger.info(f"No validation errors for {entity_name}")
        return "", 0

    # Get the spark session
    spark = get_spark_session()

    # Create a DataFrame from the errors
    # We need to provide a schema for empty DataFrames

    # Create a minimal schema if errors list is empty
    if not errors:
        schema = StructType([
            StructField("entity", StringType(), True),
            StructField("error_message", StringType(), True)
        ])
        errors_df = spark.createDataFrame([], schema)
    else:
        # Create a simple schema from the first error to avoid type issues

        # Infer the schema from the first record
        first_error = errors[0]

        # Create fields list dynamically
        fields = []
        for key, value in first_error.items():
            if isinstance(value, dict):
                # For dictionaries, use StringType with JSON conversion
                fields.append(StructField(key, StringType(), True))
            elif isinstance(value, list):
                fields.append(StructField(key, StringType(), True))
            elif isinstance(value, int):
                fields.append(StructField(key, IntegerType(), True))
            elif isinstance(value, float):
                fields.append(StructField(key, DoubleType(), True))
            elif isinstance(value, bool):
                fields.append(StructField(key, BooleanType(), True))
            else:
                # Default to string for other types
                fields.append(StructField(key, StringType(), True))

        error_schema = StructType(fields)

        # Convert complex fields to strings if needed
        clean_errors = []
        for error in errors:
            clean_error = {}
            for key, value in error.items():
                if isinstance(value, (dict, list)):
                    clean_error[key] = json.dumps(value)
                else:
                    clean_error[key] = value
            clean_errors.append(clean_error)

        errors_df = spark.createDataFrame(clean_errors, schema=error_schema)

    # Add entity column if not already present
    if "entity" not in errors_df.columns:
        errors_df = errors_df.withColumn("entity", lit(entity_name))

    # Get path for the error table
    error_path = get_error_table_path(base_path=base_path)

    # Write to validation_errors table
    return write_to_delta(
        df=errors_df,
        entity_name="validation_errors",
        base_path=base_path,
        mode="append",
        partition_cols=["entity"]
    )

def dataframe_from_models(
    models: list[Any],
    entity_name: str
) -> DataFrame:
    """
    Create a DataFrame from a list of Pydantic models.

    Args:
        models: List of Pydantic models
        entity_name: Name of the entity

    Returns:
        Spark DataFrame
    """
    spark = get_spark_session()

    if not models:
        logger.warning(f"No models provided for {entity_name}")
        # Create an empty DataFrame
        return spark.createDataFrame([], [])

    try:
        # Get model class
        model_class = models[0].__class__

        # Convert models to dictionaries
        dict_data = [model.model_dump() for model in models]

        # Create DataFrame with schema if possible
        if hasattr(model_class, "model_spark_schema"):
            schema = model_class.model_spark_schema()
            df = spark.createDataFrame(dict_data, schema)
        else:
            # Fall back to schema inference
            df = spark.createDataFrame(dict_data)

        # Add metadata columns
        return add_metadata_columns(df, entity_name)

    except Exception as e:
        logger.warning(f"Error creating DataFrame with schema: {e!s}")
        logger.info("Falling back to schema inference")

        # Create DataFrame with schema inference
        df = spark.createDataFrame(dict_data)

        # Add metadata columns
        return add_metadata_columns(df, entity_name)
