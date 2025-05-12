#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Delta loading utilities for ConnectWise data.
This module centralizes Delta table writing logic to ensure consistent handling across all entity types.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

# Import OneLake utilities for path construction and table naming
from fabric_api.onelake_utils import (
    get_table_path,
    get_table_name,
    get_partition_columns,
    prepare_dataframe_for_onelake,
    write_to_onelake,
    create_database_if_not_exists
)

# Initialize logger
logger = logging.getLogger(__name__)


def write_to_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_cols: Optional[List[str]] = None,
    add_timestamp: bool = True,
    entity_name: Optional[str] = None,
) -> Tuple[str, int]:
    """
    Write a DataFrame to Delta format.

    Args:
        df: Spark DataFrame to write
        path: Path to write to (if entity_name is provided, this is used as a fallback)
        mode: Write mode (default: append)
        partition_cols: Columns to partition by (optional)
        add_timestamp: Whether to add a processing timestamp column
        entity_name: Optional entity name for OneLake path construction

    Returns:
        Tuple of (path where data was written, number of rows written)
    """
    # If DataFrame is empty, log a warning and return
    if df.isEmpty():
        logger.warning(f"Skipping write to {path} - DataFrame is empty")
        return path, 0

    # If entity_name is provided, use OneLake utilities for path construction
    if entity_name:
        try:
            # Create database if needed
            create_database_if_not_exists(df.sparkSession)

            # Use OneLake write function
            onelake_path, _, row_count = write_to_onelake(
                df=df,
                entity_name=entity_name,
                spark=df.sparkSession,
                mode=mode,
                create_table=True
            )

            # Return the OneLake path and row count
            return onelake_path, row_count

        except Exception as e:
            logger.warning(f"Error writing to OneLake: {str(e)}")
            logger.info(f"Falling back to standard Delta write to {path}")

    # Add processing timestamp if requested
    if add_timestamp:
        timestamp = datetime.utcnow().isoformat()
        df = df.withColumn("processing_timestamp", lit(timestamp))

    # Create a temporary view of the DataFrame to force materialization
    df_to_write = df.cache()
    row_count = df_to_write.count()
    logger.info(f"Writing {row_count} rows to {path}")

    # Configure writer
    writer = df_to_write.write.format("delta").mode(mode)

    # Enable schema merging for backward compatibility
    writer = writer.option("mergeSchema", "true")

    # Add partitioning if specified
    if partition_cols and len(partition_cols) > 0:
        writer = writer.partitionBy(*partition_cols)

    # Write the data
    writer.save(path)
    logger.info(f"Successfully wrote {row_count} rows to {path}")

    # Clean up
    df_to_write.unpersist()

    return path, row_count


def build_delta_path(
    base_path: str,
    entity_name: str,
    use_onelake: bool = True,
) -> str:
    """
    Build a standardized Delta table path.

    Args:
        base_path: Base path for tables (e.g., /lakehouse/default/Tables/connectwise)
        entity_name: Name of the entity (e.g., 'agreement', 'time_entry')
        use_onelake: Whether to use OneLake path conventions (default: True)

    Returns:
        Full path to the Delta table
    """
    if use_onelake:
        try:
            # Use OneLake utilities for path construction
            return get_table_path(entity_name)
        except Exception as e:
            logger.warning(f"Error getting OneLake path: {str(e)}")
            logger.info("Falling back to standard path construction")

    # Normalize entity name (lowercase, snake_case)
    normalized_name = entity_name.lower().replace(" ", "_")

    # Build the path
    full_path = os.path.join(base_path, normalized_name)

    return full_path


def write_validation_errors(
    spark: SparkSession,
    errors: List[Dict[str, Any]],
    base_path: str,
) -> Tuple[str, int]:
    """
    Write validation errors to Delta format.

    Args:
        spark: SparkSession instance
        errors: List of validation error dictionaries
        base_path: Base path for tables

    Returns:
        Tuple of (path where data was written, number of errors written)
    """
    if not errors:
        logger.info("No validation errors to write")
        return "", 0

    # Create a DataFrame from the errors
    errors_df = spark.createDataFrame(errors)

    # Use entity name for OneLake path construction
    entity_name = "validation_errors"

    # Write to Delta using OneLake utilities
    return write_to_delta(
        df=errors_df,
        path=build_delta_path(base_path, entity_name, use_onelake=False),
        mode="append",
        partition_cols=["entity"],
        add_timestamp=True,
        entity_name=entity_name,
    )


def spark_from_model_list(
    spark: SparkSession,
    models: List[Any],
    add_timestamp: bool = True,
    entity_name: Optional[str] = None,
) -> DataFrame:
    """
    Convert a list of Pydantic models to a Spark DataFrame.

    Args:
        spark: SparkSession instance
        models: List of Pydantic models
        add_timestamp: Whether to add a processing timestamp column
        entity_name: Optional entity name for OneLake preparation

    Returns:
        Spark DataFrame
    """
    if not models:
        # For empty lists, create an empty DataFrame with the schema from the model class
        model_class = models[0].__class__ if models else None
        if model_class and hasattr(model_class, "model_spark_schema"):
            schema = model_class.model_spark_schema()
            empty_df = spark.createDataFrame([], schema)

            # If entity_name is provided, prepare for OneLake
            if entity_name:
                empty_df = prepare_dataframe_for_onelake(empty_df, entity_name, add_timestamp, False)
            elif add_timestamp:
                empty_df = empty_df.withColumn("processing_timestamp", lit(None))

            return empty_df
        else:
            # If we can't get the schema, return a truly empty DataFrame
            return spark.createDataFrame([])

    try:
        # Get the model class and schema
        model_class = models[0].__class__
        schema = model_class.model_spark_schema()

        # Convert models to dictionaries
        dict_data = [model.model_dump() for model in models]

        # Create DataFrame with schema
        df = spark.createDataFrame(dict_data, schema)

        # If entity_name is provided, prepare for OneLake
        if entity_name:
            df = prepare_dataframe_for_onelake(df, entity_name, add_timestamp, True)
        elif add_timestamp:
            timestamp = datetime.utcnow().isoformat()
            df = df.withColumn("processing_timestamp", lit(timestamp))

        return df

    except Exception as e:
        logger.warning(f"Error creating DataFrame with schema: {str(e)}")
        logger.info("Falling back to schema inference")

        # Fallback: convert to dictionaries and use schema inference
        dict_data = [model.model_dump() for model in models]
        df = spark.createDataFrame(dict_data)

        # If entity_name is provided, prepare for OneLake
        if entity_name:
            df = prepare_dataframe_for_onelake(df, entity_name, add_timestamp, True)
        elif add_timestamp:
            timestamp = datetime.utcnow().isoformat()
            df = df.withColumn("processing_timestamp", lit(timestamp))

        return df