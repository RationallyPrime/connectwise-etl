#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
OneLake utilities for Microsoft Fabric integration.
This module centralizes OneLake-related functionality including path construction, 
table naming, and partitioning strategy for consistent Delta table writes.
"""

import os
import logging
from typing import List, Optional, Dict, Any, Union, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, to_date, date_format, col

# Initialize logger
logger = logging.getLogger(__name__)

# Constants for OneLake paths and configuration
DEFAULT_LAKEHOUSE_ROOT = "/lakehouse/default"
DEFAULT_TABLES_PATH = f"{DEFAULT_LAKEHOUSE_ROOT}/Tables"
DEFAULT_STORAGE_PATH = f"{DEFAULT_LAKEHOUSE_ROOT}/Files"
DEFAULT_DATABASE = "connectwise"  # The default database name for tables
DEFAULT_TABLE_PREFIX = ""  # Optional prefix for all tables (e.g., "cw_")

# Standard partition columns for different entity types
PARTITION_CONFIG = {
    "agreement": ["agreementStatus"],  # Field in the schema
    "posted_invoice": ["date"],  # Using date field from actual schema
    "unposted_invoice": ["date"],  # Using date field from actual schema
    "time_entry": ["dateEntered"],  # Changed from workDate to dateEntered per schema
    "expense_entry": ["date"],  # Changed from expenseDate to date per schema
    "product_item": [], # No natural partition
    "validation_errors": ["entity", "timestamp_date"],
}


def build_abfss_path(relative_path: str) -> str:
    """
    Convert a relative lakehouse path to a fully qualified ABFSS path for OneLake.
    
    Args:
        relative_path: Relative path from lakehouse root (e.g., "/Tables/connectwise/invoices")
        
    Returns:
        Fully qualified ABFSS path for OneLake
    """
    # Handle paths that already start with abfss://
    if relative_path.startswith("abfss://"):
        return relative_path
        
    # Strip leading slash if present
    if relative_path.startswith("/"):
        relative_path = relative_path[1:]
    
    # Get storage account and tenant ID from environment variables
    storage_account = os.getenv("FABRIC_STORAGE_ACCOUNT")
    tenant_id = os.getenv("FABRIC_TENANT_ID")
    
    # If environment variables are available, build the full abfss path
    if storage_account and tenant_id:
        return f"abfss://lakehouse@{storage_account}.dfs.fabric.microsoft.com/{relative_path}"
    
    # If environment variables aren't available, use the path as is
    # This is useful for local development and testing
    if not relative_path.startswith("/"):
        relative_path = f"/{relative_path}"
    return relative_path


def get_table_path(entity_name: str, table_type: str = "delta") -> str:
    """
    Build a standard table path for an entity.
    
    Args:
        entity_name: Name of the entity (e.g., "Agreement", "TimeEntry")
        table_type: Type of table ("delta" or "parquet")
        
    Returns:
        Full path to the table in OneLake
    """
    # Normalize entity name (lowercase, snake_case)
    normalized_name = entity_name.lower().replace(" ", "_")
    
    # Add prefix if configured
    prefixed_name = f"{DEFAULT_TABLE_PREFIX}{normalized_name}"
    
    # Build the table path
    if table_type == "delta":
        # For Delta tables, use the Tables directory
        table_path = f"{DEFAULT_TABLES_PATH}/{DEFAULT_DATABASE}/{prefixed_name}"
    else:
        # For Parquet files, use the Files directory
        table_path = f"{DEFAULT_STORAGE_PATH}/{DEFAULT_DATABASE}/{prefixed_name}"
    
    # Convert to ABFSS path
    return build_abfss_path(table_path)


def get_table_name(entity_name: str) -> str:
    """
    Get the fully qualified table name for an entity.
    
    Args:
        entity_name: Name of the entity (e.g., "Agreement", "TimeEntry")
        
    Returns:
        Fully qualified table name (e.g., "connectwise.agreement")
    """
    # Normalize entity name (lowercase, snake_case)
    normalized_name = entity_name.lower().replace(" ", "_")
    
    # Add prefix if configured
    prefixed_name = f"{DEFAULT_TABLE_PREFIX}{normalized_name}"
    
    # Return fully qualified name
    return f"{DEFAULT_DATABASE}.{prefixed_name}"


def get_partition_columns(entity_name: str) -> List[str]:
    """
    Get the partition columns for an entity.
    
    Args:
        entity_name: Name of the entity (e.g., "Agreement", "TimeEntry")
        
    Returns:
        List of column names to use for partitioning
    """
    # Normalize entity name (lowercase, snake_case)
    normalized_name = entity_name.lower().replace(" ", "_")
    
    # Return partition columns from config or empty list
    return PARTITION_CONFIG.get(normalized_name, [])


def prepare_dataframe_for_onelake(
    df: DataFrame, 
    entity_name: str,
    add_timestamp: bool = True,
    add_date_partitions: bool = True
) -> DataFrame:
    """
    Prepare a DataFrame for writing to OneLake by adding timestamp and partition columns.
    
    Args:
        df: Spark DataFrame to prepare
        entity_name: Name of the entity
        add_timestamp: Whether to add processing timestamp
        add_date_partitions: Whether to add date-based partition columns
        
    Returns:
        Prepared DataFrame ready for OneLake
    """
    if df.isEmpty():
        return df
    
    prepared_df = df
    
    # Add processing timestamp
    if add_timestamp:
        timestamp = datetime.utcnow().isoformat()
        prepared_df = prepared_df.withColumn("processing_timestamp", lit(timestamp))
    
    # Add date-based partition columns if needed
    if add_date_partitions:
        # Define date columns to process for different entities
        date_columns = {
            "posted_invoice": "date",
            "unposted_invoice": "date",
            "time_entry": "workDate",
            "expense_entry": "expenseDate",
            "agreement": "startDate",
            "validation_errors": "timestamp",
        }
        
        normalized_name = entity_name.lower().replace(" ", "_")
        date_col = date_columns.get(normalized_name)
        
        if date_col and date_col in prepared_df.columns:
            # Create partition columns based on the date
            prepared_df = (
                prepared_df
                .withColumn(f"{date_col}_year", date_format(to_date(col(date_col)), "yyyy"))
                .withColumn(f"{date_col}_month", date_format(to_date(col(date_col)), "MM"))
                .withColumn(f"{date_col}_date", to_date(col(date_col)))
            )
            
            # Add these new columns to the partition config
            if normalized_name in PARTITION_CONFIG:
                for date_part in [f"{date_col}_year", f"{date_col}_month"]:
                    if date_part not in PARTITION_CONFIG[normalized_name]:
                        PARTITION_CONFIG[normalized_name].append(date_part)
    
    return prepared_df


def write_to_onelake(
    df: DataFrame,
    entity_name: str,
    spark: SparkSession,
    mode: str = "append",
    create_table: bool = True,
    table_properties: Optional[Dict[str, str]] = None,
    direct_write: bool = True
) -> Tuple[str, str, int]:
    """
    Write a DataFrame directly to OneLake with proper table creation.

    Args:
        df: Spark DataFrame to write
        entity_name: Name of the entity
        spark: SparkSession instance
        mode: Write mode (append, overwrite, etc.)
        create_table: Whether to create the table using SQL (recommended for Fabric)
        table_properties: Optional Delta table properties
        direct_write: Whether to write directly without intermediate steps

    Returns:
        Tuple of (table_path, table_name, row_count)
    """
    if df.isEmpty():
        logger.warning(f"Skipping write for {entity_name} - DataFrame is empty")
        return "", "", 0

    # Get standardized path and table name
    table_path = get_table_path(entity_name)
    table_name = get_table_name(entity_name)

    # Get partition columns
    partition_cols = get_partition_columns(entity_name)

    # Prepare DataFrame for OneLake
    prepared_df = prepare_dataframe_for_onelake(df, entity_name)

    # Force materialization to cache the DataFrame
    cached_df = prepared_df.cache()
    row_count = cached_df.count()

    logger.info(f"Writing {row_count} {entity_name} records to OneLake at {table_path}")

    # Set default table properties for Fabric
    if table_properties is None:
        table_properties = {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        }

    # For Fabric compatibility, use SQL to create the table
    if create_table:
        try:
            # Drop the table if mode is overwrite
            if mode.lower() == "overwrite":
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                logger.info(f"Dropped table {table_name} for overwrite")

            # Register the DataFrame as a temporary view
            view_name = f"temp_{entity_name.lower()}"
            cached_df.createOrReplaceTempView(view_name)

            # Build the CREATE TABLE statement
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING DELTA
            """

            # Add partitioning if specified
            if partition_cols:
                partition_cols_str = ", ".join(partition_cols)
                create_sql += f"\nPARTITIONED BY ({partition_cols_str})"

            # Add table properties
            props = ", ".join([f"'{k}' = '{v}'" for k, v in table_properties.items()])
            create_sql += f"\nTBLPROPERTIES ({props})"

            # Add location
            create_sql += f"\nLOCATION '{table_path}'"

            # For new tables, add AS SELECT
            if mode.lower() == "overwrite":
                create_sql += f"\nAS SELECT * FROM {view_name}"
            else:
                create_sql += ";"

                # For append, create the table first, then insert
                insert_sql = f"INSERT INTO {table_name} SELECT * FROM {view_name}"

            # Execute the SQL
            logger.info(f"Creating/updating table with SQL: {create_sql}")
            spark.sql(create_sql)

            # For append mode, execute the insert
            if mode.lower() != "overwrite":
                logger.info(f"Inserting data with SQL: {insert_sql}")
                spark.sql(insert_sql)

            logger.info(f"Successfully wrote data to table {table_name}")

        except Exception as e:
            logger.error(f"Error creating table with SQL: {str(e)}")
            logger.warning("Falling back to direct DataFrame write")

            # Fallback to direct DataFrame write
            writer = cached_df.write.format("delta").mode(mode)

            # Add partitioning if specified
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            # Add table properties
            for k, v in table_properties.items():
                writer = writer.option(k, v)

            # Write the data
            writer.save(table_path)

    else:
        # Use direct DataFrame write
        writer = cached_df.write.format("delta").mode(mode)

        # Add partitioning if specified
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        # Add table properties
        for k, v in table_properties.items():
            writer = writer.option(k, v)

        # Write the data
        writer.save(table_path)

    # Clean up
    cached_df.unpersist()

    return table_path, table_name, row_count


def direct_etl_to_onelake(
    client_data: List[Dict[str, Any]],
    entity_name: str,
    spark: SparkSession,
    model_class: Any,
    mode: str = "append"
) -> Tuple[str, int, int]:
    """
    Perform a direct ETL from client data to OneLake without intermediate steps.
    This optimized function combines validation, transformation, and loading
    in a single operation to minimize data movement.

    Args:
        client_data: Raw data from the API
        entity_name: Name of the entity
        spark: SparkSession instance
        model_class: Pydantic model class for validation
        mode: Write mode for the Delta table

    Returns:
        Tuple of (table_path, row_count, error_count)
    """
    from fabric_api import validation

    # Validate the data using Pydantic models
    logger.info(f"Validating {len(client_data)} {entity_name} records...")

    valid_objects = []
    errors = []

    # Validate each record
    for i, raw_dict in enumerate(client_data):
        record_id = raw_dict.get("id", f"Unknown-{i}")
        try:
            # Validate the record against the model
            validated_obj = model_class.model_validate(raw_dict)
            valid_objects.append(validated_obj)
        except Exception as e:
            # Log validation errors
            logger.warning(f"❌ Validation failed for {entity_name} ID {record_id}: {str(e)}")
            errors.append({
                "entity": entity_name,
                "raw_data_id": record_id,
                "errors": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })

    logger.info(f"Validation complete: {len(valid_objects)} valid, {len(errors)} invalid {entity_name} records")

    # Create DataFrame directly from valid objects
    logger.info(f"Converting {len(valid_objects)} valid {entity_name} records to DataFrame...")

    # Try to convert using the model's schema
    try:
        # Convert models to dictionaries
        dict_data = [obj.model_dump() for obj in valid_objects]

        # Create DataFrame with schema
        schema = model_class.model_spark_schema(by_alias=True)
        df = spark.createDataFrame(dict_data, schema)
    except Exception as e:
        logger.warning(f"Error creating DataFrame with schema: {str(e)}")
        logger.info("Falling back to schema inference")

        # Fallback to schema inference
        dict_data = [obj.model_dump() for obj in valid_objects]
        df = spark.createDataFrame(dict_data)

    # Write directly to OneLake
    logger.info(f"Writing {entity_name} data directly to OneLake...")
    table_path, table_name, row_count = write_to_onelake(
        df=df,
        entity_name=entity_name,
        spark=spark,
        mode=mode,
        create_table=True
    )

    # Write validation errors if any
    if errors:
        logger.info(f"Writing {len(errors)} validation errors...")
        errors_df = spark.createDataFrame(errors)
        write_to_onelake(
            df=errors_df,
            entity_name="validation_errors",
            spark=spark,
            mode="append"
        )

    return table_path, row_count, len(errors)


def create_database_if_not_exists(spark: SparkSession) -> None:
    """
    Create the connectwise database if it doesn't already exist.
    
    Args:
        spark: SparkSession instance
    """
    try:
        # Check if database exists
        databases = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]
        
        if DEFAULT_DATABASE not in databases:
            # Create the database
            logger.info(f"Creating database {DEFAULT_DATABASE}")
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {DEFAULT_DATABASE}")
        else:
            logger.info(f"Database {DEFAULT_DATABASE} already exists")
            
    except Exception as e:
        logger.warning(f"Error creating database: {str(e)}")