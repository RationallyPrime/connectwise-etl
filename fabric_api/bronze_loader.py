#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bronze Loader module for ConnectWise data.
Handles fetching data from ConnectWise API, validating it against schemas,
converting it to Spark DataFrames, and writing it to the Bronze layer.
"""

import os
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Union, Type, TypeVar

# Filter Pydantic deprecation warnings for Config options
# This is needed because sparkdantic may still use allow_population_by_field_name
warnings.filterwarnings("ignore", message="Valid config keys have changed in V2")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pydantic import BaseModel, ValidationError

from fabric_api.client import ConnectWiseClient
from fabric_api.connectwise_models import (
    Agreement,
    PostedInvoice,
    UnpostedInvoice,
    TimeEntry,
    ExpenseEntry,
    ProductItem
)
from fabric_api.extract.agreements import fetch_agreements_raw
from fabric_api.extract.invoices import fetch_posted_invoices_raw, fetch_unposted_invoices_raw
from fabric_api.extract.time import fetch_time_entries_raw
from fabric_api.extract.expenses import fetch_expense_entries_raw
from fabric_api.extract.products import fetch_product_items_raw
from fabric_api.extract._common import validate_batch
from fabric_api import log_utils as log

# Initialize ETL logger
logger = log.etl_logger

# Default table prefix for all ConnectWise entities
DEFAULT_TABLE_PREFIX = "cw_"

# Define entity configurations with standardized naming and partitioning
ENTITY_CONFIG = {
    "Agreement": {
        "fetch_func": fetch_agreements_raw,
        "model_class": Agreement,
        "output_table": f"{DEFAULT_TABLE_PREFIX}agreement",
        "partition_cols": [],  # Removed type partitioning as it's a complex type
        "description": "ConnectWise agreements with all related metadata"
    },
    "PostedInvoice": {
        "fetch_func": fetch_posted_invoices_raw,
        "model_class": PostedInvoice,
        "output_table": f"{DEFAULT_TABLE_PREFIX}posted_invoice",
        "partition_cols": [],  # Changed from "date" to avoid schema merge issues
        "description": "ConnectWise posted (finalized) invoices"
    },
    "UnpostedInvoice": {
        "fetch_func": fetch_unposted_invoices_raw,
        "model_class": UnpostedInvoice,
        "output_table": f"{DEFAULT_TABLE_PREFIX}unposted_invoice",
        "partition_cols": [],  # Changed from "date" to avoid schema merge issues
        "description": "ConnectWise unposted (draft) invoices"
    },
    "TimeEntry": {
        "fetch_func": fetch_time_entries_raw,
        "model_class": TimeEntry,
        "output_table": f"{DEFAULT_TABLE_PREFIX}time_entry",
        "partition_cols": [],  # Changed from "date" to avoid schema merge issues
        "description": "ConnectWise time entries for billing and tracking"
    },
    "ExpenseEntry": {
        "fetch_func": fetch_expense_entries_raw,
        "model_class": ExpenseEntry,
        "output_table": f"{DEFAULT_TABLE_PREFIX}expense_entry",
        "partition_cols": [],  # Changed from "date" to avoid schema merge issues
        "description": "ConnectWise expense entries for billing and tracking"
    },
    "ProductItem": {
        "fetch_func": fetch_product_items_raw,
        "model_class": ProductItem,
        "output_table": f"{DEFAULT_TABLE_PREFIX}product_item",
        "partition_cols": [],
        "description": "ConnectWise product items for billing"
    }
}

# Type variable for model class
T = TypeVar('T', bound=BaseModel)

def validate_and_convert(
    raw_data: List[Dict[str, Any]],
    model_class: Type[T]
) -> Tuple[List[T], List[Dict[str, Any]]]:
    """
    Validate raw data against a Pydantic model and return valid models and errors.
    
    Args:
        raw_data: List of raw data dictionaries
        model_class: Pydantic model class to validate against
        
    Returns:
        Tuple of (valid_models, validation_errors)
    """
    return validate_batch(raw_data, model_class)

def create_dataframe(
    spark: SparkSession,
    models: List[BaseModel],
    model_class: Type[BaseModel]
) -> DataFrame:
    """
    Create a Spark DataFrame from a list of Pydantic models using the model's schema.
    
    Args:
        spark: SparkSession instance
        models: List of validated Pydantic models
        model_class: Model class (used to get the schema)
        
    Returns:
        Spark DataFrame with the correct schema
    """
    if not models:
        # Create empty DataFrame with the correct schema
        schema = model_class.model_spark_schema(by_alias=True)
        return spark.createDataFrame([], schema)
    
    # Convert models to dictionaries
    dict_data = [model.model_dump(by_alias=True) for model in models]
    
    try:
        # Try to create with schema
        schema = model_class.model_spark_schema(by_alias=True)
        return spark.createDataFrame(dict_data, schema, verifySchema=False)
    except Exception as e:
        # Fall back to schema inference if schema creation fails
        log.warning(f"Schema-based DataFrame creation failed: {str(e)}. Falling back to schema inference.")
        return spark.createDataFrame(dict_data)

def write_to_delta(
    df: DataFrame,
    table_path: str,
    partition_cols: List[str] = None,
    mode: str = "overwrite"
) -> None:
    """
    Write a DataFrame to Delta Lake format in Microsoft Fabric OneLake.

    Args:
        df: DataFrame to write
        table_path: Path to write to (can be relative or absolute)
        partition_cols: Optional list of columns to partition by
        mode: Write mode (overwrite, append, etc.)
    """
    if df.isEmpty():
        logger.warning(f"DataFrame is empty, skipping write to {table_path}")
        return

    # Ensure path has correct format for Fabric
    fabric_path = ensure_fabric_path(table_path)

    # Check if etl_timestamp already exists (added by add_fabric_metadata)
    if "etl_timestamp" not in df.columns:
        # Add timestamp column for tracking if not already present
        from pyspark.sql.functions import current_timestamp
        df = df.withColumn("etl_timestamp", current_timestamp())

    try:
        # Handle schema evolution with cache and overwrite
        if mode == "append":
            log.warning(f"Detected append mode, switching to overwrite to avoid schema merge issues")
            mode = "overwrite"
            
        # Set up writer with Fabric-optimized options - avoid mergeSchema issues
        writer = df.write.format("delta") \
            .mode(mode) \
            .option("overwriteSchema", "true") \
            .option("delta.autoOptimize.optimizeWrite", "true") \
            .option("delta.autoOptimize.autoCompact", "true")

        # Skip partitioning to avoid schema issues
        # We removed all partition columns from the config above

        # Write the data
        log.etl_progress(f"Writing {df.count()} rows to {fabric_path}")
        writer.save(fabric_path)
        log.info(f"Successfully wrote data to {fabric_path}")

    except Exception as e:
        log.error(f"Failed to write to Delta: {str(e)}")
        # If we got a merge schema error, try with overwrite schema
        if "DELTA_FAILED_TO_MERGE" in str(e):
            log.warning(f"Schema merge issue detected. Trying with overwriteSchema=true and full overwrite mode")
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(fabric_path)
            log.info(f"Successfully wrote data with overwrite schema")
        else:
            # Re-raise other errors
            raise


def ensure_fabric_path(path: str) -> str:
    """
    Ensure a path is properly formatted for Microsoft Fabric OneLake.

    Args:
        path: Input path (can be relative or absolute)

    Returns:
        Path formatted for Fabric OneLake
    """
    # If path is already in abfss format, return it unchanged
    if path.startswith("abfss://"):
        return path

    # Handle paths starting with /lakehouse
    if path.startswith("/lakehouse"):
        # Get environment variables if available
        storage_account = os.getenv("FABRIC_STORAGE_ACCOUNT")
        tenant_id = os.getenv("FABRIC_TENANT_ID")

        # If we have the necessary info, convert to full abfss URL
        if storage_account:
            # Format: abfss://lakehouse@account.dfs.fabric.microsoft.com/path
            return f"abfss://lakehouse@{storage_account}.dfs.fabric.microsoft.com{path}"

    # If it's a simple path like 'mytable', make it a proper lakehouse path
    if not path.startswith("/"):
        # Assume it's relative to the default Tables directory
        return f"/lakehouse/default/Tables/{path}"

    # For any other path format, just return the original
    return path


def add_fabric_metadata(df: DataFrame, entity_name: str, description: str = "") -> DataFrame:
    """
    Add standardized metadata columns to a DataFrame for better tracking in Fabric.

    Args:
        df: Input DataFrame
        entity_name: Name of the entity
        description: Optional description of the entity

    Returns:
        DataFrame with added metadata columns
    """
    from pyspark.sql.functions import lit, current_timestamp

    # Add metadata columns using pyspark.sql.functions which is more compatible
    df = df.withColumn("etl_entity_name", lit("ConnectWise"))
    df = df.withColumn("etl_entity_type", lit(entity_name))
    df = df.withColumn("etl_timestamp", current_timestamp())
    df = df.withColumn("etl_version", lit("1.0"))

    return df


def register_table_metadata(
    spark: SparkSession,
    table_path: str,
    entity_name: str,
    description: str = ""
) -> None:
    """
    Register table metadata in Fabric Lakehouse.

    Args:
        spark: SparkSession instance
        table_path: Path to the table
        entity_name: Name of the entity
        description: Optional description of the entity
    """
    # Extract table name from path
    table_name = os.path.basename(table_path)

    # Check if we're running in Fabric
    try:
        # Try to get Fabric workspace info
        is_fabric = "fabric" in spark.sparkContext.getConf().get("spark.app.name", "").lower() or \
                   "lakehouse" in spark.sparkContext.getConf().get("spark.app.name", "").lower()

        if not is_fabric:
            log.debug("Not running in Fabric, skipping table metadata registration")
            return

        # Create or replace table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{ensure_fabric_path(table_path)}'
        COMMENT '{description or f"ConnectWise {entity_name} data"}'
        """

        spark.sql(create_table_sql)

        # Add table description if supported
        try:
            spark.sql(f"COMMENT ON TABLE {table_name} IS '{description or f'ConnectWise {entity_name} data'}'")
        except:
            # Not all Spark versions support this syntax
            pass

        log.info(f"Registered table {table_name} in Fabric Lakehouse")

    except Exception as e:
        log.warning(f"Failed to register table metadata in Fabric: {str(e)}")
        # Don't propagate the error

def process_entity(
    entity_name: str,
    spark: SparkSession,
    client: ConnectWiseClient,
    bronze_path: str,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    conditions: Optional[str] = None,
    write_mode: str = "overwrite"
) -> Tuple[DataFrame, List[Dict]]:
    """
    Process a single entity type from fetch to bronze write using the schema-driven approach.

    Args:
        entity_name: Name of the entity to process
        spark: SparkSession instance
        client: ConnectWiseClient instance
        bronze_path: Base path for Bronze layer
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        conditions: Conditions to filter the API query
        write_mode: Write mode (overwrite, append)

    Returns:
        Tuple containing the Spark DataFrame and list of validation errors
    """
    if entity_name not in ENTITY_CONFIG:
        raise ValueError(f"Unknown entity: {entity_name}. Must be one of {list(ENTITY_CONFIG.keys())}")

    # Start timing the operation
    with logger.timed_operation(f"process_{entity_name}") as timer:
        config = ENTITY_CONFIG[entity_name]
        fetch_func = config["fetch_func"]
        model_class = config["model_class"]
        partition_cols = config["partition_cols"]
        output_table = config["output_table"]

        # 1. Fetch raw data
        log.etl_progress(f"Starting extraction for {entity_name}")
        try:
            with logger.timed_operation(f"fetch_{entity_name}") as fetch_timer:
                raw_data = fetch_func(
                    client=client,
                    page_size=page_size,
                    max_pages=max_pages,
                    conditions=conditions
                )
                fetch_timer.add_context({"record_count": len(raw_data)})

            logger.info(f"Retrieved {len(raw_data)} {entity_name} records")
        except Exception as e:
            error_entry = logger.etl_error(
                "extraction", e,
                context={
                    "entity": entity_name,
                    "conditions": conditions,
                    "max_pages": max_pages
                }
            )
            # Re-raise with more context
            raise RuntimeError(f"Error extracting {entity_name} data: {str(e)}") from e

        # 2. Validate against schema
        log.etl_progress(f"Validating {entity_name} data")
        try:
            with logger.timed_operation(f"validate_{entity_name}"):
                validated_models, validation_errors = validate_and_convert(raw_data, model_class)

            valid_pct = 100 * len(validated_models) / len(raw_data) if raw_data else 100
            logger.validation(
                f"Validation results for {entity_name}: {len(validated_models)} valid ({valid_pct:.1f}%), "
                f"{len(validation_errors)} invalid records"
            )

            # Log validation error summary if there are errors
            if validation_errors:
                log.validation(f"Validation error summary for {entity_name}:")
                error_summary = logger.summarize_validation_errors(validation_errors)
                timer.add_context({"validation_errors": len(validation_errors)})
        except Exception as e:
            error_entry = logger.etl_error(
                "validation", e,
                context={"entity": entity_name}
            )
            # Re-raise with more context
            raise RuntimeError(f"Error validating {entity_name} data: {str(e)}") from e

        # 3. Convert to Spark DataFrame
        log.etl_progress(f"Converting {entity_name} data to DataFrame")
        try:
            with logger.timed_operation(f"dataframe_{entity_name}"):
                df = create_dataframe(spark, validated_models, model_class)

            row_count = df.count()
            logger.info(f"Created DataFrame with {row_count} rows for {entity_name}")
            timer.add_context({"dataframe_rows": row_count})
        except Exception as e:
            error_entry = logger.etl_error(
                "dataframe_conversion", e,
                context={"entity": entity_name}
            )
            # Re-raise with more context
            raise RuntimeError(f"Error creating DataFrame for {entity_name}: {str(e)}") from e

        # 4. Write to Bronze layer
        log.etl_progress(f"Writing {entity_name} data to Bronze layer")
        try:
            # Add Fabric-specific metadata columns for better tracking
            df = add_fabric_metadata(df, entity_name, config.get("description", ""))

            # Construct entity path
            entity_path = os.path.join(bronze_path, output_table)

            with logger.timed_operation(f"write_{entity_name}_to_delta"):
                # Force overwrite mode to avoid schema issues
                real_write_mode = "overwrite"
                log.info(f"Using {real_write_mode} mode for {entity_name} to avoid schema merge issues")
                write_to_delta(df, entity_path, partition_cols, real_write_mode)

            # Register table metadata in Fabric Lakehouse
            register_table_metadata(
                spark=spark,
                table_path=entity_path,
                entity_name=entity_name,
                description=config.get("description", "")
            )

            logger.info(f"Successfully wrote {entity_name} data to {entity_path}")
            timer.add_context({"output_path": entity_path})
        except Exception as e:
            error_entry = logger.etl_error(
                "delta_write", e,
                context={
                    "entity": entity_name,
                    "output_path": os.path.join(bronze_path, output_table),
                    "write_mode": write_mode
                }
            )
            # Re-raise with more context
            raise RuntimeError(f"Error writing {entity_name} data to Delta: {str(e)}") from e

        log.etl_progress(f"Completed processing for {entity_name}")
        return df, validation_errors


def process_entities(
    spark: SparkSession,
    entity_names: List[str],
    bronze_path: str,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    conditions: Optional[Dict[str, str]] = None,
    write_mode: str = "overwrite"
) -> Dict[str, Tuple[DataFrame, List[Dict]]]:
    """
    Process multiple entity types from ConnectWise API to Bronze layer.
    
    Args:
        spark: SparkSession instance
        entity_names: List of entity names to process
        bronze_path: Base path for Bronze layer
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        conditions: Dictionary mapping entity names to filter conditions
        write_mode: Write mode (overwrite, append)
    
    Returns:
        Dictionary with entity names as keys and tuples of DataFrames and validation errors as values
    """
    
    # Initialize ConnectWise client
    client = ConnectWiseClient()
    
    # Process entities
    results = {}
    all_validation_errors = {}
    
    for entity_name in entity_names:
        entity_condition = None if not conditions else conditions.get(entity_name)
        df, errors = process_entity(
            entity_name=entity_name,
            spark=spark,
            client=client,
            bronze_path=bronze_path,
            page_size=page_size,
            max_pages=max_pages,
            conditions=entity_condition,
            write_mode=write_mode
        )
        results[entity_name] = (df, errors)
        all_validation_errors[entity_name] = errors
    
    # Write validation errors if any
    if any(len(errors) > 0 for errors in all_validation_errors.values()):
        write_validation_errors(spark, all_validation_errors, bronze_path)
    
    return results


def write_validation_errors(
    spark: SparkSession,
    validation_errors: Dict[str, List[Dict]],
    bronze_path: str
) -> None:
    """
    Write validation errors to the Bronze layer.

    Args:
        spark: SparkSession instance
        validation_errors: Dictionary mapping entity names to lists of validation errors
        bronze_path: Base path for Bronze layer
    """
    with logger.timed_operation("write_validation_errors") as timer:
        # Flatten errors
        all_errors = []
        for entity, errors in validation_errors.items():
            all_errors.extend(errors)

        if not all_errors:
            logger.info("No validation errors to write")
            return

        # Log error summary
        log.etl_progress(f"Writing {len(all_errors)} validation errors to Bronze layer")

        try:
            # Create DataFrame
            errors_df = spark.createDataFrame(all_errors)

            # Add metadata columns
            from pyspark.sql.functions import lit, current_timestamp
            errors_df = errors_df.withColumn("etl_timestamp", current_timestamp())
            errors_df = errors_df.withColumn("etl_version", lit("1.0"))

            # Ensure we have the entity column for partitioning
            if "entity" not in errors_df.columns:
                from pyspark.sql.functions import lit
                errors_df = errors_df.withColumn("entity", lit("unknown"))

            # Construct proper path with Fabric formatting
            errors_path = os.path.join(bronze_path, f"{DEFAULT_TABLE_PREFIX}validation_errors")
            fabric_path = ensure_fabric_path(errors_path)

            # Write to Bronze with Fabric-optimized options
            errors_df.write.mode("append") \
                .format("delta") \
                .option("mergeSchema", "true") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .partitionBy("entity") \
                .save(fabric_path)

            # Register table metadata in Fabric Lakehouse
            register_table_metadata(
                spark=spark,
                table_path=errors_path,
                entity_name="ValidationErrors",
                description="Validation errors from ConnectWise API data processing"
            )

            log.validation(f"Wrote {len(all_errors)} validation errors to {fabric_path}")
            timer.add_context({"error_count": len(all_errors), "path": fabric_path})

            # Generate detailed error summary
            error_summary = logger.summarize_validation_errors(all_errors)
            log.validation(f"Validation error statistics: {len(all_errors)} total errors across {len(error_summary['entities'])} entities")

        except Exception as e:
            error_entry = logger.etl_error(
                "error_writing", e,
                context={
                    "error_count": len(all_errors),
                    "output_path": os.path.join(bronze_path, f"{DEFAULT_TABLE_PREFIX}validation_errors")
                }
            )
            # Log but don't re-raise - we don't want error handling to fail
            log.error(f"Failed to write validation errors: {str(e)}")


def process_all_entities(
    spark: SparkSession,
    bronze_path: str,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    conditions: Optional[Dict[str, str]] = None,
    write_mode: str = "overwrite"
) -> Dict[str, Tuple[DataFrame, List[Dict]]]:
    """
    Process all supported entity types.
    
    Args:
        spark: SparkSession instance
        bronze_path: Base path for Bronze layer
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        conditions: Dictionary mapping entity names to filter conditions
        write_mode: Write mode (overwrite, append)
    
    Returns:
        Dictionary with entity names as keys and tuples of DataFrames and validation errors as values
    """
    return process_entities(
        spark=spark,
        entity_names=list(ENTITY_CONFIG.keys()),
        bronze_path=bronze_path,
        page_size=page_size,
        max_pages=max_pages,
        conditions=conditions,
        write_mode=write_mode
    )


# Example notebook usage:
"""
# Initialize the SparkSession in your Fabric notebook
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# Set the Bronze layer path in OneLake
# This path will be automatically formatted for Fabric using ensure_fabric_path()
bronze_path = "/lakehouse/default/Tables/psa_bronze"

# Set environment variables for Fabric storage (optional, improves path resolution)
import os
os.environ["FABRIC_STORAGE_ACCOUNT"] = "your_storage_account_name"  # From your Fabric workspace settings
os.environ["FABRIC_TENANT_ID"] = "your_tenant_id"  # From your Fabric workspace settings

# Process specific entities with date filtering
from datetime import datetime, timedelta
end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

conditions = {
    "TimeEntry": f"date >= {start_date} AND date <= {end_date}",
    "ExpenseEntry": f"date >= {start_date} AND date <= {end_date}",
    "PostedInvoice": f"date >= {start_date} AND date <= {end_date}",
}

# Process just a few entities with date conditions
results = process_entities(
    spark=spark,
    entity_names=["Agreement", "PostedInvoice", "TimeEntry"],
    bronze_path=bronze_path,
    page_size=1000,
    max_pages=None,  # Load all pages
    conditions=conditions,
    write_mode="append"  # Use append for incremental loading with date filters
)

# Or process all configured entities
# results = process_all_entities(
#     spark=spark,
#     bronze_path=bronze_path,
#     conditions=conditions,
#     write_mode="append"
# )

# Show results
for entity_name, (df, errors) in results.items():
    print(f"{entity_name}: {df.count()} records, {len(errors)} validation errors")

    # Show validation error summary if errors exist
    if errors:
        from fabric_api import log_utils as log
        error_summary = log.summarize_validation_errors(errors)
        print(f"Validation error types: {error_summary['fields']}")

    # Display sample data with enhanced schema
    print(f"\n{entity_name} Schema:")
    df.printSchema()
    display(df.limit(5))

# Query the data directly using SQL
spark.sql("SELECT * FROM cw_time_entry WHERE date >= current_date() - INTERVAL 7 DAYS").show(5)

print("Bronze loading complete - All tables are registered in the Fabric Lakehouse catalog")
"""