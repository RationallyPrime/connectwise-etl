#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Bronze Loader module for ConnectWise data.
Handles fetching data from ConnectWise API, validating it against schemas,
converting it to Spark DataFrames, and writing it to the Bronze layer.
"""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

from pydantic import ValidationError
from pyspark.sql import SparkSession, DataFrame
from sparkdantic import create_spark_schema

from fabric_api.client import ConnectWiseClient
from fabric_api import schemas
from fabric_api.extract.agreements import fetch_agreements_raw
from fabric_api.extract.invoices import fetch_posted_invoices_raw, fetch_unposted_invoices_raw
from fabric_api.extract.time import fetch_time_entries_raw
from fabric_api.extract.expenses import fetch_expense_entries_raw
from fabric_api.extract.products import fetch_product_items_raw

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define entity configurations
ENTITY_CONFIG: dict[str, dict[str, Any]] = {
    "Agreement": {
        "fetch_func": fetch_agreements_raw,
        "schema": schemas.Agreement,
        "partition_cols": ["type_id"]
    },
    "PostedInvoice": {
        "fetch_func": fetch_posted_invoices_raw,
        "schema": schemas.Invoice,
        "partition_cols": ["type"]
    },
    "UnpostedInvoice": {
        "fetch_func": fetch_unposted_invoices_raw,
        "schema": schemas.UnpostedInvoice,
        "partition_cols": ["status_id"]
    },
    "TimeEntry": {
        "fetch_func": fetch_time_entries_raw,
        "schema": schemas.TimeEntry,
        "partition_cols": ["company_type"]
    },
    "ExpenseEntry": {
        "fetch_func": fetch_expense_entries_raw,
        "schema": schemas.ExpenseEntry,
        "partition_cols": ["company_type"]
    },
    "ProductItem": {
        "fetch_func": fetch_product_items_raw,
        "schema": schemas.ProductItem,
        "partition_cols": []
    }
}

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
    Generic function to process a single entity type from fetch to bronze write.
    
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
    
    config = ENTITY_CONFIG[entity_name]
    fetch_func = config["fetch_func"]
    schema_class = config["schema"]
    partition_cols = config["partition_cols"]
    
    # 1. Fetch raw data
    logger.info(f"Fetching raw {entity_name} data...")
    raw_dicts = fetch_func(
        client=client,
        page_size=page_size,
        max_pages=max_pages,
        conditions=conditions
    )
    logger.info(f"Retrieved {len(raw_dicts)} {entity_name} records")
    
    # 2. Validate against schema
    validated_objects = []
    validation_errors = []
    
    for i, raw_dict in enumerate(raw_dicts):
        record_id = raw_dict.get("id", f"Unknown-{i}")
        try:
            validated_obj = schema_class.model_validate(raw_dict)
            validated_objects.append(validated_obj)
        except ValidationError as e:
            logger.warning(f"❌ Validation failed for {entity_name} ID {record_id}: {e.json()}")
            validation_errors.append({
                "entity": entity_name,
                "raw_data_id": record_id,
                "errors": e.errors(),
                "timestamp": datetime.utcnow().isoformat()
            })
    
    logger.info(f"Validation complete: {len(validated_objects)} valid, {len(validation_errors)} invalid {entity_name} records")
    
    # 3. Convert to Spark DataFrame
    if validated_objects:
        # Convert Pydantic objects to dictionaries
        dict_data = [obj.model_dump() for obj in validated_objects]
        # Create Spark schema from Pydantic model
        schema = create_spark_schema(schema_class, by_alias=True)
        # Create DataFrame
        df = spark.createDataFrame(dict_data, schema)
    else:
        # Create empty DataFrame with correct schema
        schema = create_spark_schema(schema_class, by_alias=True)
        df = spark.createDataFrame([], schema)
        logger.info(f"No valid {entity_name} records to process, created empty DataFrame")
    
    # 4. Write to Bronze layer
    entity_path = os.path.join(bronze_path, entity_name.lower())
    logger.info(f"Writing {df.count()} {entity_name} records to {entity_path}")
    
    writer = df.write.mode(write_mode).format("delta")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(entity_path)
    
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
    # Flatten errors
    all_errors = []
    for entity, errors in validation_errors.items():
        all_errors.extend(errors)
    
    if not all_errors:
        logger.info("No validation errors to write")
        return
    
    # Create DataFrame
    errors_df = spark.createDataFrame(all_errors)
    
    # Write to Bronze
    errors_path = os.path.join(bronze_path, "validation_errors")
    errors_df.write.mode("append").format("delta").partitionBy("entity").save(errors_path)
    logger.info(f"Wrote {len(all_errors)} validation errors to {errors_path}")


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

# Set the Bronze layer path
bronze_path = "dbfs:/mnt/bronze/connectwise"

# Process specific entities
results = process_entities(
    spark=spark,
    entity_names=["Agreement", "PostedInvoice", "TimeEntry"],
    bronze_path=bronze_path,
    page_size=100,
    max_pages=1  # Set to None for all pages in production
)

# Or process all configured entities
# results = process_all_entities(spark=spark, bronze_path=bronze_path)

# Show results
for entity_name, (df, errors) in results.items():
    print(f"{entity_name}: {df.count()} records, {len(errors)} validation errors")
    display(df.limit(5))

print("Bronze loading complete")
"""
