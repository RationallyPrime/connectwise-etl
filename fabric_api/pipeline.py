#!/usr/bin/env python
"""
Streamlined orchestration pipeline for ConnectWise data ETL.
"""

import logging
from datetime import datetime, timedelta
from typing import Any

from .client import ConnectWiseClient
from .core.config import ENTITY_CONFIG
from .core.spark_utils import get_spark_session
from .extract.generic import extract_entity, get_model_class
from .storage.delta import write_to_delta, write_validation_errors
from .transform.flatten import flatten_dataframe

logger = logging.getLogger(__name__)

def process_entity(
    entity_name: str,
    client: ConnectWiseClient | None = None,
    conditions: str | None = None,
    page_size: int = 100,
    max_pages: int | None = None,
    base_path: str | None = None,
    mode: str = "append",
    flatten_structs: bool = True
) -> dict[str, Any]:
    """
    Process a single entity through the complete ETL pipeline.

    Args:
        entity_name: Name of the entity to process
        client: ConnectWiseClient instance (created if None)
        conditions: API query conditions
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        base_path: Base path for tables
        mode: Write mode (append, overwrite)
        flatten_structs: Whether to flatten nested structures

    Returns:
        Dictionary with processing results
    """
    # Create dependencies if needed
    spark = get_spark_session()
    client = client or ConnectWiseClient()

    # Extract data with validation
    logger.info(f"Extracting {entity_name} data...")
    valid_models, errors = extract_entity(
        client=client,
        entity_name=entity_name,
        page_size=page_size,
        max_pages=max_pages,
        conditions=conditions,
        return_validated=True
    )

    # Check if we have valid data
    if not valid_models:
        logger.warning(f"No valid {entity_name} data extracted")

        # Write errors if present
        if errors:
            logger.warning(f"Writing {len(errors)} validation errors")
            write_validation_errors(spark, entity_name, errors, base_path)

        return {
            "entity": entity_name,
            "extracted": len(errors),
            "validated": 0,
            "errors": len(errors),
            "rows_written": 0,
            "path": ""
        }

    # Convert models to dictionary data for DataFrame
    logger.info(f"Converting {len(valid_models)} valid {entity_name} models to DataFrame")
    dict_data = [model.model_dump() for model in valid_models]

    # Get model schema if available
    model_class = get_model_class(entity_name)
    schema = None
    if hasattr(model_class, "model_spark_schema"):
        schema = model_class.model_spark_schema()

    # Create DataFrame
    if schema:
        df = spark.createDataFrame(dict_data, schema)
    else:
        df = spark.createDataFrame(dict_data)

    # Apply flattening if requested
    if flatten_structs:
        logger.info(f"Flattening nested structures in {entity_name} data")
        df = flatten_dataframe(df)

    # Write to Delta
    logger.info(f"Writing {entity_name} data to Delta")
    path, row_count = write_to_delta(
        df=df,
        entity_name=entity_name,
        base_path=base_path,
        mode=mode,
    )

    # Write errors if present
    if errors:
        logger.info(f"Writing {len(errors)} validation errors")
        write_validation_errors(spark, entity_name, errors, base_path)

    # Return results
    return {
        "entity": entity_name,
        "extracted": len(valid_models) + len(errors),
        "validated": len(valid_models),
        "errors": len(errors),
        "rows_written": row_count,
        "path": path
    }

def process_all_entities(
    entity_names: list[str] | None = None,
    client: ConnectWiseClient | None = None,
    conditions: str | dict[str, str] | None = None,
    page_size: int = 100,
    max_pages: int | None = None,
    base_path: str | None = None,
    mode: str = "append",
    flatten_structs: bool = True
) -> dict[str, dict[str, Any]]:
    """
    Process multiple entity types.

    Args:
        entity_names: List of entity names to process (all if None)
        client: ConnectWiseClient instance (created if None)
        conditions: API query conditions (string or dict mapping entity names to conditions)
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        base_path: Base path for tables
        mode: Write mode (append, overwrite)
        flatten_structs: Whether to flatten nested structures

    Returns:
        Dictionary mapping entity names to result dictionaries
    """
    # Create client if needed
    client = client or ConnectWiseClient()

    # Use all entities if none specified
    if entity_names is None:
        entity_names = list(ENTITY_CONFIG.keys())

    # Process each entity
    results = {}
    for entity_name in entity_names:
        # Get entity-specific conditions if provided as dict
        entity_conditions = None
        if isinstance(conditions, dict):
            entity_conditions = conditions.get(entity_name)
        else:
            entity_conditions = conditions

        # Process entity
        results[entity_name] = process_entity(
            entity_name=entity_name,
            client=client,
            conditions=entity_conditions,
            page_size=page_size,
            max_pages=max_pages,
            base_path=base_path,
            mode=mode,
            flatten_structs=flatten_structs
        )

    return results

def run_incremental_etl(
    start_date: str | None = None,
    end_date: str | None = None,
    entity_names: list[str] | None = None,
    lookback_days: int = 30,
    client: ConnectWiseClient | None = None,
    page_size: int = 100,
    max_pages: int | None = None,
    base_path: str | None = None,
    flatten_structs: bool = True
) -> dict[str, dict[str, Any]]:
    """
    Run an incremental ETL process based on date range.

    Args:
        start_date: Start date for incremental load (YYYY-MM-DD)
        end_date: End date for incremental load (YYYY-MM-DD)
        entity_names: List of entity names to process (all if None)
        lookback_days: Days to look back if no start_date
        client: ConnectWiseClient instance (created if None)
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        base_path: Base path for tables
        flatten_structs: Whether to flatten nested structures

    Returns:
        Dictionary mapping entity names to result dictionaries
    """
    # Calculate date range
    if not start_date:
        start = datetime.now() - timedelta(days=lookback_days)
        start_date = start.strftime("%Y-%m-%d")

    if not end_date:
        end = datetime.now()
        end_date = end.strftime("%Y-%m-%d")

    logger.info(f"Running incremental ETL from {start_date} to {end_date}")

    # Build date-based condition
    condition = f"lastUpdated>=[{start_date}] AND lastUpdated<[{end_date}]"

    # Run process with date condition
    return process_all_entities(
        entity_names=entity_names,
        client=client,
        conditions=condition,
        page_size=page_size,
        max_pages=max_pages,
        base_path=base_path,
        mode="append",
        flatten_structs=flatten_structs
    )

def run_daily_etl(
    entity_names: list[str] | None = None,
    days_back: int = 1,
    client: ConnectWiseClient | None = None,
    page_size: int = 100,
    max_pages: int | None = None,
    base_path: str | None = None,
    flatten_structs: bool = True
) -> dict[str, dict[str, Any]]:
    """
    Run daily ETL process for yesterday's data.

    Args:
        entity_names: List of entity names to process (all if None)
        days_back: Number of days to look back
        client: ConnectWiseClient instance (created if None)
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        base_path: Base path for tables
        flatten_structs: Whether to flatten nested structures

    Returns:
        Dictionary mapping entity names to result dictionaries
    """
    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=days_back)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Today's date
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")

    logger.info(f"Running daily ETL for {yesterday_str}")

    # Use incremental ETL with yesterday's date
    return run_incremental_etl(
        start_date=yesterday_str,
        end_date=today_str,
        entity_names=entity_names,
        client=client,
        page_size=page_size,
        max_pages=max_pages,
        base_path=base_path,
        flatten_structs=flatten_structs
    )
