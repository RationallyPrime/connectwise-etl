#!/usr/bin/env python
"""
Streamlined orchestration pipeline for ConnectWise data ETL.
Optimized for Microsoft Fabric execution environment.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from .client import ConnectWiseClient
from .core.config import ENTITY_CONFIG
from .core.spark_utils import get_spark_session
from .extract.generic import extract_entity, get_model_class
from .storage.fabric_delta import dataframe_from_models, write_errors, write_to_delta
from .transform.dataframe_utils import flatten_all_nested_structures

logger = logging.getLogger(__name__)

def process_entity(
    entity_name: str,
    client: Optional[ConnectWiseClient] = None,
    conditions: Optional[str] = None,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    base_path: Optional[str] = None,
    mode: str = "append",
    flatten_structs: bool = True
) -> Dict[str, Any]:
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
    # Create client if needed
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
            # Make sure errors is a list, not a dict
            error_list = errors if isinstance(errors, list) else [errors]
            _, _ = write_errors(error_list, entity_name, base_path)

        return {
            "entity": entity_name,
            "extracted": len(errors),
            "validated": 0,
            "errors": len(errors),
            "rows_written": 0,
            "path": ""
        }

    # Create DataFrame directly from models using our utility
    logger.info(f"Converting {len(valid_models)} valid {entity_name} models to DataFrame")
    # Ensure valid_models is a list
    model_list = valid_models if isinstance(valid_models, list) else [valid_models]
    df = dataframe_from_models(model_list, entity_name)

    # Apply flattening if requested
    if flatten_structs:
        logger.info(f"Flattening nested structures in {entity_name} data")
        df = flatten_all_nested_structures(df)

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
        # Make sure errors is a list, not a dict
        error_list = errors if isinstance(errors, list) else [errors]
        _, _ = write_errors(error_list, entity_name, base_path)

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
    entity_names: Optional[List[str]] = None,
    client: Optional[ConnectWiseClient] = None,
    conditions: Optional[Union[str, Dict[str, str]]] = None,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    base_path: Optional[str] = None,
    mode: str = "append",
    flatten_structs: bool = True
) -> Dict[str, Dict[str, Any]]:
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
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    entity_names: Optional[List[str]] = None,
    lookback_days: int = 30,
    client: Optional[ConnectWiseClient] = None,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    base_path: Optional[str] = None,
    flatten_structs: bool = True
) -> Dict[str, Dict[str, Any]]:
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
    entity_names: Optional[List[str]] = None,
    days_back: int = 1,
    client: Optional[ConnectWiseClient] = None,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    base_path: Optional[str] = None,
    flatten_structs: bool = True
) -> Dict[str, Dict[str, Any]]:
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