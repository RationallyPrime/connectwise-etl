"""Bronze to Silver pipeline leveraging Pydantic models and medallion architecture."""
import logging
import sys
from typing import Dict, Any, Optional, Type

from pydantic import BaseModel
from pyspark.sql import DataFrame

from unified_etl_core.bronze.reader import read_bronze_table
from unified_etl_core.silver.pydantic_processor import process_bronze_to_silver_with_pydantic
from unified_etl_core.utils.config import extract_table_config


def run_bronze_to_silver_pipeline(
    entity_name: str,
    bronze_table_name: str,
    lakehouse_root: str,
    entity_config: Optional[Dict[str, Any]] = None,
    model_class: Optional[Type[BaseModel]] = None
) -> None:
    """
    Run Bronze to Silver pipeline for a specific entity.
    
    This follows the medallion architecture principles:
    - Bronze: Raw data as-is
    - Silver: Validated, standardized, type-converted data using Pydantic models
    
    Args:
        entity_name: Name of entity for Silver table naming
        bronze_table_name: Name of bronze table
        lakehouse_root: Root path for tables
        entity_config: Optional entity configuration
        model_class: Optional Pydantic model for validation
    """
    logging.info(f"Starting Bronze to Silver processing for {entity_name}")
    
    # Use provided config or create default
    if entity_config is None:
        entity_config = extract_table_config(entity_name, {})
    
    # Delegate to Pydantic processor
    process_bronze_to_silver_with_pydantic(
        entity_name=entity_name,
        bronze_table_name=bronze_table_name,
        lakehouse_root=lakehouse_root,
        entity_config=entity_config,
        model_class=model_class
    )
    
    logging.info(f"Completed Bronze to Silver processing for {entity_name}")


def run_bronze_to_silver_with_connectwise_config(
    entity_name: str,
    bronze_table_name: str,
    lakehouse_root: str,
    connectwise_config: Dict[str, Any]
) -> None:
    """
    Run Bronze to Silver pipeline using ConnectWise YAML configuration.
    
    Args:
        entity_name: Name of entity
        bronze_table_name: Name of bronze table
        lakehouse_root: Root path for tables
        connectwise_config: ConnectWise configuration from YAML
    """
    # Extract entity-specific config
    entity_config = connectwise_config.get('entities', {}).get(entity_name, {})
    
    # Try to get the corresponding Pydantic model
    model_class = None
    try:
        # Import ConnectWise models
        from unified_etl_connectwise.models import (
            Agreement, TimeEntry, ExpenseEntry, ProductItem, Invoice
        )
        
        # Map entity names to model classes
        model_mapping = {
            'agreement': Agreement,
            'time_entry': TimeEntry,
            'expense_entry': ExpenseEntry,
            'product_item': ProductItem,
            'invoice': Invoice
        }
        
        model_class = model_mapping.get(entity_name)
        if model_class:
            logging.info(f"Using Pydantic model {model_class.__name__} for {entity_name}")
        else:
            logging.warning(f"No Pydantic model found for {entity_name}")
            
    except ImportError as e:
        logging.warning(f"Could not import ConnectWise models: {e}")
    
    # Run the pipeline
    run_bronze_to_silver_pipeline(
        entity_name=entity_name,
        bronze_table_name=bronze_table_name,
        lakehouse_root=lakehouse_root,
        entity_config=entity_config,
        model_class=model_class
    )