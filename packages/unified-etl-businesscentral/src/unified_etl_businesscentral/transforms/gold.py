"""Business Central Gold layer integration module.

This module provides the integration point for the unified ETL pipeline's main.py.
It orchestrates the creation of all BC gold layer tables including dimensions and facts.
"""

import logging
from typing import Any

from pyspark.sql import SparkSession

from ..orchestrate import orchestrate_bc_gold_layer

logger = logging.getLogger(__name__)


def create_bc_gold_layer(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    gold_path: str,
    batch_id: str,
    dimension_mapping: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Create Business Central gold layer tables.
    
    This is the main integration point called by unified_etl_core.main.py
    for Business Central gold layer processing.
    
    Args:
        spark: REQUIRED SparkSession
        bronze_path: REQUIRED path to bronze layer
        silver_path: REQUIRED path to silver layer
        gold_path: REQUIRED path to gold layer
        batch_id: REQUIRED batch identifier
        dimension_mapping: Optional BC dimension mapping
        
    Returns:
        Dictionary with processing statistics
    """
    logger.info("Starting Business Central gold layer processing")
    
    return orchestrate_bc_gold_layer(
        spark=spark,
        bronze_path=bronze_path,
        silver_path=silver_path,
        gold_path=gold_path,
        batch_id=batch_id,
        dimension_mapping=dimension_mapping
    )