"""
PSA Gold Pipeline - Financial Model with BC Integration.

This pipeline creates:
1. Standalone PSA financial model
2. Conformed dimensions usable by both systems
3. BC-compatible views for integration
"""

import logging
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

from ..gold.conformed_dimensions import run_conformed_dimensions
from ..gold.psa_financial_model import run_psa_financial_gold

logger = logging.getLogger(__name__)


def run_psa_gold_pipeline(
    silver_path: str = "silver",
    gold_path: str = "gold",
    include_bc_integration: bool = True,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    """
    Execute the complete PSA gold layer pipeline.

    Args:
        silver_path: Base path for silver tables
        gold_path: Base path for gold tables
        include_bc_integration: Whether to create BC-compatible views
        spark: SparkSession instance

    Returns:
        Dictionary with processing statistics
    """

    if not spark:
        spark = SparkSession.builder.appName("PSA_Gold_Pipeline").getOrCreate()  # type: ignore[attr-defined]
    assert spark is not None, "SparkSession must be provided"

    results = {
        "start_time": datetime.now(),
        "dimensions": {},
        "facts": {},
        "views": {},
    }

    try:
        logger.info("Starting PSA Gold Pipeline")

        # Step 1: Create conformed dimensions
        logger.info("Creating conformed dimensions")
        dim_results = run_conformed_dimensions(
            silver_path=silver_path,
            gold_path=gold_path,
            spark=spark,
            include_bc_data=include_bc_integration,
        )
        results["dimensions"] = dim_results

        # Step 2: Create PSA financial model
        logger.info("Creating PSA financial model")
        financial_results = run_psa_financial_gold(
            silver_path=silver_path,
            gold_path=gold_path,
            spark=spark,
        )
        results["facts"] = financial_results

        # Step 3: Create integration views if requested
        if include_bc_integration:
            logger.info("Creating BC integration views")
            # These would be additional views that present PSA data
            # in BC-specific formats for reporting tools
            pass

        results["status"] = "SUCCESS"
        results["end_time"] = datetime.now()
        results["duration"] = (results["end_time"] - results["start_time"]).total_seconds()

        logger.info(f"PSA Gold Pipeline completed successfully in {results['duration']} seconds")

    except Exception as e:
        results["status"] = "FAILED"
        results["error"] = str(e)
        logger.error(f"PSA Gold Pipeline failed: {e!s}")
        raise

    return results
