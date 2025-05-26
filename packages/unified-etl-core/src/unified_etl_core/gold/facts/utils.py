"""
Utility functions for fact table processing.
"""

from pyspark.sql import SparkSession
from unified_etl.gold.generic_fact import create_fact_table
from unified_etl.utils import logging
from unified_etl.utils.config_utils import extract_table_config


def get_fact_table_name(base_name: str) -> str:
    """
    Get the standardized fact table name.

    Args:
        base_name: Base name of the fact table

    Returns:
        Standardized fact table name
    """
    config = extract_table_config(base_name)
    return config.get("gold_name") or f"fact_{base_name.lower()}"


def process_fact_table(
    spark: SparkSession, silver_path: str, gold_path: str, entity_name: str, mode: str = "append"
) -> bool:
    """
    Process a single fact table using the generic fact creator.

    Args:
        spark: Active SparkSession
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        entity_name: Name of the entity to process
        mode: Write mode ("append" or "overwrite")

    Returns:
        True if successful, False otherwise
    """
    try:
        with logging.span("process_fact_table", entity_name=entity_name):
            logging.info(f"Processing fact table for {entity_name}")

            # Read silver table
            silver_table_path = f"{silver_path}.{entity_name}"
            silver_df = spark.table(silver_table_path)

            # Create fact table using generic creator
            fact_df = create_fact_table(
                spark=spark, silver_df=silver_df, entity_name=entity_name, gold_path=gold_path
            )

            # Write to gold layer
            fact_table_name = get_fact_table_name(entity_name)
            fact_table_path = f"{gold_path}.{fact_table_name}"

            fact_df.write.format("delta").mode(mode).saveAsTable(fact_table_path)

            logging.info(f"Successfully processed fact table {fact_table_name}")
            return True

    except Exception as e:
        logging.error(f"Failed to process fact table for {entity_name}: {e!s}")
        return False


def process_gold_layer(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    entities: list[str] | None = None,
    mode: str = "append",
) -> dict[str, bool]:
    """
    Process the entire gold layer for multiple entities.

    Args:
        spark: Active SparkSession
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        entities: List of entities to process (if None, processes all configured)
        mode: Write mode ("append" or "overwrite")

    Returns:
        Dictionary mapping entity names to success status
    """
    results = {}

    # Default entities if none specified
    if entities is None:
        entities = [
            "GLEntry",
            "GLAccount",
            "Customer",
            "Vendor",
            "Item",
            "Job",
            "SalesInvoiceHeader",
            "SalesInvoiceLine",
            "CustLedgerEntry",
            "VendorLedgerEntry",
            "JobLedgerEntry",
        ]

    for entity_name in entities:
        results[entity_name] = process_fact_table(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            entity_name=entity_name,
            mode=mode,
        )

    return results
