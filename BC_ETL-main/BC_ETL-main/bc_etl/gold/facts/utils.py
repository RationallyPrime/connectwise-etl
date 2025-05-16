"""
Utility functions for fact table processing.
"""

from collections.abc import Callable
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from bc_etl.gold.facts.fact_agreement import create_agreement_fact
from bc_etl.gold.facts.fact_finance import create_finance_fact
from bc_etl.gold.facts.fact_item import create_item_fact
from bc_etl.gold.facts.fact_sales import create_sales_fact
from bc_etl.utils import logging
from bc_etl.utils.config_utils import extract_table_config, get_fiscal_settings
from bc_etl.utils.exceptions import FactTableError
from bc_etl.utils.watermark_manager import manage_watermark


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


def resolve_fact_function(fact_type: str) -> Callable:
    """
    Resolve the appropriate fact function based on the fact type.

    Args:
        fact_type: Type of fact to create (e.g., "finance", "sales", "item")

    Returns:
        Function to create the fact table

    Raises:
        FactTableError: If the fact type is not supported
    """
    fact_functions = {
        "finance": create_finance_fact,
        "sales": create_sales_fact,
        "item": create_item_fact,
        "agreement": create_agreement_fact,
    }

    if fact_type.lower() not in fact_functions:
        supported = ", ".join(fact_functions.keys())
        raise FactTableError(f"Unsupported fact type: {fact_type}. Supported types: {supported}")

    return fact_functions[fact_type.lower()]


def process_fact_table(
    spark: SparkSession,
    fact_type: str,
    silver_path: str,
    gold_path: str,
    min_year: int | None = None,
    incremental: bool = False,
    last_processed_time: datetime | None = None,
    fiscal_year_start: int = 1,
) -> DataFrame:
    """
    Process a specific fact table.

    Args:
        spark: Spark session
        fact_type: Type of fact to create
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        min_year: Optional minimum year to include
        incremental: Whether to perform incremental loading
        last_processed_time: Timestamp of last successful processing
        fiscal_year_start: Start month of fiscal year (1-12)

    Returns:
        Processed fact table DataFrame
    """
    # Get fiscal settings if not provided
    if min_year is None or fiscal_year_start == 1:
        fiscal_settings = get_fiscal_settings()
        min_year = min_year or fiscal_settings.get("min_year")
        fiscal_year_start = fiscal_settings.get("fiscal_year_start", 1)

    # Manage watermark for incremental processing
    if incremental:
        last_processed_time = manage_watermark(
            spark=spark,
            table_name=f"fact_{fact_type}",
            is_incremental=incremental,
            last_processed_time=last_processed_time,
            default_lookback_days=30,  # Default to 30 days if no watermark exists
        )

    # Resolve and call the appropriate fact function
    fact_function = resolve_fact_function(fact_type)

    with logging.span(f"process_fact_{fact_type}", incremental=incremental, min_year=min_year):
        return fact_function(
            spark=spark,
            silver_path=silver_path,
            gold_path=gold_path,
            min_year=min_year,
            incremental=incremental,
            last_processed_time=last_processed_time,
            fiscal_year_start=fiscal_year_start,
        )


def process_gold_layer(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    fact_types: list[str] | None = None,
    min_year: int | None = None,
    incremental: bool = False,
    last_processed_time: datetime | None = None,
) -> bool:
    """
    Process all fact tables in the gold layer.

    Args:
        spark: Spark session
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        fact_types: Optional list of fact types to process
        min_year: Optional minimum year to include
        incremental: Whether to perform incremental loading
        last_processed_time: Timestamp of last successful processing

    Returns:
        True if all fact tables were processed successfully, False otherwise
    """
    if fact_types is None:
        fact_types = ["finance", "sales", "item", "agreement"]

    # Get fiscal settings
    fiscal_settings = get_fiscal_settings()
    if min_year is None:
        min_year = fiscal_settings.get("min_year")
    fiscal_year_start = fiscal_settings.get("fiscal_year_start", 1)

    success = True
    for fact_type in fact_types:
        try:
            with logging.span(f"process_{fact_type}_fact"):
                logging.info(f"Processing {fact_type} fact table")

                # Process the fact table
                df = process_fact_table(
                    spark=spark,
                    fact_type=fact_type,
                    silver_path=silver_path,
                    gold_path=gold_path,
                    min_year=min_year,
                    incremental=incremental,
                    last_processed_time=last_processed_time,
                    fiscal_year_start=fiscal_year_start,
                )

                # Get the target table name
                table_name = get_fact_table_name(fact_type)
                full_table_path = f"{gold_path}.{table_name}" if gold_path else table_name

                # Write the fact table
                logging.info(f"Writing {fact_type} fact table to {full_table_path}")
                df.write.format("delta").mode("overwrite").saveAsTable(full_table_path)

                # Log completion
                row_count = df.count()
                logging.info(f"Successfully wrote {row_count} rows to {full_table_path}")

                # Update watermark if incremental
                if incremental:
                    manage_watermark(
                        spark=spark,
                        table_name=f"fact_{fact_type}",
                        is_incremental=True,
                        last_processed_time=datetime.now(),  # Use current time as processing completed
                    )

        except Exception as e:
            logging.error(f"Error processing {fact_type} fact table: {e}", exc_info=True)
            success = False

    return success
