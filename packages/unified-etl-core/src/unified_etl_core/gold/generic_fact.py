"""
Generic fact table creator for all Business Central entities.
Replaces individual fact_* functions with a unified approach.
"""

from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from unified_etl.gold.keys import generate_surrogate_keys
from unified_etl.gold.utils import add_audit_columns, join_dimension
from unified_etl.utils import logging
from unified_etl.utils.config_loader import get_table_config
from unified_etl.utils.exceptions import FactTableError


def create_fact_table(
    spark: SparkSession, silver_df: DataFrame, entity_name: str, gold_path: str, **kwargs: Any
) -> DataFrame:
    """
    Create a fact table for any Business Central entity using configuration-driven approach.

    Args:
        spark: Active SparkSession
        silver_df: Silver layer DataFrame to transform
        entity_name: Name of the entity (e.g., 'GLEntry', 'Customer', etc.)
        gold_path: Path to gold layer for dimension lookups
        **kwargs: Additional configuration options

    Returns:
        Gold layer fact DataFrame with all enrichments

    Raises:
        FactTableError: If fact table creation fails
    """
    try:
        with logging.span("create_fact_table", entity_name=entity_name):
            logging.info(f"Creating fact table for {entity_name}")

            # Get entity configuration
            entity_config = get_table_config(entity_name)
            if not entity_config:
                logging.warning(f"No configuration found for {entity_name}, using defaults")
                entity_config = {}

            fact_df = silver_df

            # 1. Generate surrogate keys if configured
            surrogate_keys = entity_config.get("surrogate_keys", [])
            if surrogate_keys:
                fact_df = generate_surrogate_keys(
                    spark=spark, df=fact_df, key_configs=surrogate_keys, gold_path=gold_path
                )

            # 2. Join with dimensions if configured
            dimension_joins = entity_config.get("dimension_joins", [])
            for dim_join in dimension_joins:
                dimension_name = dim_join.get("dimension")
                join_keys = dim_join.get("join_keys")

                if dimension_name and join_keys:
                    fact_df = join_dimension(
                        spark=spark,
                        fact_df=fact_df,
                        dimension_name=dimension_name,
                        fact_join_keys=join_keys,
                        gold_path=gold_path,
                    )

            # 3. Add calculated columns if configured
            calculated_columns = entity_config.get("calculated_columns", {})
            for col_name, col_expression in calculated_columns.items():
                if col_name not in fact_df.columns:
                    # Parse the expression - this is a simplified approach
                    # In practice, you might want a more sophisticated expression parser
                    fact_df = fact_df.withColumn(col_name, F.expr(col_expression))

            # 4. Add business key columns if configured
            business_keys = entity_config.get("business_keys", [])
            for biz_key in business_keys:
                key_name = biz_key.get("name")
                source_columns = biz_key.get("source_columns", [])

                if key_name and source_columns and key_name not in fact_df.columns:
                    # Create composite business key
                    if len(source_columns) == 1:
                        fact_df = fact_df.withColumn(key_name, F.col(source_columns[0]))
                    else:
                        # Concatenate multiple columns
                        concat_expr = F.concat_ws("_", *[F.col(col) for col in source_columns])
                        fact_df = fact_df.withColumn(key_name, concat_expr)

            # 5. Add standard audit columns
            fact_df = add_audit_columns(fact_df, layer="gold")

            # 6. Add entity identifier column for tracking
            if "EntityType" not in fact_df.columns:
                fact_df = fact_df.withColumn("EntityType", F.lit(entity_name))

            logging.info(f"Created {entity_name} fact table with {fact_df.count()} rows")
            return fact_df

    except Exception as e:
        error_msg = f"Failed to create fact table for {entity_name}: {e!s}"
        logging.error(error_msg)
        raise FactTableError(error_msg) from e


def create_generic_fact_tables(
    spark: SparkSession, silver_path: str, gold_path: str, entities: list[str] | None = None
) -> dict[str, DataFrame]:
    """
    Create fact tables for multiple entities using the generic approach.

    Args:
        spark: Active SparkSession
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        entities: List of entity names to process (if None, processes all configured)

    Returns:
        Dictionary mapping entity names to fact DataFrames
    """
    results = {}

    # If no entities specified, you could read from configuration
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
        try:
            # Read from silver layer
            silver_table_path = f"{silver_path}.{entity_name}"
            silver_df = spark.table(silver_table_path)

            # Create fact table
            fact_df = create_fact_table(
                spark=spark, silver_df=silver_df, entity_name=entity_name, gold_path=gold_path
            )

            results[entity_name] = fact_df

        except Exception as e:
            logging.error(f"Failed to create fact table for {entity_name}: {e!s}")
            results[entity_name] = None

    return results
