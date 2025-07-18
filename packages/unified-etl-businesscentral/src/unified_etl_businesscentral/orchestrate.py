"""
Business Central Gold Layer Orchestration.

Orchestrates the creation of all dimension and fact tables for the warehouse schema.
"""

import logging
from typing import Any

from pyspark.sql import SparkSession
from unified_etl_core.utils.base import ErrorCode
from unified_etl_core.utils.decorators import with_etl_error_handling
from unified_etl_core.utils.exceptions import ETLConfigError

from .config import BC_FACT_CONFIGS, SILVER_CONFIG
from .transforms.facts import create_agreement_fact, create_purchase_fact
from .transforms.global_dimensions import (
    create_date_dimension,
    create_due_date_dimension,
    create_global_dimensions,
)
from .transforms.gold_utils import build_bc_account_hierarchy, create_bc_dimension_bridge


@with_etl_error_handling(operation="orchestrate_bc_gold_layer")
def orchestrate_bc_gold_layer(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    gold_path: str,
    batch_id: str,
    dimension_mapping: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Orchestrate the creation of all BC gold layer tables.

    Args:
        spark: REQUIRED SparkSession
        bronze_path: REQUIRED path to bronze layer
        silver_path: REQUIRED path to silver layer
        gold_path: REQUIRED path to gold layer
        batch_id: REQUIRED batch identifier
        dimension_mapping: Optional BC dimension mapping (defaults to standard mapping)

    Returns:
        Dictionary with processing statistics
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not bronze_path:
        raise ETLConfigError("bronze_path is required", code=ErrorCode.CONFIG_MISSING)
    if not silver_path:
        raise ETLConfigError("silver_path is required", code=ErrorCode.CONFIG_MISSING)
    if not gold_path:
        raise ETLConfigError("gold_path is required", code=ErrorCode.CONFIG_MISSING)
    if not batch_id:
        raise ETLConfigError("batch_id is required", code=ErrorCode.CONFIG_MISSING)

    # Default BC dimension mapping
    if dimension_mapping is None:
        dimension_mapping = {
            "GD1": "TEAM",
            "GD2": "PRODUCT",
            "GD3": "DEPARTMENT",
            "GD4": "PROJECT",
            "GD5": "LOCATION",
            "GD6": "BRAND",
            "GD7": "CHANNEL",
            "GD8": "REGION"
        }

    stats = {
        "dimensions_created": {},
        "facts_created": {}
    }

    logging.info("Starting BC Gold Layer orchestration")

    # Phase 1: Verify Silver layer dimensions exist
    logging.info("Phase 1: Verifying Silver layer dimensions exist")
    missing_silver = []
    for entity_name, entity_config in SILVER_CONFIG.items():
        if entity_config["gold_name"].startswith("dim_"):
            try:
                silver_table_path = f"{silver_path}/{entity_name}"
                df = spark.read.format("delta").load(silver_table_path)
                logging.info(f"Found silver table {entity_name}: {df.count()} rows")
            except Exception as e:
                logging.error(f"Silver table not found for {entity_name}: {e}")
                missing_silver.append(entity_name)

    if missing_silver:
        raise ETLConfigError(
            f"Required silver tables not found: {missing_silver}",
            code=ErrorCode.DATA_ACCESS_ERROR,
            details={"missing_tables": missing_silver, "silver_path": silver_path}
        )

    # Phase 2: Create static dimensions
    logging.info("Phase 2: Creating static dimensions")

    # Create Date dimension
    date_dim = create_date_dimension(spark)
    date_dim.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_Date")
    stats["dimensions_created"]["dim_Date"] = date_dim.count()
    logging.info(f"Created dim_Date: {date_dim.count()} rows")

    # Create Due Date dimension
    due_date_dim = create_due_date_dimension(spark)
    due_date_dim.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_DueDate")
    stats["dimensions_created"]["dim_DueDate"] = due_date_dim.count()
    logging.info(f"Created dim_DueDate: {due_date_dim.count()} rows")

    # Phase 3: Create global dimensions (GD1-GD8)
    logging.info("Phase 3: Creating global dimensions")
    gd_results = create_global_dimensions(
        spark=spark,
        silver_path=silver_path,
        gold_path=gold_path,
        dimension_mapping=dimension_mapping
    )
    for dim_name, dim_df in gd_results.items():
        dim_df.write.mode("overwrite").format("delta").save(f"{gold_path}/{dim_name}")
        stats["dimensions_created"][dim_name] = dim_df.count()
        logging.info(f"Created {dim_name}: {dim_df.count()} rows")

    # Phase 4: Create dimension bridge
    logging.info("Phase 4: Creating dimension bridge")
    bridge_df = create_bc_dimension_bridge(
        spark=spark,
        silver_path=silver_path,
        gold_path=gold_path,
        dimension_types={k: v for k, v in dimension_mapping.items() if k in ["GD1", "GD2"]}
    )
    bridge_df.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_DimensionBridge")
    stats["dimensions_created"]["dim_DimensionBridge"] = bridge_df.count()
    logging.info(f"Created dim_DimensionBridge: {bridge_df.count()} rows")

    # Phase 5: Enhance GLAccount with hierarchy
    logging.info("Phase 5: Enhancing GLAccount with hierarchy")
    # Read the GLAccount dimension
    gl_account_df = spark.read.format("delta").load(f"{silver_path}/GLAccount")

    # Build hierarchy
    gl_with_hierarchy = build_bc_account_hierarchy(
        df=gl_account_df,
        indentation_col="Indentation",
        no_col="No",
        surrogate_key_col="GLAccountKey"
    )

    # Write enhanced dimension
    gl_with_hierarchy.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_GLAccount")
    stats["dimensions_created"]["dim_GLAccount"] = gl_with_hierarchy.count()
    logging.info(f"Enhanced dim_GLAccount: {gl_with_hierarchy.count()} rows")

    # Phase 6: Create fact tables
    logging.info("Phase 6: Creating fact tables")

    # Create Purchase fact
    purchase_fact = create_purchase_fact(
        spark=spark,
        silver_path=silver_path,
        gold_path=gold_path,
        batch_id=batch_id
    )
    purchase_fact.write.mode("overwrite").format("delta").save(f"{gold_path}/fact_Purchase")
    stats["facts_created"]["fact_Purchase"] = purchase_fact.count()
    logging.info(f"Created fact_Purchase: {purchase_fact.count()} rows")

    # Create Agreement fact
    agreement_fact = create_agreement_fact(
        spark=spark,
        silver_path=silver_path,
        gold_path=gold_path,
        batch_id=batch_id
    )
    agreement_fact.write.mode("overwrite").format("delta").save(f"{gold_path}/fact_Agreement")
    stats["facts_created"]["fact_Agreement"] = agreement_fact.count()
    logging.info(f"Created fact_Agreement: {agreement_fact.count()} rows")

    # Create additional facts based on config
    for fact_name, _fact_config in BC_FACT_CONFIGS.items():
        if fact_name not in ["fact_Purchase", "fact_Agreement"]:  # Skip already processed
            # Generic fact processing (would need implementation)
            logging.warning(f"Skipping {fact_name} - needs specific implementation")

    # Summary
    total_dims = sum(stats["dimensions_created"].values())
    total_facts = sum(stats["facts_created"].values())
    logging.info(f"BC Gold Layer orchestration complete: {total_dims} dimension rows, {total_facts} fact rows")

    return stats


@with_etl_error_handling(operation="create_warehouse_schema_mapping")
def create_warehouse_schema_mapping() -> dict[str, str]:
    """
    Create mapping from BC gold tables to warehouse schema tables.

    Returns:
        Dictionary mapping BC table names to warehouse schema names
    """
    return {
        # Dimensions
        "dim_Customer": "dim_Customer",
        "dim_Vendor": "dim_Vendor",
        "dim_Item": "dim_Item",
        "dim_GLAccount": "dim_GLAccount",
        "dim_Currency": "dim_Currency",
        "dim_Company": "dim_Company",
        "dim_Resource": "dim_Resource",
        "dim_Date": "dim_Date",
        "dim_DueDate": "dim_DueDate",
        "dim_GD1": "dim_GD1",
        "dim_GD2": "dim_GD2",
        "dim_GD3": "dim_GD3",
        "dim_GD4": "dim_GD4",
        "dim_GD5": "dim_GD5",
        "dim_GD6": "dim_GD6",
        "dim_GD7": "dim_GD7",
        "dim_GD8": "dim_GD8",

        # Facts
        "fact_Purchase": "fact_Purchase",
        "fact_Agreement": "fact_Agreement",
        "fact_GLEntry": "fact_GLEntry",
        "fact_SalesInvoiceLines": "fact_SalesInvoiceLines",

        # Bridges
        "dim_DimensionBridge": "m2m_DimensionSet",
        "m2m_GLAccount": "m2m_GLAccount"
    }

