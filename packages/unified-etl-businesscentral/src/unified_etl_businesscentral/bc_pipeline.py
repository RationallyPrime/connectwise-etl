"""
Business Central ETL Pipeline - Dedicated orchestration for BC data flow.

This module provides a BC-specific pipeline that processes BC2ADLS exported data
through Bronze → Silver → Gold layers WITHOUT using the core framework's generic
orchestration. It respects BC's unique data patterns and requirements.

Following CLAUDE.md fail-fast principles - ALL parameters required.
"""

from typing import Any

from pyspark.sql import SparkSession
from unified_etl_core.utils import (
    ETLConfigError,
    ErrorLevel,
    get_logger,
    with_etl_error_handling,
)

from unified_etl_businesscentral.config import BC_FACT_CONFIGS, SILVER_CONFIG
from unified_etl_businesscentral.processor import BusinessCentralBronzeProcessor
from unified_etl_businesscentral.silver_processor import BusinessCentralSilverProcessor
from unified_etl_businesscentral.transforms import facts, gold_utils

# Initialize structlog logger
logger = get_logger(__name__)


def run_bc_pipeline(
    lakehouse_root: str,
    spark: SparkSession | None = None,
    entities: list[str] | None = None,
    layers: list[str] | None = None,
) -> dict[str, Any]:
    """Run the complete Business Central ETL pipeline.

    This is the main entry point for BC ETL processing. Unlike the core framework's
    generic pipeline, this is tailored specifically for BC's data patterns:
    - Bronze: Reads BC2ADLS exported data with validation
    - Silver: Applies BC-specific column operations
    - Gold: Creates facts and dimensions with company-aware logic

    Args:
        lakehouse_root: Root path for lakehouse tables (REQUIRED)
        spark: SparkSession to use, creates one if not provided
        entities: List of entity names to process, None for all
        layers: List of layers to process ('bronze', 'silver', 'gold'), None for all

    Returns:
        Dictionary with processing results and statistics

    Raises:
        ETLConfigError: If required parameters are missing
        ETLProcessingError: If pipeline execution fails
    """
    if not lakehouse_root:
        raise ETLConfigError("lakehouse_root is required")

    # Get or create Spark session
    if not spark:
        # In Fabric, spark is usually available globally
        import sys
        spark = sys.modules["__main__"].spark

    # Type assertion for mypy/basedpyright
    assert spark is not None, "SparkSession must be available"

    # Determine which layers to process
    if not layers:
        layers = ["bronze", "silver", "gold"]

    # Determine which entities to process
    if not entities:
        entities = list(SILVER_CONFIG.keys())

    logger.info(
        "bc_pipeline_started",
        lakehouse_root=lakehouse_root,
        entities_count=len(entities),
        layers=layers,
    )

    results = {"bronze": {}, "silver": {}, "gold": {}}

    try:
        # Run Bronze processing if requested
        if "bronze" in layers:
            results["bronze"] = _run_bronze_layer(
                spark=spark, lakehouse_root=lakehouse_root, entities=entities
            )

        # Run Silver processing if requested
        if "silver" in layers:
            results["silver"] = _run_silver_layer(
                spark=spark,
                lakehouse_root=lakehouse_root,
                entities=entities,
                bronze_results=results.get("bronze", {}),
            )

        # Run Gold processing if requested
        if "gold" in layers:
            results["gold"] = _run_gold_layer(
                spark=spark, lakehouse_root=lakehouse_root, silver_results=results.get("silver", {})
            )

        logger.info("bc_pipeline_completed", results=results)
        return results

    except Exception as e:
        logger.error("bc_pipeline_failed", error=str(e))
        raise


@with_etl_error_handling(ErrorLevel.ERROR)
def _run_bronze_layer(
    spark: SparkSession, lakehouse_root: str, entities: list[str]
) -> dict[str, Any]:
    """Run Bronze layer processing for BC entities.

    Reads BC2ADLS exported data and validates with Pydantic models.
    """
    logger.info("bc_bronze_layer_started", entities_count=len(entities))

    processor = BusinessCentralBronzeProcessor(spark)
    bronze_results = {}

    for entity_name in entities:
        if entity_name not in SILVER_CONFIG:
            logger.warning("bc_bronze_entity_not_configured", entity=entity_name)
            continue

        config = SILVER_CONFIG[entity_name]
        bronze_table = config["bronze_source"]

        try:
            # Process entity through Bronze validation
            validated_df = processor.process_entity(
                entity_name=entity_name,
                table_name=bronze_table,
                output_path=f"{lakehouse_root}/bronze/bc_{entity_name.lower()}",
            )

            bronze_results[entity_name] = {
                "status": "success",
                "row_count": validated_df.count() if validated_df else 0,
            }

        except Exception as e:
            logger.error("bc_bronze_entity_failed", entity=entity_name, error=str(e))
            bronze_results[entity_name] = {"status": "failed", "error": str(e)}

    logger.info("bc_bronze_layer_completed", results=bronze_results)
    return bronze_results


@with_etl_error_handling(ErrorLevel.ERROR)
def _run_silver_layer(
    spark: SparkSession, lakehouse_root: str, entities: list[str], bronze_results: dict[str, Any]
) -> dict[str, Any]:
    """Run Silver layer processing for BC entities.

    Applies BC-specific transformations like column renames and calculations.
    """
    logger.info("bc_silver_layer_started", entities_count=len(entities))

    processor = BusinessCentralSilverProcessor(spark)
    silver_results = {}

    for entity_name in entities:
        # Skip if Bronze failed
        if bronze_results and bronze_results.get(entity_name, {}).get("status") == "failed":
            logger.warning("bc_silver_skipping_failed_bronze", entity=entity_name)
            continue

        try:
            # Read Bronze data
            bronze_path = f"{lakehouse_root}/bronze/bc_{entity_name.lower()}"
            bronze_df = spark.read.format("delta").load(bronze_path)

            # Apply Silver transformations
            silver_df = processor.process_entity(entity_name, bronze_df)

            # Write to Silver layer
            silver_path = f"{lakehouse_root}/silver/bc_{entity_name.lower()}"
            silver_df.write.mode("overwrite").format("delta").save(silver_path)

            silver_results[entity_name] = {
                "status": "success",
                "row_count": silver_df.count(),
                "column_count": len(silver_df.columns),
            }

        except Exception as e:
            logger.error("bc_silver_entity_failed", entity=entity_name, error=str(e))
            silver_results[entity_name] = {"status": "failed", "error": str(e)}

    logger.info("bc_silver_layer_completed", results=silver_results)
    return silver_results


@with_etl_error_handling(ErrorLevel.ERROR)
def _run_gold_layer(
    spark: SparkSession, lakehouse_root: str, silver_results: dict[str, Any]
) -> dict[str, Any]:
    """Run Gold layer processing for BC facts and dimensions.

    Creates dimensional models using BC-specific logic and company-aware joins.
    """
    logger.info("bc_gold_layer_started")

    gold_results = {"dimensions": {}, "facts": {}}

    # First create dimensions (needed for fact table joins)
    gold_results["dimensions"] = _create_bc_dimensions(
        spark=spark, lakehouse_root=lakehouse_root, silver_results=silver_results
    )

    # Then create fact tables
    gold_results["facts"] = _create_bc_facts(
        spark=spark, lakehouse_root=lakehouse_root, silver_results=silver_results
    )

    logger.info("bc_gold_layer_completed", results=gold_results)
    return gold_results


def _create_bc_dimensions(
    spark: SparkSession, lakehouse_root: str, silver_results: dict[str, Any]
) -> dict[str, Any]:
    """Create BC dimension tables in Gold layer."""
    logger.info("bc_gold_dimensions_started")

    dimension_results = {}

    # Create dimension tables from Silver entities marked as dimensions
    dimension_entities = [
        entity
        for entity, config in SILVER_CONFIG.items()
        if config.get("gold_name", "").startswith("dim_")
    ]

    for entity_name in dimension_entities:
        # Skip if Silver failed
        if silver_results.get(entity_name, {}).get("status") == "failed":
            logger.warning("bc_gold_dimension_skipping_failed_silver", entity=entity_name)
            continue

        try:
            # Read Silver data
            silver_path = f"{lakehouse_root}/silver/bc_{entity_name.lower()}"
            silver_df = spark.read.format("delta").load(silver_path)

            # Get gold table name
            gold_name = SILVER_CONFIG[entity_name]["gold_name"]

            # Write dimension to Gold
            gold_path = f"{lakehouse_root}/gold/{gold_name}"
            silver_df.write.mode("overwrite").format("delta").save(gold_path)

            dimension_results[gold_name] = {"status": "success", "row_count": silver_df.count()}

        except Exception as e:
            logger.error("bc_gold_dimension_failed", entity=entity_name, error=str(e))
            dimension_results[entity_name] = {"status": "failed", "error": str(e)}

    # Create special BC dimension bridge
    try:
        if "dimensionsetentry" in [e.lower() for e in silver_results]:
            dim_set_df = spark.read.format("delta").load(
                f"{lakehouse_root}/silver/bc_dimensionsetentry"
            )
            dim_value_df = spark.read.format("delta").load(
                f"{lakehouse_root}/silver/bc_dimensionvalue"
            )

            dimension_bridge_df = gold_utils.create_bc_dimension_bridge(
                spark=spark,
                dimension_set_df=dim_set_df,
                dimension_value_df=dim_value_df,
                dimension_types=["TEAM", "PROJECT", "DEPARTMENT"],
            )

            dimension_bridge_df.write.mode("overwrite").format("delta").save(
                f"{lakehouse_root}/gold/dim_DimensionBridge"
            )

            dimension_results["dim_DimensionBridge"] = {
                "status": "success",
                "row_count": dimension_bridge_df.count(),
            }

    except Exception as e:
        logger.error("bc_gold_dimension_bridge_failed", error=str(e))
        dimension_results["dim_DimensionBridge"] = {"status": "failed", "error": str(e)}

    logger.info("bc_gold_dimensions_completed", results=dimension_results)
    return dimension_results


def _create_bc_facts(
    spark: SparkSession, lakehouse_root: str, silver_results: dict[str, Any]
) -> dict[str, Any]:
    """Create BC fact tables in Gold layer."""
    logger.info("bc_gold_facts_started")

    fact_results = {}

    # Process each configured fact table
    for fact_name, fact_config in BC_FACT_CONFIGS.items():
        try:
            # Check if source entities are available
            source_entities = fact_config["source_entities"]
            missing_entities = [
                e for e in source_entities if silver_results.get(e, {}).get("status") != "success"
            ]

            if missing_entities:
                logger.warning(
                    "bc_gold_fact_missing_sources", fact=fact_name, missing=missing_entities
                )
                continue

            # Get the fact creation function
            if fact_name == "fact_Purchase":
                fact_df = facts.create_purchase_fact(spark=spark, lakehouse_root=lakehouse_root)
            elif fact_name == "fact_Agreement":
                fact_df = facts.create_agreement_fact(spark=spark, lakehouse_root=lakehouse_root)
            else:
                logger.warning("bc_gold_fact_not_implemented", fact=fact_name)
                continue

            # Write fact to Gold
            gold_path = f"{lakehouse_root}/gold/{fact_name}"
            fact_df.write.mode("overwrite").format("delta").save(gold_path)

            fact_results[fact_name] = {"status": "success", "row_count": fact_df.count()}

        except Exception as e:
            logger.error("bc_gold_fact_failed", fact=fact_name, error=str(e))
            fact_results[fact_name] = {"status": "failed", "error": str(e)}

    logger.info("bc_gold_facts_completed", results=fact_results)
    return fact_results
