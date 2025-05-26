# unified_etl/pipeline/bronze_silver_enhanced.py
"""
Enhanced bronze to silver transformation pipeline with proper separation of concerns.

Silver Layer Responsibilities:
1. Validate with Pydantic models for type safety and structure validation
2. Convert data types (string dates → timestamps, numeric strings → numbers)
3. Flatten nested columns (expand complex objects into flat table structure)
4. Strip prefixes/suffixes from column names if needed
5. Preserve all business data (no column removal, only structure improvement)
"""

from datetime import datetime as dt

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from unified_etl.silver.cleanse import apply_data_types
from unified_etl.silver.flatten import flatten_nested_columns, strip_column_prefixes_suffixes
from unified_etl.silver.scd import apply_scd_type_1, apply_scd_type_2
from unified_etl.silver.standardize import add_standard_audit_columns, get_silver_table_name
from unified_etl.silver.validate import apply_pydantic_validation
from unified_etl.utils import logging, watermark_manager
from unified_etl.utils.exceptions import ColumnStandardizationError
from unified_etl.utils.naming import construct_table_path


class SilverTransformation:
    """
    Handles the bronze to silver transformation with proper layer separation.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_bronze_to_silver(
        self,
        bronze_df: DataFrame,
        entity_name: str,
        table_base_name: str,
        lakehouse_root: str,
        apply_scd: bool = True,
        scd_type: int = 2,
        prefixes_to_strip: list[str] | None = None,
        suffixes_to_strip: list[str] | None = None,
    ) -> DataFrame:
        """
        Transform bronze data to silver with proper separation of concerns.

        Args:
            bronze_df: Raw bronze DataFrame
            entity_name: Entity name for Pydantic validation (e.g., 'Agreement')
            table_base_name: Base table name for configuration lookup
            lakehouse_root: Root path for lakehouse storage
            apply_scd: Whether to apply SCD logic
            scd_type: Type of SCD to apply (1 or 2)
            prefixes_to_strip: API prefixes to remove from column names
            suffixes_to_strip: API suffixes to remove from column names

        Returns:
            Transformed silver DataFrame
        """
        try:
            with logging.span("bronze_to_silver_transformation", entity=entity_name):
                # Step 1: Validate with Pydantic models
                logging.info("Step 1: Applying Pydantic validation", entity=entity_name)
                validated_df = apply_pydantic_validation(bronze_df, entity_name)

                # Step 2: Convert data types (string dates → timestamps, etc.)
                logging.info("Step 2: Converting data types", entity=entity_name)
                typed_df = apply_data_types(validated_df, table_base_name)

                # Step 3: Flatten nested columns
                logging.info("Step 3: Flattening nested columns", entity=entity_name)
                flattened_df = flatten_nested_columns(typed_df, max_depth=3)

                # Step 4: Strip prefixes/suffixes from column names
                if prefixes_to_strip or suffixes_to_strip:
                    logging.info("Step 4: Stripping column prefixes/suffixes", entity=entity_name)
                    cleaned_df = strip_column_prefixes_suffixes(
                        flattened_df, prefixes_to_strip, suffixes_to_strip
                    )
                else:
                    cleaned_df = flattened_df

                # Step 5: Add standard audit columns
                logging.info("Step 5: Adding audit columns", entity=entity_name)
                audited_df = add_standard_audit_columns(cleaned_df)

                # Step 6: Apply SCD logic if requested
                if apply_scd:
                    logging.info(f"Step 6: Applying SCD Type {scd_type}", entity=entity_name)
                    silver_table_path = construct_table_path(
                        lakehouse_root, "silver", get_silver_table_name(table_base_name)
                    )

                    if scd_type == 1:
                        final_df = apply_scd_type_1(audited_df, silver_table_path, entity_name)
                    else:
                        final_df = apply_scd_type_2(audited_df, silver_table_path, entity_name)
                else:
                    final_df = audited_df

                logging.info(
                    "Silver transformation complete",
                    entity=entity_name,
                    input_rows=bronze_df.count(),
                    output_rows=final_df.count(),
                    input_columns=len(bronze_df.columns),
                    output_columns=len(final_df.columns),
                )

                return final_df

        except Exception as e:
            error_msg = f"Bronze to silver transformation failed for {entity_name}: {e!s}"
            logging.error(error_msg, entity=entity_name)
            raise ColumnStandardizationError(error_msg) from e


def run_bronze_to_silver_pipeline(
    spark: SparkSession,
    entities: list[str],
    lakehouse_root: str,
    bronze_layer: str = "bronze",
    silver_layer: str = "silver",
    incremental: bool = True,
    lookback_days: int = 7,
) -> bool:
    """
    Run the complete bronze to silver pipeline for multiple entities.

    Args:
        spark: SparkSession
        entities: List of entity names to process
        lakehouse_root: Root path for lakehouse storage
        bronze_layer: Bronze layer name
        silver_layer: Silver layer name
        incremental: Whether to use incremental processing
        lookback_days: Days to look back for incremental processing

    Returns:
        True if successful, False otherwise
    """
    transformer = SilverTransformation(spark)

    success_count = 0
    total_entities = len(entities)

    with logging.span("bronze_to_silver_pipeline", total_entities=total_entities):
        for entity_name in entities:
            try:
                logging.info(f"Processing entity: {entity_name}")

                # Read from bronze layer
                bronze_table_path = construct_table_path(lakehouse_root, bronze_layer, entity_name)

                if not spark.catalog.tableExists(bronze_table_path):
                    logging.warning(f"Bronze table not found: {bronze_table_path}")
                    continue

                bronze_df = spark.table(bronze_table_path)

                # Apply incremental filtering if requested
                if incremental:
                    last_processed = watermark_manager.get_last_processed_time(
                        entity_name, silver_layer
                    )

                    if last_processed:
                        cutoff_time = last_processed
                    else:
                        cutoff_time = dt.now() - dt.timedelta(days=lookback_days)

                    # Filter for recent changes (assuming SystemModifiedAt exists)
                    if "SystemModifiedAt" in bronze_df.columns:
                        bronze_df = bronze_df.filter(
                            F.col("SystemModifiedAt") >= F.lit(cutoff_time)
                        )

                # Transform to silver
                silver_df = transformer.transform_bronze_to_silver(
                    bronze_df=bronze_df,
                    entity_name=entity_name,
                    table_base_name=entity_name,
                    lakehouse_root=lakehouse_root,
                    apply_scd=True,
                    scd_type=2,
                    prefixes_to_strip=["api_", "sys_"],  # Common API prefixes
                    suffixes_to_strip=["_id", "_ref"],  # Common API suffixes
                )

                # Write to silver layer
                silver_table_path = construct_table_path(lakehouse_root, silver_layer, entity_name)

                silver_df.write.mode("append" if incremental else "overwrite").option(
                    "mergeSchema", "true"
                ).saveAsTable(silver_table_path)

                # Update watermark
                if incremental:
                    watermark_manager.update_watermark(entity_name, silver_layer)

                success_count += 1
                logging.info(f"Successfully processed {entity_name} to silver layer")

            except Exception as e:
                logging.error(f"Failed to process {entity_name}: {e}", entity=entity_name)
                continue

    logging.info(
        "Bronze to silver pipeline complete",
        successful_entities=success_count,
        total_entities=total_entities,
        success_rate=f"{(success_count / total_entities) * 100:.1f}%",
    )

    return success_count == total_entities


# Enhanced Bronze-Silver Pipeline with specific entity configurations
def run_enhanced_bronze_silver_pipeline(
    spark: SparkSession, lakehouse_root: str, entity_configs: list[dict] | None = None
) -> bool:
    """
    Run enhanced bronze to silver pipeline with entity-specific configurations.

    Args:
        spark: SparkSession
        lakehouse_root: Root path for lakehouse storage
        entity_configs: List of entity configuration dictionaries

    Returns:
        True if successful, False otherwise
    """
    if entity_configs is None:
        # Default entity configurations
        entity_configs = [
            {
                "entity_name": "Agreement",
                "table_base_name": "Agreement",
                "prefixes_to_strip": ["api_", "cw_"],
                "suffixes_to_strip": ["_ref", "_id"],
                "scd_type": 2,
            },
            {
                "entity_name": "TimeEntry",
                "table_base_name": "TimeEntry",
                "prefixes_to_strip": ["api_"],
                "suffixes_to_strip": ["_reference"],
                "scd_type": 2,
            },
            {
                "entity_name": "Invoice",
                "table_base_name": "Invoice",
                "prefixes_to_strip": ["api_"],
                "suffixes_to_strip": ["_ref"],
                "scd_type": 1,  # Invoices might use SCD Type 1
            },
        ]

    transformer = SilverTransformation(spark)
    processed_entities = []

    for config in entity_configs:
        try:
            entity_name = config["entity_name"]

            # Read bronze data
            bronze_path = construct_table_path(lakehouse_root, "bronze", entity_name)

            if not spark.catalog.tableExists(bronze_path):
                logging.warning(f"Bronze table not found: {bronze_path}")
                continue

            bronze_df = spark.table(bronze_path)

            # Transform to silver with entity-specific configuration
            silver_df = transformer.transform_bronze_to_silver(
                bronze_df=bronze_df,
                entity_name=entity_name,
                table_base_name=config.get("table_base_name", entity_name),
                lakehouse_root=lakehouse_root,
                apply_scd=True,
                scd_type=config.get("scd_type", 2),
                prefixes_to_strip=config.get("prefixes_to_strip"),
                suffixes_to_strip=config.get("suffixes_to_strip"),
            )

            # Write to silver
            silver_path = construct_table_path(lakehouse_root, "silver", entity_name)

            silver_df.write.mode("append").option("mergeSchema", "true").saveAsTable(silver_path)

            processed_entities.append(entity_name)
            logging.info(f"Enhanced silver processing complete for {entity_name}")

        except Exception as e:
            logging.error(
                f"Enhanced silver processing failed for {config.get('entity_name', 'unknown')}: {e}"
            )
            continue

    logging.info(
        "Enhanced bronze to silver pipeline complete",
        processed_entities=processed_entities,
        total_processed=len(processed_entities),
    )

    return len(processed_entities) > 0
