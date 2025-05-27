# unified_etl/gold/enhanced_fact_creator.py
"""
Enhanced fact table creator implementing proper gold layer separation of concerns.

Gold Layer Responsibilities:
1. Add surrogate keys - Generate business-meaningful unique identifiers
2. Create date dimension - Standard calendar table with business periods
3. Build dimension bridge - Connect facts to multiple dimensions
4. Add dimension tables - Normalize entities not present in source
5. Table-specific transforms - Account hierarchy, organizational structures
6. Calculated columns - Business metrics and derived values
7. NO column removal - Only addition and enrichment
"""

from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from unified_etl_core.gold.date_dimension import (
    add_date_dimension_joins,
    create_or_update_date_dimension_table,
)
from unified_etl_core.utils import logging
from unified_etl_core.utils.exceptions import FactTableError
from unified_etl_core.utils.naming import construct_table_path


class GoldLayerEnhancer:
    """
    Handles gold layer enhancement with proper separation of concerns.
    """

    def __init__(self, spark: SparkSession, lakehouse_root: str):
        self.spark = spark
        self.lakehouse_root = lakehouse_root

        # Ensure date dimension exists
        create_or_update_date_dimension_table(spark, lakehouse_root)

    def enhance_silver_to_gold(
        self, silver_df: DataFrame, entity_name: str, entity_config: dict[str, Any] | None = None
    ) -> DataFrame:
        """
        Transform silver data to gold with business intelligence enhancements.

        Args:
            silver_df: Silver layer DataFrame
            entity_name: Name of the entity
            entity_config: Configuration for entity-specific transformations

        Returns:
            Enhanced gold DataFrame
        """
        if entity_config is None:
            entity_config = self._get_default_entity_config(entity_name)

        try:
            with logging.span("silver_to_gold_enhancement", entity=entity_name):
                gold_df = silver_df

                # Step 1: Add surrogate keys
                logging.info("Step 1: Adding surrogate keys", entity=entity_name)
                gold_df = self._add_surrogate_keys(gold_df, entity_name, entity_config)

                # Step 2: Add date dimension joins
                logging.info("Step 2: Adding date dimension joins", entity=entity_name)
                gold_df = self._add_date_dimension_joins(gold_df, entity_config)

                # Step 3: Create/join dimension tables
                logging.info("Step 3: Creating and joining dimensions", entity=entity_name)
                gold_df = self._add_dimension_joins(gold_df, entity_name, entity_config)

                # Step 4: Add calculated business columns
                logging.info("Step 4: Adding calculated columns", entity=entity_name)
                gold_df = self._add_calculated_columns(gold_df, entity_config)

                # Step 5: Apply table-specific transformations
                logging.info("Step 5: Applying table-specific transforms", entity=entity_name)
                gold_df = self._apply_table_specific_transforms(gold_df, entity_name, entity_config)

                # Step 6: Add business metadata
                logging.info("Step 6: Adding business metadata", entity=entity_name)
                gold_df = self._add_business_metadata(gold_df, entity_name)

                logging.info(
                    "Gold layer enhancement complete",
                    entity=entity_name,
                    input_columns=len(silver_df.columns),
                    output_columns=len(gold_df.columns),
                    columns_added=len(gold_df.columns) - len(silver_df.columns),
                )

                return gold_df

        except Exception as e:
            error_msg = f"Gold layer enhancement failed for {entity_name}: {e!s}"
            logging.error(error_msg, entity=entity_name)
            raise FactTableError(error_msg) from e

    def _add_surrogate_keys(
        self, df: DataFrame, entity_name: str, entity_config: dict[str, Any]
    ) -> DataFrame:
        """Add surrogate keys for business identification."""
        surrogate_keys = entity_config.get("surrogate_keys", [])
        result_df = df

        for key_config in surrogate_keys:
            key_name = key_config.get("name")
            source_columns = key_config.get("source_columns", [])
            key_type = key_config.get("type", "hash")  # hash, sequence, composite

            if not key_name or not source_columns:
                continue

            if key_name in df.columns:
                logging.debug(f"Surrogate key {key_name} already exists, skipping")
                continue

            if key_type == "hash":
                # Create hash-based surrogate key
                concat_cols = [F.coalesce(F.col(col), F.lit("NULL")) for col in source_columns]
                hash_input = F.concat_ws("|", *concat_cols)
                result_df = result_df.withColumn(key_name, F.hash(hash_input))

            elif key_type == "sequence":
                # Create sequence-based surrogate key
                result_df = result_df.withColumn(
                    key_name, F.row_number().over(F.Window.orderBy(*source_columns))
                )

            elif key_type == "composite":
                # Create composite key from source columns
                concat_cols = [F.coalesce(F.col(col), F.lit("")) for col in source_columns]
                result_df = result_df.withColumn(key_name, F.concat_ws("_", *concat_cols))

            logging.debug(f"Added surrogate key: {key_name} ({key_type})")

        return result_df

    def _add_date_dimension_joins(self, df: DataFrame, entity_config: dict[str, Any]) -> DataFrame:
        """Add date dimension joins for time intelligence."""
        date_columns = entity_config.get("date_columns", [])

        if not date_columns:
            # Auto-detect common date columns
            date_columns = [
                col
                for col in df.columns
                if any(
                    keyword in col.lower() for keyword in ["date", "time", "created", "modified"]
                )
            ]

        if date_columns:
            return add_date_dimension_joins(df, date_columns, self.lakehouse_root)

        return df

    def _add_dimension_joins(
        self, df: DataFrame, entity_name: str, entity_config: dict[str, Any]
    ) -> DataFrame:
        """Add joins to dimension tables."""
        dimension_joins = entity_config.get("dimension_joins", [])
        result_df = df

        for dim_join in dimension_joins:
            dimension_name = dim_join.get("dimension")
            join_type = dim_join.get("join_type", "left")
            join_condition = dim_join.get("join_condition")

            if not dimension_name or not join_condition:
                continue

            try:
                dim_path = construct_table_path(self.lakehouse_root, "gold", dimension_name)

                if self.spark.catalog.tableExists(dim_path):
                    dim_df = self.spark.table(dim_path)

                    # Simple join condition parsing (enhance as needed)
                    if "=" in join_condition:
                        left_col, right_col = join_condition.split("=")
                        left_col = left_col.strip()
                        right_col = right_col.strip()

                        result_df = result_df.join(
                            dim_df.select("*").alias(f"dim_{dimension_name}"),
                            F.col(left_col) == F.col(f"dim_{dimension_name}.{right_col}"),
                            join_type,
                        )

                        logging.debug(f"Added dimension join: {dimension_name}")
                else:
                    logging.warning(f"Dimension table not found: {dim_path}")

            except Exception as e:
                logging.warning(f"Failed to join dimension {dimension_name}: {e}")
                continue

        return result_df

    def _add_calculated_columns(self, df: DataFrame, entity_config: dict[str, Any]) -> DataFrame:
        """Add calculated business columns."""
        calculated_columns = entity_config.get("calculated_columns", {})
        result_df = df

        for col_name, col_expression in calculated_columns.items():
            if col_name in df.columns:
                logging.debug(f"Calculated column {col_name} already exists, skipping")
                continue

            try:
                # Handle different types of calculations
                if isinstance(col_expression, str):
                    # SQL expression
                    result_df = result_df.withColumn(col_name, F.expr(col_expression))
                elif isinstance(col_expression, dict):
                    # Complex calculation definition
                    calc_type = col_expression.get("type", "expression")

                    if calc_type == "expression":
                        result_df = result_df.withColumn(
                            col_name, F.expr(col_expression.get("sql", "NULL"))
                        )
                    elif calc_type == "aggregate":
                        # Handle aggregate calculations
                        pass  # Implementation depends on specific requirements

                logging.debug(f"Added calculated column: {col_name}")

            except Exception as e:
                logging.warning(f"Failed to add calculated column {col_name}: {e}")
                continue

        return result_df

    def _apply_table_specific_transforms(
        self, df: DataFrame, entity_name: str, entity_config: dict[str, Any]
    ) -> DataFrame:
        """Apply entity-specific business transformations."""

        # Account hierarchy for financial entities
        if entity_name in ["GLAccount", "GLEntry"]:
            df = self._add_account_hierarchy(df)

        # Agreement hierarchy for PSA entities
        if entity_name in ["Agreement", "TimeEntry", "ExpenseEntry"]:
            df = self._add_agreement_hierarchy(df)

        # Customer hierarchy for customer entities
        if entity_name in ["Customer", "Invoice"]:
            df = self._add_customer_hierarchy(df)

        # Apply custom transforms from config
        custom_transforms = entity_config.get("custom_transforms", [])
        for transform in custom_transforms:
            df = self._apply_custom_transform(df, transform)

        return df

    def _add_account_hierarchy(self, df: DataFrame) -> DataFrame:
        """Add account hierarchy for financial entities."""
        if "accountNumber" in df.columns:
            # Extract account hierarchy levels from account number
            df = (
                df.withColumn("AccountLevel1", F.substring(F.col("accountNumber"), 1, 1))
                .withColumn("AccountLevel2", F.substring(F.col("accountNumber"), 1, 3))
                .withColumn("AccountLevel3", F.substring(F.col("accountNumber"), 1, 5))
            )

        return df

    def _add_agreement_hierarchy(self, df: DataFrame) -> DataFrame:
        """Add agreement hierarchy for PSA entities."""
        # Add agreement type groupings, parent agreements, etc.
        return df

    def _add_customer_hierarchy(self, df: DataFrame) -> DataFrame:
        """Add customer hierarchy and segmentation."""
        # Add customer segments, territories, etc.
        return df

    def _apply_custom_transform(self, df: DataFrame, transform: dict[str, Any]) -> DataFrame:
        """Apply a custom transformation."""
        # Implementation for custom transforms
        return df

    def _add_business_metadata(self, df: DataFrame, entity_name: str) -> DataFrame:
        """Add business metadata columns."""
        return (
            df.withColumn("EntityType", F.lit(entity_name))
            .withColumn("GoldProcessedAt", F.current_timestamp())
            .withColumn("DataQualityScore", F.lit(100))
        )  # Placeholder

    def _get_default_entity_config(self, entity_name: str) -> dict[str, Any]:
        """Get default configuration for an entity."""
        default_configs = {
            "Agreement": {
                "surrogate_keys": [
                    {"name": "AgreementSK", "source_columns": ["id"], "type": "hash"}
                ],
                "date_columns": ["dateEntered", "startDate", "endDate"],
                "calculated_columns": {
                    "AgreementDurationDays": "datediff(endDate, startDate)",
                    "IsActive": "CASE WHEN endDate > current_date() THEN true ELSE false END",
                },
            },
            "TimeEntry": {
                "surrogate_keys": [
                    {"name": "TimeEntrySK", "source_columns": ["id"], "type": "hash"}
                ],
                "date_columns": ["timeStart", "timeEnd", "dateEntered"],
                "calculated_columns": {
                    "BillableHours": "CASE WHEN billableOption = 'Billable' THEN actualHours ELSE 0 END",
                    "NonBillableHours": "CASE WHEN billableOption != 'Billable' THEN actualHours ELSE 0 END",
                },
            },
            "Invoice": {
                "surrogate_keys": [{"name": "InvoiceSK", "source_columns": ["id"], "type": "hash"}],
                "date_columns": ["date", "dueDate"],
                "calculated_columns": {
                    "DaysToPayment": "datediff(dueDate, date)",
                    "IsOverdue": "CASE WHEN dueDate < current_date() THEN true ELSE false END",
                },
            },
        }

        return default_configs.get(
            entity_name,
            {
                "surrogate_keys": [
                    {"name": f"{entity_name}SK", "source_columns": ["id"], "type": "hash"}
                ],
                "date_columns": [],
                "calculated_columns": {},
            },
        )


def create_enhanced_fact_table(
    spark: SparkSession,
    silver_df: DataFrame,
    entity_name: str,
    lakehouse_root: str,
    entity_config: dict[str, Any] | None = None,
) -> DataFrame:
    """
    Create an enhanced fact table following gold layer best practices.

    Args:
        spark: SparkSession
        silver_df: Silver layer DataFrame
        entity_name: Name of the entity
        lakehouse_root: Root path for lakehouse storage
        entity_config: Optional configuration for entity-specific transformations

    Returns:
        Enhanced gold layer DataFrame
    """
    enhancer = GoldLayerEnhancer(spark, lakehouse_root)
    return enhancer.enhance_silver_to_gold(silver_df, entity_name, entity_config)


def run_enhanced_gold_pipeline(
    spark: SparkSession,
    lakehouse_root: str,
    entities: list[str],
    entity_configs: dict[str, dict[str, Any]] | None = None,
) -> bool:
    """
    Run the enhanced gold pipeline for multiple entities.

    Args:
        spark: SparkSession
        lakehouse_root: Root path for lakehouse storage
        entities: List of entity names to process
        entity_configs: Optional entity-specific configurations

    Returns:
        True if successful, False otherwise
    """
    entity_configs = entity_configs or {}
    enhancer = GoldLayerEnhancer(spark, lakehouse_root)

    success_count = 0

    for entity_name in entities:
        try:
            logging.info(f"Processing {entity_name} to gold layer")

            # Read from silver
            silver_path = construct_table_path(lakehouse_root, "silver", entity_name)

            if not spark.catalog.tableExists(silver_path):
                logging.warning(f"Silver table not found: {silver_path}")
                continue

            silver_df = spark.table(silver_path)

            # Enhance to gold
            gold_df = enhancer.enhance_silver_to_gold(
                silver_df, entity_name, entity_configs.get(entity_name)
            )

            # Write to gold
            gold_path = construct_table_path(lakehouse_root, "gold", f"Fact{entity_name}")

            gold_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(gold_path)

            success_count += 1
            logging.info(f"Successfully processed {entity_name} to gold layer")

        except Exception as e:
            logging.error(f"Failed to process {entity_name} to gold: {e}")
            continue

    logging.info(
        f"Enhanced gold pipeline complete: {success_count}/{len(entities)} entities processed"
    )

    return success_count == len(entities)
