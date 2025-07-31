"""
Business Central-specific Silver layer processor.

This module implements BC-specific silver transformations using SILVER_CONFIG,
handling column operations appropriate for flat tabular BC data exported via BC2ADLS.

Following CLAUDE.md fail-fast principles - ALL parameters required.
"""

# Type hints included in function signatures directly

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from unified_etl_core.utils import (
    ETLConfigError,
    ETLProcessingError,
    ErrorLevel,
    get_logger,
    with_etl_error_handling,
)

from unified_etl_businesscentral.config import SILVER_CONFIG

# Initialize structlog logger from core
logger = get_logger(__name__)


class BusinessCentralSilverProcessor:
    """Process Business Central data from Bronze to Silver layer.

    Handles BC-specific transformations:
    - Column drops (e.g., _etl_bronze_timestamp)
    - Column renames (e.g., No_1 → ItemNo)
    - Calculated columns (e.g., ExtendedAmount = Quantity * DirectUnitCost)
    - Surrogate key generation with company partitioning

    NO JSON flattening or nested structure handling - BC data is already flat.
    """

    spark: SparkSession

    def __init__(self, spark: SparkSession):
        """Initialize BC Silver processor.

        Args:
            spark: Active SparkSession (REQUIRED - fail-fast)

        Raises:
            ETLConfigError: If spark is None
        """
        if not spark:
            raise ETLConfigError("SparkSession is required for BC Silver processor")

        self.spark = spark
        logger.info("bc_silver_processor_initialized")

    @with_etl_error_handling(ErrorLevel.ERROR, operation="process_entity")
    def process_entity(self, entity_name: str, bronze_df: DataFrame) -> DataFrame:
        """Process a single BC entity from Bronze to Silver.

        Args:
            entity_name: Name of the entity (REQUIRED)
            bronze_df: Bronze layer DataFrame (REQUIRED)

        Returns:
            Silver layer DataFrame with transformations applied

        Raises:
            ETLConfigError: If entity not found in SILVER_CONFIG
            ETLProcessingError: If transformation fails
        """
        if not entity_name:
            raise ETLConfigError("entity_name is required")
        if not bronze_df:
            raise ETLConfigError("bronze_df is required")

        # Get entity configuration
        if entity_name not in SILVER_CONFIG:
            raise ETLConfigError(
                f"Entity '{entity_name}' not found in SILVER_CONFIG. Available entities: {list(SILVER_CONFIG.keys())}"
            )

        config = SILVER_CONFIG[entity_name]
        logger.info("bc_silver_processing_entity", entity=entity_name)

        # Start with bronze DataFrame
        silver_df = bronze_df

        # 1. Drop unwanted columns
        drop_columns = config.get("drop_columns", [])
        if drop_columns:
            logger.debug("bc_silver_dropping_columns", columns=drop_columns)
            existing_columns = silver_df.columns
            columns_to_drop = [col for col in drop_columns if col in existing_columns]
            if columns_to_drop:
                silver_df = silver_df.drop(*columns_to_drop)
                logger.info(
                    "bc_silver_columns_dropped", entity=entity_name, count=len(columns_to_drop)
                )

        # 2. Rename columns
        rename_columns = config.get("rename_columns", {})
        if rename_columns:
            logger.debug("bc_silver_renaming_columns", mappings=rename_columns)
            for old_name, new_name in rename_columns.items():
                if old_name in silver_df.columns:
                    silver_df = silver_df.withColumnRenamed(old_name, new_name)
                    logger.debug("bc_silver_column_renamed", old_name=old_name, new_name=new_name)
                else:
                    logger.warning(
                        "bc_silver_rename_column_not_found", column=old_name, entity=entity_name
                    )

        # 3. Add calculated columns
        calculated_columns = config.get("calculated_columns", {})
        if calculated_columns:
            logger.debug(
                "bc_silver_adding_calculated_columns", columns=list(calculated_columns.keys())
            )
            for col_name, expression in calculated_columns.items():
                try:
                    silver_df = silver_df.withColumn(col_name, F.expr(expression))
                    logger.debug("bc_silver_calculated_column_added", column=col_name)
                except Exception as e:
                    raise ETLProcessingError(
                        f"Failed to create calculated column '{col_name}' with expression '{expression}': {e!s}"
                    )

        # 4. Generate surrogate keys
        surrogate_keys = config.get("surrogate_keys", [])
        for sk_config in surrogate_keys:
            silver_df = self._generate_surrogate_key(
                df=silver_df,
                key_name=sk_config["name"],
                business_keys=sk_config["business_keys"],
                entity_name=entity_name,
            )

        # Log final schema and row count
        row_count = silver_df.count()
        logger.info(
            "bc_silver_processing_completed",
            entity=entity_name,
            row_count=row_count,
            column_count=len(silver_df.columns),
        )

        return silver_df

    def _generate_surrogate_key(
        self, df: DataFrame, key_name: str, business_keys: list[str], entity_name: str
    ) -> DataFrame:
        """Generate surrogate key for BC entity with company partitioning.

        Args:
            df: DataFrame to add surrogate key to (REQUIRED)
            key_name: Name of the surrogate key column (REQUIRED)
            business_keys: List of business key columns (REQUIRED)
            entity_name: Name of the entity for logging (REQUIRED)

        Returns:
            DataFrame with surrogate key added

        Raises:
            ETLProcessingError: If business keys not found
        """
        # Validate business keys exist
        missing_keys = [key for key in business_keys if key not in df.columns]
        if missing_keys:
            raise ETLProcessingError(
                f"Business keys {missing_keys} not found in {entity_name}. Available columns: {df.columns}"
            )

        # Determine partition column - use $Company if it's a business key
        partition_cols = ["$Company"] if "$Company" in business_keys else []

        logger.debug(
            "bc_silver_generating_surrogate_key",
            key_name=key_name,
            entity=entity_name,
            business_keys=business_keys,
        )

        # Create window specification
        if partition_cols:
            window_spec = Window.partitionBy(*partition_cols).orderBy(*business_keys)
        else:
            window_spec = Window.orderBy(*business_keys)

        # Generate surrogate key using dense_rank
        df_with_key = df.withColumn(key_name, F.dense_rank().over(window_spec))

        logger.info("bc_silver_surrogate_key_generated", key_name=key_name, entity=entity_name)

        return df_with_key

    def process_all_entities(self, bronze_data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Process all BC entities from Bronze to Silver.

        Args:
            bronze_data: Dictionary of entity_name -> bronze DataFrame (REQUIRED)

        Returns:
            Dictionary of entity_name -> silver DataFrame

        Raises:
            ETLConfigError: If bronze_data is empty
        """
        if not bronze_data:
            raise ETLConfigError("bronze_data dictionary is required and cannot be empty")

        silver_data = {}
        total_entities = len(bronze_data)

        logger.info("bc_silver_processing_all_entities", total_entities=total_entities)

        for idx, (entity_name, bronze_df) in enumerate(bronze_data.items(), 1):
            logger.info(
                "bc_silver_processing_entity_progress",
                current=idx,
                total=total_entities,
                entity=entity_name,
            )

            try:
                silver_df = self.process_entity(entity_name, bronze_df)
                silver_data[entity_name] = silver_df

            except Exception as e:
                logger.error("bc_silver_entity_processing_failed", entity=entity_name, error=str(e))
                raise

        logger.info("bc_silver_all_entities_processed", processed_count=len(silver_data))

        return silver_data
