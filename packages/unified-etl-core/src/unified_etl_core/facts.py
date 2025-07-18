"""
Generic fact table creation - universal patterns only.

Provides configuration-driven fact table creation that works for any domain:
- Surrogate key addition
- ETL metadata enrichment
- Entity type tagging
- Business key creation

Business-specific fact logic delegated to individual packages.
Following CLAUDE.md: Generic where possible, specialized where necessary.
"""

# Inline implementations to avoid circular imports
from datetime import datetime

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from .config import ETLConfig, FactConfig
from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import ETLConfigError, ETLProcessingError


@with_etl_error_handling(operation="add_etl_metadata")
def _add_etl_metadata(df: DataFrame, layer: str, source: str) -> DataFrame:
    """Add universal ETL metadata columns (inline to avoid circular imports)."""
    if not df:
        raise ETLConfigError("DataFrame is required", code=ErrorCode.CONFIG_MISSING)
    if not layer:
        raise ETLConfigError("Layer is required", code=ErrorCode.CONFIG_MISSING)
    if not source:
        raise ETLConfigError("Source is required", code=ErrorCode.CONFIG_MISSING)

    metadata_df = (
        df.withColumn(f"_etl_{layer}_processed_at", F.current_timestamp())
        .withColumn("_etl_source", F.lit(source))
    )

    metadata_df = metadata_df.withColumn(
        "_etl_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
    )

    return metadata_df


@with_etl_error_handling(operation="generate_surrogate_key")
def _generate_surrogate_key(
    df: DataFrame,
    business_keys: list[str],
    key_name: str,
    partition_columns: list[str] | None = None,
    start_value: int = 1,
) -> DataFrame:
    """Generate surrogate keys (inline to avoid circular imports)."""
    if not df:
        raise ETLConfigError("DataFrame is required", code=ErrorCode.CONFIG_MISSING)
    if not business_keys:
        raise ETLConfigError("business_keys list is required and cannot be empty", code=ErrorCode.CONFIG_MISSING)
    if not key_name:
        raise ETLConfigError("key_name is required", code=ErrorCode.CONFIG_MISSING)

    # Validate business keys exist
    missing_keys = [k for k in business_keys if k not in df.columns]
    if missing_keys:
        raise ETLProcessingError(
            f"Business key columns not found in DataFrame: {missing_keys}",
            code=ErrorCode.GOLD_SURROGATE_KEY,
            details={"missing_keys": missing_keys, "available_columns": df.columns}
        )

    try:
        # Create window spec
        if partition_columns:
            window = Window.partitionBy(*[F.col(k) for k in partition_columns]).orderBy(
                *[F.col(k) for k in business_keys]
            )
        else:
            window = Window.orderBy(*[F.col(k) for k in business_keys])

        # Generate surrogate key
        result_df = df.withColumn(
            key_name, (F.dense_rank().over(window) + F.lit(start_value - 1)).cast("int")
        )

        return result_df

    except Exception as e:
        raise ETLProcessingError(
            f"Surrogate key generation failed: {e}",
            code=ErrorCode.GOLD_SURROGATE_KEY,
            details={"key_name": key_name, "business_keys": business_keys, "error": str(e)}
        ) from e


@with_etl_error_handling(operation="create_generic_fact_table")
def create_generic_fact_table(
    config: ETLConfig,
    fact_config: FactConfig,
    silver_df: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    """
    Create fact table using universal patterns. All parameters REQUIRED.

    Args:
        config: REQUIRED ETL configuration
        fact_config: REQUIRED fact table configuration
        silver_df: REQUIRED silver DataFrame
        spark: REQUIRED SparkSession
    """
    # Validate configs - FAIL FAST
    fact_config.validate_config()

    if not silver_df:
        raise ETLConfigError("silver_df is required", code=ErrorCode.CONFIG_MISSING)
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)

    fact_df = silver_df

    # 1. Generate surrogate keys (REQUIRED)
    for surrogate_key in fact_config.surrogate_keys:
        fact_df = _generate_surrogate_key(
            df=fact_df,
            business_keys=fact_config.business_keys,
            key_name=surrogate_key,
            partition_columns=None,
        )

    # 2. Add calculated columns (REQUIRED)
    for calc_col in fact_config.calculated_columns:
        fact_df = fact_df.withColumn(calc_col.name, F.expr(calc_col.expression))

    # 3. Create composite business key if multiple keys
    if len(fact_config.business_keys) > 1:
        concat_expr = F.concat_ws("_", *[F.col(col) for col in fact_config.business_keys])
        fact_df = fact_df.withColumn("BusinessKey", concat_expr)
    else:
        fact_df = fact_df.withColumn("BusinessKey", F.col(fact_config.business_keys[0]))

    # 4. Add universal ETL metadata (REQUIRED)
    if fact_config.add_audit_columns:
        fact_df = _add_etl_metadata(fact_df, layer="gold", source=fact_config.source)

    # 5. Add entity identifier if multi-entity fact
    if fact_config.add_entity_type:
        fact_df = fact_df.withColumn(
            fact_config.entity_type_column,
            F.lit(fact_config.name)
        )

    # 6. Select final columns in proper order
    final_columns = (
        fact_config.surrogate_keys +
        fact_config.business_keys +
        fact_config.dimension_columns +
        fact_config.measure_columns +
        [col.name for col in fact_config.calculated_columns]
    )

    if fact_config.add_entity_type:
        final_columns.append(fact_config.entity_type_column)

    if fact_config.add_audit_columns:
        final_columns.extend([
            "_etl_gold_processed_at",
            "_etl_source",
            "_etl_batch_id"
        ])

    return fact_df.select(*final_columns)
