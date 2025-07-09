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
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import ETLConfigError, ETLProcessingError

# Legacy exception aliases for backward compatibility
FactTableError = ETLProcessingError
SurrogateKeyError = ETLProcessingError


def _add_etl_metadata(df: DataFrame, layer: str = "gold", source: str | None = None) -> DataFrame:
    """Add universal ETL metadata columns (inline to avoid circular imports)."""
    if not df:
        raise FactTableError("DataFrame is required")

    metadata_df = df.withColumn(f"_etl_{layer}_processed_at", F.current_timestamp())

    if source:
        metadata_df = metadata_df.withColumn("_etl_source", F.lit(source))

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


def create_generic_fact_table(
    silver_df: DataFrame,
    entity_name: str,
    surrogate_keys: list[dict[str, Any]],
    business_keys: list[dict[str, Any]],
    calculated_columns: dict[str, str],
    source: str,
) -> DataFrame:
    """
    Create fact table using universal patterns. All parameters REQUIRED.

    Args:
        silver_df: REQUIRED silver DataFrame
        entity_name: REQUIRED entity name
        surrogate_keys: REQUIRED surrogate key configurations
        business_keys: REQUIRED business key configurations
        calculated_columns: REQUIRED calculated column configurations
        source: REQUIRED source system name
    """
    if not silver_df:
        raise FactTableError("silver_df is required")
    if not entity_name:
        raise FactTableError("entity_name is required")
    if surrogate_keys is None:
        raise FactTableError("surrogate_keys is required")
    if business_keys is None:
        raise FactTableError("business_keys is required")
    if calculated_columns is None:
        raise FactTableError("calculated_columns is required")
    if not source:
        raise FactTableError("source is required")

    fact_df = silver_df

    # 1. Generate surrogate keys (REQUIRED)
    for key_config in surrogate_keys:
        fact_df = _generate_surrogate_key(
            df=fact_df,
            business_keys=key_config["business_keys"],
            key_name=key_config["name"],
            partition_columns=key_config.get("partition_columns"),
        )

    # 2. Add calculated columns (REQUIRED)
    for col_name, col_expression in calculated_columns.items():
        fact_df = fact_df.withColumn(col_name, F.expr(col_expression))

    # 3. Add business key columns (REQUIRED)
    for biz_key in business_keys:
        source_columns = biz_key["source_columns"]
        key_name = biz_key["name"]

        if len(source_columns) == 1:
            fact_df = fact_df.withColumn(key_name, F.col(source_columns[0]))
        else:
            concat_expr = F.concat_ws("_", *[F.col(col) for col in source_columns])
            fact_df = fact_df.withColumn(key_name, concat_expr)

    # 4. Add universal ETL metadata (REQUIRED)
    fact_df = _add_etl_metadata(fact_df, layer="gold", source=source)

    # 5. Add entity identifier (REQUIRED)
    fact_df = fact_df.withColumn("EntityType", F.lit(entity_name))

    return fact_df
