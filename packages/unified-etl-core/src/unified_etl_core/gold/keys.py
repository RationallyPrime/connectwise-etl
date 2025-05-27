# unified_etl/gold/keys.py

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from unified_etl_core.utils import logging
from unified_etl_core.utils.exceptions import SurrogateKeyError
from unified_etl_core.utils.naming import standardize_table_reference


def generate_surrogate_key(
    df: DataFrame,
    business_keys: list[str],
    key_name: str | None = None,
    table_name: str | None = None,
    company_partition: bool = True,
    start_value: int = 1,
) -> DataFrame:
    """
    Generate consistent integer surrogate keys for dimensions with proper company partitioning.

    Args:
        df: DataFrame to process
        business_keys: List of columns forming the business key
        key_name: Name for the surrogate key column (derived from table_name if None)
        table_name: Base table name for key name derivation
        company_partition: Whether to partition by company
        start_value: Starting value for surrogate key

    Returns:
        DataFrame with surrogate key added

    Raises:
        SurrogateKeyError: If surrogate key generation fails
    """
    try:
        # Determine key name
        if key_name is None:
            if table_name is None:
                raise SurrogateKeyError("Either key_name or table_name must be provided")
            # Use standardize_table_reference to get proper case name without numeric suffix
            base_name = standardize_table_reference(table_name, include_suffix=False)
            key_name = f"{base_name}Key"

        span_attrs = {"key_name": key_name, "business_keys": business_keys}
        with logging.span("generate_surrogate_key", **span_attrs):
            # Validate business keys
            missing_keys = [k for k in business_keys if k not in df.columns]
            if missing_keys:
                error_msg = f"Business key columns not found in DataFrame: {missing_keys}"
                logging.error(error_msg, available_columns=df.columns)
                raise SurrogateKeyError(error_msg)

            # Create window spec based on partition setting
            if company_partition and "`$Company`" in df.columns:
                window = Window.partitionBy(F.col("`$Company`")).orderBy(
                    *[F.col(k) for k in business_keys]
                )
                logging.debug("Using company-partitioned window", company_column="`$Company`")
            else:
                window = Window.orderBy(*[F.col(k) for k in business_keys])
                logging.debug("Using non-partitioned window")

            # Generate dense_rank-based surrogate key
            result_df = df.withColumn(
                key_name, (F.dense_rank().over(window) + F.lit(start_value - 1)).cast("int")
            )

            # Verify key was generated with expected properties
            if key_name not in result_df.columns:
                raise SurrogateKeyError(f"Failed to generate surrogate key column: {key_name}")

            # Calculate key statistics
            key_count = result_df.select(key_name).distinct().count()
            row_count = result_df.count()

            logging.info(
                "Surrogate key generation complete",
                key_name=key_name,
                unique_key_count=key_count,
                total_rows=row_count,
                density_pct=round((key_count / row_count) * 100, 2) if row_count > 0 else 0,
            )

            return result_df

    except Exception as e:
        if not isinstance(e, SurrogateKeyError):
            error_msg = f"Failed to generate surrogate key: {e!s}"
            logging.error(error_msg)
            raise SurrogateKeyError(error_msg) from e
        raise
