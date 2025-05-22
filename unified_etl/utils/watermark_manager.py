# unified_etl/utils/watermark_manager.py
from datetime import datetime, timedelta
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from unified_etl.utils import logging

WATERMARK_TABLE_PATH = "LH.metadata.etl_watermarks"  # Adjust schema/table name if needed


def _ensure_watermark_table(spark: SparkSession) -> None:
    """Creates the watermark table if it doesn't exist."""
    schema = StructType(
        [
            StructField("source_table", StringType(), False),
            StructField(
                "last_processed_value_str", StringType(), True
            ),  # Store as string for flexibility (ts, int)
            StructField("updated_at", TimestampType(), False),
        ]
    )
    if not spark.catalog.tableExists(WATERMARK_TABLE_PATH):
        try:
            spark.createDataFrame([], schema).write.format("delta").saveAsTable(
                WATERMARK_TABLE_PATH
            )
            logging.info(f"Created watermark table: {WATERMARK_TABLE_PATH}")
        except Exception as e:
            logging.error(f"Failed to create watermark table {WATERMARK_TABLE_PATH}: {e}")
            raise


def get_watermark(spark: SparkSession, source_table: str) -> str | None:
    """
    Retrieves the last processed watermark value for a given source table.

    Args:
        spark: SparkSession.
        source_table: The name of the source table (e.g., 'GLEntry-17').

    Returns:
        The last processed watermark value as a string, or None if not found.
    """
    _ensure_watermark_table(spark)
    try:
        watermark_df = spark.table(WATERMARK_TABLE_PATH)
        result = (
            watermark_df.filter(F.col("source_table") == source_table)
            .select("last_processed_value_str")
            .first()
        )
        if result:
            logging.info(
                f"Retrieved watermark for {source_table}: {result['last_processed_value_str']}"
            )
            return str(result["last_processed_value_str"])
        else:
            logging.info(
                f"No watermark found for {source_table}, will perform full load or use default lookback."
            )
            return None
    except Exception as e:
        logging.error(f"Failed to get watermark for {source_table}: {e}")
        # In case of error, returning None might trigger a full load, which is safer
        return None


def update_watermark(spark: SparkSession, source_table: str, new_watermark_value: Any) -> None:
    """
    Updates the watermark value for a given source table using Delta MERGE.

    Args:
        spark: SparkSession.
        source_table: The name of the source table (e.g., 'GLEntry-17').
        new_watermark_value: The new watermark value (will be cast to string).
    """
    if new_watermark_value is None:
        logging.warning(
            f"Attempted to update watermark for {source_table} with None value. Skipping."
        )
        return

    _ensure_watermark_table(spark)
    try:
        from delta.tables import DeltaTable

        watermark_delta_table = DeltaTable.forPath(
            spark, WATERMARK_TABLE_PATH.replace(".", "/")
        )  # Path for DeltaTable API

        new_watermark_str = str(new_watermark_value)

        # Create a DataFrame with the new watermark data
        update_df = spark.createDataFrame(
            [(source_table, new_watermark_str, datetime.now())],
            ["source_table", "last_processed_value_str", "updated_at"],
        )

        # Perform MERGE operation (UPSERT)
        watermark_delta_table.alias("target").merge(
            update_df.alias("source"), "target.source_table = source.source_table"
        ).whenMatchedUpdate(
            set={
                "last_processed_value_str": F.col("source.last_processed_value_str"),
                "updated_at": F.col("source.updated_at"),
            }
        ).whenNotMatchedInsert(
            values={
                "source_table": F.col("source.source_table"),
                "last_processed_value_str": F.col("source.last_processed_value_str"),
                "updated_at": F.col("source.updated_at"),
            }
        ).execute()

        logging.info(f"Successfully updated watermark for {source_table} to {new_watermark_str}")

    except Exception as e:
        logging.error(f"Failed to update watermark for {source_table}: {e}")
        # Decide if this should raise an exception to halt the pipeline
        # raise


def get_last_watermark_datetime(
    spark: SparkSession,
    table_name: str,
    default_days: int | None = None,
) -> datetime | None:
    """
    Get the last watermark as a datetime object, with option for default lookback.

    Args:
        spark: SparkSession
        table_name: Table identifier
        default_days: Days to look back if no watermark exists

    Returns:
        Datetime of last processing or None
    """
    watermark_str = get_watermark(spark, table_name)

    if watermark_str:
        try:
            # Try parsing as ISO format timestamp first
            return datetime.fromisoformat(watermark_str.replace("Z", "+00:00"))
        except ValueError:
            try:
                # Try parsing as date string next
                return datetime.strptime(watermark_str, "%Y-%m-%d")
            except ValueError:
                logging.warning(f"Could not parse watermark '{watermark_str}' as datetime")

    # If no watermark or parsing failed, use default lookback if provided
    if default_days is not None:
        default_dt = datetime.now() - timedelta(days=default_days)
        logging.info(
            f"No valid watermark found for {table_name}, using default lookback of {default_days} days",
            default_datetime=default_dt.isoformat(),
        )
        return default_dt

    return None


def update_current_watermark(
    spark: SparkSession,
    table_name: str,
) -> None:
    """
    Update the watermark for a table to the current timestamp.

    Args:
        spark: SparkSession
        table_name: Table identifier
    """
    current_time = datetime.now()
    update_watermark(spark, table_name, current_time)
    logging.info(f"Updated watermark for {table_name} to current time: {current_time.isoformat()}")


def calculate_new_watermark(
    spark: SparkSession,
    table_name: str,
    df_timestamp_col: str,
    df: Any | None = None,
) -> datetime | None:
    """
    Calculate a new watermark value based on a DataFrame's max timestamp.

    Args:
        spark: SparkSession
        table_name: Table identifier
        df_timestamp_col: Timestamp column to use for watermark
        df: Optional DataFrame to use (if provided)

    Returns:
        New watermark value or None if not determinable
    """
    if df is None:
        # No DataFrame provided
        return None

    import pyspark.sql.functions as F  # noqa: N812

    # Check if timestamp column exists
    if df_timestamp_col not in df.columns:
        logging.warning(
            f"Timestamp column '{df_timestamp_col}' not found in DataFrame for watermark calculation",
            available_columns=df.columns,
        )
        return None

    # Get max timestamp
    max_ts_row = df.agg(F.max(F.col(df_timestamp_col))).collect()[0]
    new_watermark = max_ts_row[0]

    if new_watermark:
        logging.info(
            f"Calculated new watermark for {table_name}",
            max_timestamp=new_watermark,
            timestamp_column=df_timestamp_col,
        )
        return new_watermark

    return None


def manage_watermark(
    spark: SparkSession,
    table_name: str,
    is_incremental: bool,
    last_processed_time: datetime | None,
    df_timestamp_col: str | None = None,
    df: Any | None = None,
    default_lookback_days: int | None = None,
) -> datetime:
    """
    Comprehensive watermark management function that:
    1. Gets the current watermark or default lookback
    2. Calculates a new watermark if a DataFrame is provided
    3. Updates the watermark if processing was incremental

    Args:
        spark: SparkSession
        table_name: Table identifier
        is_incremental: Whether processing was incremental
        last_processed_time: Last processed timestamp provided by caller
        df_timestamp_col: Timestamp column for watermark calculation
        df: Optional DataFrame for new watermark calculation
        default_lookback_days: Default lookback days if no watermark exists

    Returns:
        Timestamp used for processing (current or new)
    """
    # Get current watermark or use provided value
    current_timestamp = last_processed_time
    if current_timestamp is None:
        current_timestamp = get_last_watermark_datetime(
            spark, table_name, default_days=default_lookback_days
        )

    # Calculate new watermark if DataFrame and timestamp column provided
    new_watermark = None
    if is_incremental and df is not None and df_timestamp_col:
        new_watermark = calculate_new_watermark(spark, table_name, df_timestamp_col, df)

    # Update watermark if processing was incremental and we have a new value
    if is_incremental:
        if new_watermark:
            update_watermark(spark, table_name, new_watermark)
            logging.info(
                f"Updated watermark for {table_name}",
                new_watermark=new_watermark,
                previous_watermark=current_timestamp,
            )
        else:
            # If no calculable watermark, use current timestamp
            update_current_watermark(spark, table_name)

    return current_timestamp or datetime.now()
