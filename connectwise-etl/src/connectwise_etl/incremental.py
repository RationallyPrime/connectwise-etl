"""Incremental processing support for the unified ETL framework.

Handles two separate concerns:
1. API incremental extraction - using source system timestamps (dateEntered, lastUpdated)
2. Layer-to-layer propagation - using ETL processing timestamps (etlTimestamp, _etl_processed_at)
"""

import logging
from datetime import datetime
from typing import Any, Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row

from .utils.decorators import with_etl_error_handling

logger = logging.getLogger(__name__)


class IncrementalProcessor:
    """Handles incremental processing logic across all ETL layers."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @with_etl_error_handling(operation="get_last_source_timestamp")
    def get_last_source_timestamp(
        self, table_name: str, source_date_column: str = "dateEntered"
    ) -> datetime | None:
        """Get the maximum source system timestamp from Bronze table.

        This tells us the most recent record we have from the source system.
        Used for API incremental extraction.

        Args:
            table_name: Bronze table name
            source_date_column: Column tracking source system dates (e.g. dateEntered, lastUpdated)
        """
        try:
            if not self.spark.catalog.tableExists(table_name):
                return None

            # Check if column exists
            columns = [col.name for col in self.spark.table(table_name).schema]
            if source_date_column not in columns:
                logger.warning(f"Column {source_date_column} not found in {table_name}")
                return None

            result: list[Row] = self.spark.sql(f"""
                SELECT MAX({source_date_column}) as last_source_date
                FROM {table_name}
                WHERE {source_date_column} IS NOT NULL
            """).collect()

            return result[0][0] if result and result[0][0] else None
        except Exception as e:
            logger.warning(f"Could not get last {source_date_column} from {table_name}: {e}")
            return None

    @with_etl_error_handling(operation="get_last_etl_timestamp")
    def get_last_etl_timestamp(self, table_name: str) -> datetime | None:
        """Get the maximum ETL processing timestamp from a table.

        This tells us when we last processed data into this table.
        Used for layer-to-layer propagation (Bronze->Silver->Gold).
        """
        try:
            if not self.spark.catalog.tableExists(table_name):
                return None

            # Bronze uses etlTimestamp, Silver/Gold use _etl_processed_at
            timestamp_col: Literal["etlTimestamp", "_etl_processed_at"] = (
                "etlTimestamp" if "bronze" in table_name else "_etl_processed_at"
            )

            result: list[Row] = self.spark.sql(f"""
                SELECT MAX({timestamp_col}) as last_refresh
                FROM {table_name}
            """).collect()

            return result[0][0] if result and result[0][0] else None
        except Exception as e:
            logger.warning(f"Could not get last ETL timestamp from {table_name}: {e}")
            return None

    def get_new_records_for_layer(
        self,
        source_table: str,
        target_table: str,
    ) -> DataFrame:
        """Get records from source that are newer than what's in target.

        For layer-to-layer propagation (Bronze->Silver->Gold).
        Compares ETL timestamps to find what needs to be processed.
        """
        # Get the last time we processed data into the target
        last_target_etl = self.get_last_etl_timestamp(target_table)

        if last_target_etl:
            logger.info(f"Getting records from {source_table} processed after {last_target_etl}")

            # Check what timestamp columns exist in source table
            source_df = self.spark.table(source_table)
            available_columns = source_df.columns

            # Try to find the right timestamp column
            timestamp_col = None
            if "etlTimestamp" in available_columns:
                timestamp_col = "etlTimestamp"
            elif "_etl_processed_at" in available_columns:
                timestamp_col = "_etl_processed_at"
            elif "_etl_timestamp" in available_columns:
                timestamp_col = "_etl_timestamp"
            else:
                # No ETL timestamp column found - can't do incremental
                logger.warning(
                    f"No ETL timestamp column found in {source_table}. Available columns: {available_columns[:10]}..."
                )
                logger.info(f"Falling back to full table scan for {source_table}")
                return source_df

            return self.spark.sql(f"""
                SELECT * FROM {source_table}
                WHERE {timestamp_col} > '{last_target_etl.isoformat()}'
            """)
        else:
            logger.info(
                f"No timestamp found in {target_table}, getting all records from {source_table}"
            )
            return self.spark.table(source_table)

    def get_changed_records(
        self,
        source_table: str,
        since_timestamp: datetime | None = None,
        target_table: str | None = None,
    ) -> DataFrame:
        """Legacy method for backward compatibility. Use get_new_records_for_layer instead.

        Get records that changed since the last ETL run.
        If since_timestamp is not provided, uses the max ETL timestamp from target table.
        """
        # If both source and target tables provided, use the new method
        if target_table and not since_timestamp:
            return self.get_new_records_for_layer(source_table, target_table)

        # Otherwise handle the legacy case
        if since_timestamp is None and target_table:
            since_timestamp = self.get_last_etl_timestamp(target_table)

        if since_timestamp:
            logger.info(f"Getting records from {source_table} since {since_timestamp}")

            # Check what timestamp columns exist in source table
            source_df = self.spark.table(source_table)
            available_columns = source_df.columns

            # Try to find the right timestamp column
            timestamp_col = None
            if "etlTimestamp" in available_columns:
                timestamp_col = "etlTimestamp"
            elif "_etl_processed_at" in available_columns:
                timestamp_col = "_etl_processed_at"
            elif "_etl_timestamp" in available_columns:
                timestamp_col = "_etl_timestamp"
            else:
                # No ETL timestamp column found - can't do incremental
                logger.warning(f"No ETL timestamp column found in {source_table}")
                logger.info(f"Falling back to full table scan")
                return source_df

            return self.spark.sql(f"""
                SELECT * FROM {source_table}
                WHERE {timestamp_col} > '{since_timestamp.isoformat()}'
            """)
        else:
            logger.info(f"No timestamp found, getting all records from {source_table}")
            return self.spark.table(source_table)

    def merge_bronze_incremental(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_key: str = "id",
        deduplicate: bool = True,
    ) -> tuple[int, int]:
        """Merge incremental data into Bronze table using MERGE.

        Returns:
            Tuple of (records_merged, total_records)
        """
        if deduplicate:
            # Remove duplicates from source
            source_deduped = source_df.dropDuplicates([merge_key])
            if source_deduped.count() < source_df.count():
                logger.info(f"Removed {source_df.count() - source_deduped.count()} duplicates")
            source_df = source_deduped

        # Create temp view for merge
        temp_view = f"temp_{target_table.split('.')[-1]}_merge"
        source_df.createOrReplaceTempView(temp_view)

        # Get columns that exist in both source and target
        target_columns: list[str] = self.spark.table(target_table).columns
        source_columns: list[str] = source_df.columns

        # Only update columns that exist in the target table
        common_columns: list[str] = [col for col in source_columns if col in target_columns]
        update_cols: list[str] = [col for col in common_columns if col != merge_key]
        update_expr: str = ", ".join([f"target.{col} = source.{col}" for col in update_cols])

        # For insert, only use columns that exist in target
        insert_cols: str = ", ".join(common_columns)
        insert_values: str = ", ".join([f"source.{col}" for col in common_columns])

        # Execute MERGE
        merge_sql: str = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED THEN
            UPDATE SET {update_expr}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_values})
        """

        self.spark.sql(merge_sql)

        # Clean up
        self.spark.catalog.dropTempView(temp_view)

        # Get final count
        final_count = self.spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]

        return source_df.count(), final_count

    def merge_silver_scd1(
        self, source_df: DataFrame, target_table: str, business_keys: list[str]
    ) -> int:
        """Merge Silver data using SCD Type 1 (overwrite) strategy."""
        # Create temp view
        temp_view = f"temp_{target_table.split('.')[-1]}_scd1"
        source_df.createOrReplaceTempView(temp_view)

        # Build merge conditions
        merge_conditions: str = " AND ".join(
            [f"target.{key} = source.{key}" for key in business_keys]
        )

        # Execute MERGE
        merge_sql: str = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {merge_conditions}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView(temp_view)

        return source_df.count()

    def merge_gold_fact(
        self, source_df: DataFrame, target_table: str, merge_keys: list[str]
    ) -> int:
        """Merge fact table updates."""
        # Create temp view
        temp_view = f"temp_{target_table.split('.')[-1]}_fact"
        source_df.createOrReplaceTempView(temp_view)

        # Build merge conditions
        merge_conditions: str = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

        # Execute MERGE
        merge_sql: str = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {merge_conditions}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        self.spark.sql(sqlQuery=merge_sql)
        self.spark.catalog.dropTempView(viewName=temp_view)

        return source_df.count()


def build_incremental_conditions(
    entity_name: str, since_date: str, entity_config: dict[str, Any] | None = None
) -> str | None:
    """Build API conditions for incremental extraction based on entity type.

    Args:
        entity_name: Name of the entity being extracted
        since_date: Date string in YYYY-MM-DD format
        entity_config: Optional entity-specific configuration

    Returns:
        ConnectWise API condition string or None for full extraction
    """
    # Special cases that always need full extraction
    if entity_name == "UnpostedInvoice":
        # Unposted invoices are always in flux, fetch all
        return None

    # Check entity config for custom incremental column
    if entity_config and "incremental_column" in entity_config:
        column = entity_config["incremental_column"]
        # Strip any nested path notation for API conditions
        if "." in column:
            column = column.split(".")[-1]
        return f"{column}>=[{since_date}]"

    # Build entity-specific conditions based on actual available fields
    # Note: These conditions only catch NEW or recently modified records based on available fields

    if entity_name == "Agreement":
        # Get agreements that started or ended recently
        return f"(startDate>=[{since_date}] or endDate>=[{since_date}])"

    elif entity_name == "Company":
        # Companies acquired since the date
        return f"dateAcquired>=[{since_date}]"

    elif entity_name == "TimeEntry":
        # Only new time entries
        return f"dateEntered>=[{since_date}]"

    elif entity_name == "ExpenseEntry":
        # Expense entries by date
        return f"date>=[{since_date}]"

    elif entity_name == "Invoice":
        # Invoices by date
        return f"date>=[{since_date}]"

    elif entity_name == "ProductItem":
        # Get products purchased or cancelled since date
        # Using OR condition to catch both purchased and cancelled items
        return f"(purchaseDate>=[{since_date}] or cancelledDate>=[{since_date}])"

    elif entity_name in ["Contact", "Opportunity", "Project", "Ticket"]:
        # These might have lastUpdated (need to verify)
        return f"lastUpdated>=[{since_date}]"

    else:
        # Default fallback - try dateEntered as it's most common
        logger.warning(f"Unknown entity {entity_name}, using dateEntered for incremental")
        return f"dateEntered>=[{since_date}]"


def get_incremental_lookback_days(entity_name: str, default: int = 30) -> int:
    """Get the appropriate lookback period for an entity."""
    # Entity-specific lookback periods
    lookback_config: dict[str, int] = {
        "Agreement": 90,  # Agreements change less frequently
        "TimeEntry": 30,  # Recent time entries
        "ExpenseEntry": 30,  # Recent expenses
        "Invoice": 60,  # Invoices may be updated after creation
        "PostedInvoice": 90,  # Posted invoices rarely change
        "ProductItem": 180,  # Products are relatively stable
        "UnpostedInvoice": 7,  # Work in progress, very recent
    }

    return lookback_config.get(entity_name, default)
