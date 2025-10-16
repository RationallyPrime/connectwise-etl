"""Incremental processing utilities using Spark SQL MERGE."""

from __future__ import annotations

from datetime import datetime

import structlog
from pyspark.sql import DataFrame, Row, SparkSession

from etl_core.domain.protocols.types import TSCDType

logger = structlog.get_logger(__name__)


class IncrementalHandler:
    """
    Handles generic incremental processing logic using Spark SQL MERGE.
    This is a concrete utility class provided by the core framework.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_timestamp_col(self, table_name: str) -> str:
        """Determine the ETL timestamp column based on convention."""
        # Assuming convention: 'bronze' uses 'etl_timestamp', others use '_etl_processed_at'
        # This relies on the table name including the schema (e.g., lakehouse.bronze.table)
        parts = table_name.split(".")
        schema = parts[1] if len(parts) > 1 else "unknown"
        return "etl_timestamp" if schema == "bronze" else "_etl_processed_at"

    def get_last_etl_timestamp(self, table_name: str) -> datetime | None:
        """Get the maximum ETL timestamp from a target table."""
        try:
            if not self.spark.catalog.tableExists(table_name):
                return None

            timestamp_col = self._get_timestamp_col(table_name)

            result: list[Row] = self.spark.sql(f"""
                SELECT MAX({timestamp_col}) as last_refresh
                FROM {table_name}
            """).collect()

            return result[0]["last_refresh"] if result and result[0]["last_refresh"] else None
        except Exception as e:
            logger.warning("Could not get last timestamp", table=table_name, error=str(e))
            return None

    def get_changed_records(
        self,
        source_table: str,
        target_table: str,
    ) -> DataFrame:
        """Get records from the source that changed since the last load into the target."""
        since_timestamp = self.get_last_etl_timestamp(target_table)

        if since_timestamp:
            logger.info(
                "Filtering changed records", source=source_table, since=since_timestamp.isoformat()
            )
            timestamp_col = self._get_timestamp_col(source_table)

            return self.spark.sql(f"""
                SELECT * FROM {source_table}
                WHERE {timestamp_col} > '{since_timestamp.isoformat()}'
            """)
        else:
            logger.info(
                "No previous timestamp found, performing full load from source", source=source_table
            )
            return self.spark.table(source_table)

    def merge(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_keys: list[str],
        scd_type: TSCDType = "scd1",
    ) -> int:
        """
        Merge data using the specified SCD strategy via MERGE INTO.
        """
        if scd_type == "scd1":
            return self._merge_scd1(source_df, target_table, merge_keys)
        elif scd_type == "scd2":
            # SCD2 implementation omitted for brevity, but would go here
            raise NotImplementedError("SCD Type 2 merge is not yet implemented.")
        else:
            raise ValueError(f"Unsupported SCD type: {scd_type}")

    def _merge_scd1(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_keys: list[str],
        deduplicate_source: bool = True,
    ) -> int:
        """Helper for SCD Type 1 (overwrite)."""
        if not merge_keys:
            raise ValueError("merge_keys must be provided.")

        # Deduplicate source if requested
        if deduplicate_source:
            source_count = source_df.count()
            source_df = source_df.dropDuplicates(merge_keys)
            deduped_count = source_df.count()
            if deduped_count < source_count:
                logger.info("Deduplicated source records", removed=source_count - deduped_count)

        # Create temp view for merge
        temp_view = f"temp_merge_source_{abs(hash(target_table))}"
        source_df.createOrReplaceTempView(temp_view)

        # Build merge conditions
        merge_conditions = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])

        # Execute MERGE
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {merge_conditions}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        logger.debug("Executing MERGE SQL", sql=merge_sql)
        self.spark.sql(merge_sql)

        # Clean up
        self.spark.catalog.dropTempView(temp_view)

        return source_df.count()
