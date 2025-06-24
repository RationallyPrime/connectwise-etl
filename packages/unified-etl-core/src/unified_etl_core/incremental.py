"""Incremental processing support for the unified ETL framework.

Uses existing _etl_timestamp columns for change tracking - no separate watermark table needed!
"""

import logging
from datetime import datetime, timedelta
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class IncrementalProcessor:
    """Handles incremental processing logic across all ETL layers."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def get_last_etl_timestamp(self, table_name: str) -> datetime | None:
        """Get the maximum _etl_timestamp from a table to know when it was last updated."""
        try:
            if not self.spark.catalog.tableExists(table_name):
                return None
                
            result = self.spark.sql(f"""
                SELECT MAX(_etl_timestamp) as last_refresh 
                FROM {table_name}
            """).collect()
            
            return result[0][0] if result and result[0][0] else None
        except Exception as e:
            logger.warning(f"Could not get last timestamp from {table_name}: {e}")
            return None
    
    def get_changed_records(
        self,
        source_table: str,
        since_timestamp: datetime | None = None,
        target_table: str | None = None
    ) -> DataFrame:
        """Get records that changed since the last ETL run.
        
        If since_timestamp is not provided, uses the max _etl_timestamp from target table.
        """
        if since_timestamp is None and target_table:
            since_timestamp = self.get_last_etl_timestamp(target_table)
        
        if since_timestamp:
            logger.info(f"Getting records from {source_table} since {since_timestamp}")
            return self.spark.sql(f"""
                SELECT * FROM {source_table}
                WHERE _etl_timestamp > '{since_timestamp.isoformat()}'
            """)
        else:
            logger.info(f"No timestamp found, getting all records from {source_table}")
            return self.spark.table(source_table)
    
    def merge_bronze_incremental(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_key: str = "id",
        deduplicate: bool = True
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
        
        # Get columns for merge
        source_columns = source_df.columns
        update_cols = [col for col in source_columns if col != merge_key]
        update_expr = ", ".join([f"target.{col} = source.{col}" for col in update_cols])
        insert_cols = ", ".join(source_columns)
        insert_values = ", ".join([f"source.{col}" for col in source_columns])
        
        # Execute MERGE
        merge_sql = f"""
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
        self,
        source_df: DataFrame,
        target_table: str,
        business_keys: list[str]
    ) -> int:
        """Merge Silver data using SCD Type 1 (overwrite) strategy."""
        # Create temp view
        temp_view = f"temp_{target_table.split('.')[-1]}_scd1"
        source_df.createOrReplaceTempView(temp_view)
        
        # Build merge conditions
        merge_conditions = " AND ".join([
            f"target.{key} = source.{key}" for key in business_keys
        ])
        
        # Execute MERGE
        merge_sql = f"""
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
        self,
        source_df: DataFrame,
        target_table: str,
        merge_keys: list[str]
    ) -> int:
        """Merge fact table updates."""
        # Create temp view
        temp_view = f"temp_{target_table.split('.')[-1]}_fact"
        source_df.createOrReplaceTempView(temp_view)
        
        # Build merge conditions
        merge_conditions = " AND ".join([
            f"target.{key} = source.{key}" for key in merge_keys
        ])
        
        # Execute MERGE
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {merge_conditions}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        self.spark.sql(merge_sql)
        self.spark.catalog.dropTempView(temp_view)
        
        return source_df.count()


def build_incremental_conditions(
    entity_name: str,
    since_date: str,
    entity_config: dict[str, Any] | None = None
) -> str:
    """Build API conditions for incremental extraction based on entity type."""
    # Default patterns based on ConnectWise entity types
    if entity_name in ["TimeEntry", "ExpenseEntry"]:
        # Use dateEntered for work entries
        return f"dateEntered>=[{since_date}]"
    elif entity_name in ["Agreement", "Invoice", "PostedInvoice", "ProductItem"]:
        # Use lastUpdated for master data
        return f"lastUpdated>=[{since_date}]"
    elif entity_name == "UnpostedInvoice":
        # Unposted invoices are always in flux, fetch all
        return None
    else:
        # Check entity config for custom incremental column
        if entity_config and "incremental_column" in entity_config:
            column = entity_config["incremental_column"]
            return f"{column}>=[{since_date}]"
        else:
            # Default to lastUpdated
            return f"lastUpdated>=[{since_date}]"


def get_incremental_lookback_days(entity_name: str, default: int = 30) -> int:
    """Get the appropriate lookback period for an entity."""
    # Entity-specific lookback periods
    lookback_config = {
        "Agreement": 90,      # Agreements change less frequently
        "TimeEntry": 30,      # Recent time entries
        "ExpenseEntry": 30,   # Recent expenses
        "Invoice": 60,        # Invoices may be updated after creation
        "PostedInvoice": 90,  # Posted invoices rarely change
        "ProductItem": 180,   # Products are relatively stable
        "UnpostedInvoice": 7, # Work in progress, very recent
    }
    
    return lookback_config.get(entity_name, default)