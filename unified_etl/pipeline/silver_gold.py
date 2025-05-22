"""
Modern silver to gold pipeline using generic fact table creator.
Simplified approach that eliminates hardcoded entity-specific logic.
"""

from datetime import datetime as dt, timedelta

from pyspark.sql import SparkSession

from unified_etl.gold.facts import create_fact_table, process_gold_layer
from unified_etl.gold.hierarchy import build_account_hierarchy
from unified_etl.gold.keys import generate_surrogate_key
from unified_etl.pipeline.writer import write_with_schema_conflict_handling
from unified_etl.silver.standardize import get_silver_table_name
from unified_etl.utils import logging
from unified_etl.utils.config_loader import get_table_config
from unified_etl.utils.config_utils import extract_table_config, get_fiscal_settings
from unified_etl.utils.watermark_manager import manage_watermark


def process_silver_to_gold_table(
    spark: SparkSession,
    table_base_name: str,
    silver_db: str,
    gold_db: str,
    is_full_refresh: bool = False,
    last_processed_time: dt | None = None,
    fiscal_year_start: int = 1,
) -> bool:
    """
    Process a single table from silver to gold using the generic fact creator.
    
    Args:
        spark: Active SparkSession
        table_base_name: Base name of the table (e.g., 'GLEntry', 'Customer')
        silver_db: Silver database/schema name
        gold_db: Gold database/schema name
        is_full_refresh: Whether to perform full refresh
        last_processed_time: Timestamp of last processing
        fiscal_year_start: Starting month of fiscal year (1-12)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with logging.span("process_silver_to_gold_table", table_name=table_base_name):
            logging.info(f"Processing {table_base_name} from silver to gold")
            
            # Get configuration for the table
            table_config = get_table_config(table_base_name)
            if not table_config:
                logging.warning(f"No configuration found for {table_base_name}, using defaults")
                table_config = {}
            
            # Determine silver table name
            silver_table_name = get_silver_table_name(table_base_name)
            
            # Check if silver table exists
            try:
                silver_df = spark.table(f"{silver_db}.{silver_table_name}")
                logging.info(f"Found silver table {silver_db}.{silver_table_name} with {silver_df.count()} rows")
            except Exception as e:
                logging.warning(f"Silver table {silver_db}.{silver_table_name} not found: {str(e)}")
                return False
            
            # Apply incremental filtering if not full refresh
            if not is_full_refresh and last_processed_time:
                # Look for common timestamp columns
                timestamp_cols = [col for col in silver_df.columns 
                                if any(ts_name in col.lower() 
                                      for ts_name in ['timestamp', 'modified', 'updated', 'created', 'date'])]
                
                if timestamp_cols:
                    # Use the first timestamp column found
                    ts_col = timestamp_cols[0]
                    silver_df = silver_df.filter(silver_df[ts_col] > last_processed_time)
                    logging.info(f"Applied incremental filter on {ts_col} > {last_processed_time}")
            
            # Create fact table using generic creator
            gold_df = create_fact_table(
                spark=spark,
                silver_df=silver_df,
                entity_name=table_base_name,
                gold_path=gold_db
            )
            
            if gold_df is None:
                logging.error(f"Failed to create gold DataFrame for {table_base_name}")
                return False
            
            # Determine gold table name
            gold_table_name = table_config.get("gold_name", f"fact_{table_base_name}")
            
            # Write to gold layer
            write_success = write_with_schema_conflict_handling(
                df=gold_df,
                database=gold_db,
                table_name=gold_table_name,
                mode="append" if not is_full_refresh else "overwrite"
            )
            
            if write_success:
                logging.info(f"Successfully wrote {table_base_name} to gold layer as {gold_table_name}")
                
                # Update watermark if incremental
                if not is_full_refresh:
                    manage_watermark(
                        spark=spark,
                        table_name=table_base_name,
                        processed_time=dt.now(),
                        operation="silver_to_gold"
                    )
                
                return True
            else:
                logging.error(f"Failed to write {table_base_name} to gold layer")
                return False
                
    except Exception as e:
        logging.error(f"Error processing {table_base_name} from silver to gold: {str(e)}")
        return False


def run_silver_to_gold_pipeline(
    spark: SparkSession,
    silver_db: str,
    gold_db: str,
    tables: list[str] | None = None,
    is_full_refresh: bool = False,
    fiscal_year_start: int = 1,
) -> dict[str, bool]:
    """
    Run the complete silver to gold pipeline for multiple tables.
    
    Args:
        spark: Active SparkSession
        silver_db: Silver database/schema name
        gold_db: Gold database/schema name
        tables: List of table names to process (if None, processes all configured)
        is_full_refresh: Whether to perform full refresh
        fiscal_year_start: Starting month of fiscal year (1-12)
        
    Returns:
        Dictionary mapping table names to success status
    """
    logging.info("Starting silver to gold pipeline")
    
    # Default tables if none specified
    if tables is None:
        tables = [
            "GLEntry", "GLAccount", "Customer", "Vendor", "Item", "Job",
            "SalesInvoiceHeader", "SalesInvoiceLine", "CustLedgerEntry",
            "VendorLedgerEntry", "JobLedgerEntry", "Resource", "Dimension",
            "DimensionValue", "DimensionSetEntry", "CompanyInformation"
        ]
    
    results = {}
    
    for table_name in tables:
        logging.info(f"Processing table: {table_name}")
        
        # Get last processed time for incremental processing
        last_processed_time = None
        if not is_full_refresh:
            # You could implement watermark retrieval here
            # last_processed_time = get_last_watermark(table_name)
            pass
        
        success = process_silver_to_gold_table(
            spark=spark,
            table_base_name=table_name,
            silver_db=silver_db,
            gold_db=gold_db,
            is_full_refresh=is_full_refresh,
            last_processed_time=last_processed_time,
            fiscal_year_start=fiscal_year_start
        )
        
        results[table_name] = success
        
        if success:
            logging.info(f"✓ Successfully processed {table_name}")
        else:
            logging.error(f"✗ Failed to process {table_name}")
    
    # Build account hierarchy if GL tables were processed
    if any(table.startswith("GL") or table == "GLAccount" for table in tables):
        try:
            logging.info("Building account hierarchy")
            # Get GL accounts table for hierarchy building
            try:
                gl_accounts_df = spark.table(f"{gold_db}.dim_GLAccount")
                hierarchy_df = build_account_hierarchy(gl_accounts_df)
                # You might want to write this back or use it further
                logging.info("Account hierarchy built successfully")
            except Exception:
                logging.info("GL accounts table not found, skipping hierarchy build")
            logging.info("✓ Account hierarchy built successfully")
        except Exception as e:
            logging.error(f"✗ Failed to build account hierarchy: {str(e)}")
    
    # Summary
    successful_count = sum(results.values())
    total_count = len(results)
    logging.info(f"Pipeline completed: {successful_count}/{total_count} tables processed successfully")
    
    return results


def process_dimension_table(
    spark: SparkSession,
    table_base_name: str,
    silver_db: str,
    gold_db: str,
    is_full_refresh: bool = False,
) -> bool:
    """
    Process a dimension table from silver to gold.
    Dimensions typically don't need the full fact table treatment.
    
    Args:
        spark: Active SparkSession
        table_base_name: Base name of the dimension table
        silver_db: Silver database/schema name
        gold_db: Gold database/schema name
        is_full_refresh: Whether to perform full refresh
        
    Returns:
        True if successful, False otherwise
    """
    try:
        with logging.span("process_dimension_table", table_name=table_base_name):
            # Get silver table
            silver_table_name = get_silver_table_name(table_base_name)
            silver_df = spark.table(f"{silver_db}.{silver_table_name}")
            
            # For dimensions, we typically just add surrogate keys and audit columns
            dim_df = silver_df
            
            # Add surrogate key if not present
            key_column = f"{table_base_name}Key"
            if key_column not in dim_df.columns:
                dim_df = generate_surrogate_key(
                    df=dim_df,
                    key_name=key_column,
                    business_keys=[col for col in dim_df.columns if col.lower() in ['id', 'no', 'code', 'name']][:2]
                )
            
            # Add audit columns (implement add_audit_columns if needed)
            # dim_df = add_audit_columns(dim_df, layer="gold")
            
            # Write to gold
            gold_table_name = f"dim_{table_base_name}"
            
            write_success = write_with_schema_conflict_handling(
                df=dim_df,
                database=gold_db,
                table_name=gold_table_name,
                mode="overwrite" if is_full_refresh else "append"
            )
            
            if write_success:
                logging.info(f"Successfully processed dimension {table_base_name}")
                return True
            else:
                logging.error(f"Failed to write dimension {table_base_name}")
                return False
                
    except Exception as e:
        logging.error(f"Error processing dimension {table_base_name}: {str(e)}")
        return False


# Legacy interface compatibility
class SilverGoldPipeline:
    """Legacy interface for backwards compatibility."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def process_all_tables(
        self,
        silver_db: str,
        gold_db: str,
        tables: list[str] | None = None,
        is_full_refresh: bool = False,
    ) -> dict[str, bool]:
        """Process all tables using the new generic approach."""
        return run_silver_to_gold_pipeline(
            spark=self.spark,
            silver_db=silver_db,
            gold_db=gold_db,
            tables=tables,
            is_full_refresh=is_full_refresh
        )