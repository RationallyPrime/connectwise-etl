"""
Incremental data refresh for ConnectWise ETL - Fabric Notebook Version

This code refreshes only recent data instead of re-fetching 5 years worth of records.
Designed to run as a single cell in a Microsoft Fabric notebook.

Prerequisites:
- Run pip install commands in a separate cell first
- Ensure Key Vault secrets are configured (CW_AUTH_USERNAME, CW_AUTH_PASSWORD, CW_CLIENTID)
"""

# Standard imports
from datetime import datetime, timedelta
import logging
from pyspark.sql import DataFrame

# ETL framework imports
from unified_etl_connectwise import ConnectWiseClient
from unified_etl_connectwise.api_utils import build_condition_string

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def refresh_recent_data(days_back: int = 30) -> dict[str, DataFrame]:
    """
    Refresh only recent data from ConnectWise.
    
    Strategy:
    - TimeEntry/ExpenseEntry: Only recent entries (by dateEntered)
    - Agreement/PostedInvoice: Only recently updated (by lastUpdated)
    - UnpostedInvoice: ALL records (they're work in progress)
    
    Args:
        days_back: Number of days to look back (default 30)
        
    Returns:
        Dictionary mapping entity names to DataFrames
    """
    # Initialize client
    client = ConnectWiseClient()
    
    # Calculate date threshold
    since_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    print(f"Refreshing data since: {since_date}")
    
    results = {}
    
    # Use the existing endpoint mapping from client._get_entity_name
    # Note: ConnectWise API endpoints:
    # - /finance/invoices returns UnpostedInvoice entities
    # - /finance/invoices/posted returns PostedInvoice entities
    endpoints = {
        "TimeEntry": "/time/entries",
        "Agreement": "/finance/agreements", 
        "UnpostedInvoice": "/finance/invoices",  # Returns UnpostedInvoice entities
        "PostedInvoice": "/finance/invoices/posted",  # Posted invoices
        "ExpenseEntry": "/expense/entries",
    }
    
    for entity_name, endpoint in endpoints.items():
        print(f"\nRefreshing {entity_name}...")
        
        # Build appropriate conditions based on entity type
        if entity_name in ["TimeEntry", "ExpenseEntry"]:
            conditions = build_condition_string(date_entered_gte=since_date)
            order_by = "dateEntered desc"
        else:  # Agreement, PostedInvoice, UnpostedInvoice
            conditions = f"(lastUpdated>=[{since_date}])"  
            order_by = "lastUpdated desc"
        
        try:
            df = client.extract(
                endpoint=endpoint,
                conditions=conditions,
                order_by=order_by,
                page_size=1000
            )
            
            # Add ETL metadata columns to match existing Bronze schema
            from pyspark.sql import functions as F
            df = df.withColumn("etl_timestamp", F.current_timestamp().cast("string"))
            df = df.withColumn("etl_entity", F.lit(entity_name))
            df = df.withColumn("etlTimestamp", F.current_timestamp().cast("string"))
            df = df.withColumn("etlEntity", F.lit(entity_name))
            
            results[entity_name] = df
            print(f"  Found {df.count()} {entity_name} records")
        except Exception as e:
            print(f"  ERROR extracting {entity_name}: {e}")
            results[entity_name] = None
    
    return results


def refresh_specific_date_range(start_date: str, end_date: str) -> dict[str, int]:
    """
    Refresh data for a specific date range.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        Dictionary mapping entity names to record counts
    """
    client = ConnectWiseClient()
    print(f"Refreshing data from {start_date} to {end_date}")
    
    results = {}
    
    # Time entries in date range
    conditions = f"dateEntered>=[{start_date}] AND dateEntered<=[{end_date}]"
    time_entries = client.extract(
        endpoint="/time/entries",
        conditions=conditions,
        order_by="dateEntered desc",
        page_size=1000
    )
    results["time_entries"] = time_entries.count()
    
    return results


def check_latest_records() -> None:
    """
    Check the most recent records to see what dates we have.
    """
    client = ConnectWiseClient()
    
    # Get just the 10 most recent time entries
    recent_time = client.paginate(
        endpoint="/time/entries",
        entity_name="time_entries",
        fields="id,dateEntered,notes",
        order_by="dateEntered desc",
        max_pages=1,
        page_size=10
    )
    
    print("\nMost recent time entries:")
    for entry in recent_time[:5]:
        print(f"  {entry['dateEntered']}: {entry.get('notes', '')[:50]}...")
    
    # Get most recent invoices
    recent_invoices = client.paginate(
        endpoint="/finance/invoices",
        entity_name="invoices",
        fields="id,invoiceNumber,dateCreated,lastUpdated",
        order_by="lastUpdated desc",
        max_pages=1,
        page_size=10
    )
    
    print("\nMost recent invoices:")
    for inv in recent_invoices[:5]:
        print(f"  Invoice keys: {list(inv.keys())[:5]}...")  # Debug line to see actual field names
        print(f"  {inv.get('invoiceNumber', inv.get('id', 'N/A'))}: Created {inv.get('dateCreated', inv.get('date', 'N/A'))}, Updated {inv.get('lastUpdated', 'N/A')}")


# ==============================================================================
# FABRIC NOTEBOOK EXECUTION
# ==============================================================================

# BEFORE RUNNING THIS CELL, YOU NEED TO KNOW YOUR TABLE PATHS!
# 
# BRONZE → SILVER → GOLD TABLE MAPPING:
#
# Bronze Tables (from API):
#   Agreement        → Silver: Agreement        → Gold: fact_agreement_period
#   TimeEntry        → Silver: TimeEntry        → Gold: fact_time_entry
#   ExpenseEntry     → Silver: ExpenseEntry     → Gold: fact_expense_entry  
#   ProductItem      → Silver: ProductItem      → (used in fact_invoice_line)
#   PostedInvoice    → Silver: PostedInvoice    → Gold: fact_invoice_line (requires API permission)
#   UnpostedInvoice  → Silver: UnpostedInvoice  → (may also feed fact_invoice_line)
#
# Gold combines multiple Silver tables:
#   fact_time_entry: TimeEntry + Agreement + Member
#   fact_invoice_line: PostedInvoice/UnpostedInvoice + TimeEntry + ProductItem + Agreement
#
# Based on your SHOW TABLES output, your tables are case-sensitive!

# Configuration - UPDATE THESE BASED ON YOUR ENVIRONMENT
LAKEHOUSE_ROOT = "/lakehouse/default/Tables/"  # Update if different
DAYS_TO_REFRESH = 30  # How many days back to refresh

# Option 1: Check what's the latest data we have
print("=== Checking Latest Records ===")
check_latest_records()

# Option 2: Refresh recent data (recommended for incremental updates)
print(f"\n=== Refreshing Last {DAYS_TO_REFRESH} Days ===")
results = refresh_recent_data(DAYS_TO_REFRESH)
print("\nRefresh Summary:")
for entity, df in results.items():
    if df is not None:
        print(f"  {entity}: {df.count()} records")
    else:
        print(f"  {entity}: ERROR")

# Option 3: Refresh specific date range (uncomment to use)
# start_date = "2024-11-01"
# end_date = "2024-12-31"
# results = refresh_specific_date_range(start_date, end_date)

# Option 4: Write refreshed data to Bronze tables
# The client.extract() already returns validated Spark DataFrames with proper schema

# Initialize client to get spark session
client = ConnectWiseClient()
spark = client.spark

# Debug: Check what tables exist in bronze schema
print("\n=== Checking Bronze Tables ===")
try:
    bronze_tables = spark.sql("SHOW TABLES IN bronze")
    bronze_tables.show(100, truncate=False)
except Exception as e:
    print(f"Error listing bronze tables: {e}")

# Write the refreshed data to Bronze tables
print("\n=== Writing to Bronze Tables ===")
for entity_name, df in results.items():
    if df is not None and df.count() > 0:
        try:
            bronze_table = f"bronze.{entity_name}"
            
            # The DataFrame from client.extract() is already validated
            # Use append mode with mergeSchema to handle evolution
            df.write.mode("append").option("mergeSchema", "true").saveAsTable(bronze_table)
            
            print(f"  Appended {df.count()} records to {bronze_table}")
            
        except Exception as e:
            print(f"  ERROR writing {entity_name}: {e}")
            # Try to understand the error better
            if "doesn't exist" in str(e) or "Table or view not found" in str(e):
                print(f"  Table {bronze_table} doesn't exist. Creating it...")
                try:
                    df.write.mode("overwrite").saveAsTable(bronze_table)
                    print(f"  Created {bronze_table} with {df.count()} records")
                except Exception as e2:
                    print(f"  ERROR creating table: {e2}")

print("\n=== Incremental Refresh Complete ===")
print("Next steps:")
print("1. Verify the data looks correct")
print("2. Merge (don't overwrite!) the fresh data to Bronze tables")
print("3. Run Silver transformations for the refreshed entities")
print("4. Run Gold transformations (fact tables) that depend on updated Silver tables")

# CASCADE UPDATE EXAMPLE:
# After merging to Bronze, you need to update Silver and Gold layers
#
# # 1. Update Silver layer for refreshed entities
# from unified_etl_core.silver import process_silver_layer
# from unified_etl_connectwise.config import ENTITY_CONFIGS
#
# # Process only the entities we refreshed
# refreshed_entities = [name for name, df in results.items() if df is not None]
# for entity in refreshed_entities:
#     if entity in ENTITY_CONFIGS:
#         config = ENTITY_CONFIGS[entity]
#         process_silver_layer(
#             bronze_table=config["bronze_table"],
#             silver_table=config["silver_table"],
#             # ... other config params
#         )
#
# # 2. Update Gold layer (fact tables that depend on Silver)
# from unified_etl_connectwise.transforms import (
#     create_time_entry_fact,
#     create_invoice_line_fact,
#     create_expense_entry_fact,
#     create_agreement_period_fact
# )
#
# # Update fact tables based on what was refreshed
# if "time_entry" in refreshed_entities or "agreement" in refreshed_entities:
#     # fact_time_entry depends on silver_timeentry + silver_agreement
#     create_time_entry_fact(spark, "silver_", "gold_")
#
# if "invoice" in refreshed_entities or "time_entry" in refreshed_entities:
#     # fact_invoice_line depends on multiple silver tables
#     create_invoice_line_fact(spark, "silver_", "gold_")