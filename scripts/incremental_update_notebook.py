#!/usr/bin/env python3
"""
Incremental Update Pipeline for Fabric
Copy this into a Fabric notebook and run cell by cell
"""

# %%
# CELL 1: Install Dependencies
# Install the wheel files
# %pip install /lakehouse/default/Files/unified_etl_core-1.0.0-py3-none-any.whl
# %pip install /lakehouse/default/Files/unified_etl_connectwise-1.0.0-py3-none-any.whl

# %%
# CELL 2: Set Credentials
import os

# Set ConnectWise credentials
os.environ["CW_AUTH_USERNAME"] = "thekking+yemGyHDPdJ1hpuqx"
os.environ["CW_AUTH_PASSWORD"] = "yMqpe26Jcu55FbQk"
os.environ["CW_CLIENTID"] = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"

print("✅ Credentials configured")

# %%
# CELL 2B: ONE-TIME Schema Update - DELETE THIS CELL AFTER RUNNING!
# This refreshes gold layer to pick up new columns added to time entry fact
from unified_etl_core.main import run_etl_pipeline
import logging

print("🔧 ONE-TIME: Refreshing Gold layer to update schema with new columns...")
print("   (Delete this cell after running!)")

logging.basicConfig(level=logging.INFO)

# Full refresh of gold layer to pick up new time entry columns
run_etl_pipeline(
    integrations=["connectwise"],
    layers=["gold"],
    mode="full"
)

print("\n✅ Schema updated! You can now delete this cell.")

# %%
# CELL 3: Run Incremental Update
from unified_etl_core.main import run_etl_pipeline
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Run incremental pipeline with 7-day lookback
print("🔄 Starting Incremental ETL Pipeline...")

# Create proper entity configurations for Gold processing
entity_configs = {
    "agreement": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "AgreementSK", "business_keys": ["id"]}],
        "business_keys": [{"name": "AgreementBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {
            "estimated_monthly_revenue": "CASE WHEN applicationUnits = 'Amount' THEN COALESCE(applicationLimit, 0) ELSE 0 END"
        }
    },
    "invoice": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "InvoiceSK", "business_keys": ["id"]}],
        "business_keys": [{"name": "InvoiceBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {}
    },
    "timeentry": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "TimeentrySK", "business_keys": ["id"]}],
        "business_keys": [{"name": "TimeentryBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {}
    },
    "expenseentry": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "ExpenseentrySK", "business_keys": ["id"]}],
        "business_keys": [{"name": "ExpenseentryBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {}
    },
    "productitem": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "ProductitemSK", "business_keys": ["id"]}],
        "business_keys": [{"name": "ProductitemBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {}
    }
}

run_etl_pipeline(
    integrations=["connectwise"],
    layers=["bronze", "silver", "gold"],
    mode="incremental",
    lookback_days=7,
    config={"entities": entity_configs}
)

print("\n✅ Incremental update complete!")

# %%
# CELL 4: Custom Lookback for Specific Run
# Use this cell for a custom lookback period if needed
"""
# Example: Run with 14-day lookback
run_etl_pipeline(
    integrations=["connectwise"],
    layers=["bronze", "silver", "gold"],
    mode="incremental",
    lookback_days=14
)
"""

# %%
# CELL 5: Verify Recent Data
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.getActiveSession()

# Check most recent data in each layer
print("\n📊 Most Recent Data by Layer:")
print("="*50)

# Bronze layer
for entity in ["timeentry", "agreement", "invoice", "expenseentry"]:
    table = f"bronze.bronze_cw_{entity}"
    try:
        latest = spark.sql(f"""
            SELECT MAX(_etl_timestamp) as last_update
            FROM {table}
        """).collect()[0][0]
        print(f"Bronze {entity}: {latest}")
    except:
        pass

print()

# Silver layer  
for entity in ["timeentry", "agreement", "invoice", "expenseentry"]:
    table = f"silver.silver_cw_{entity}"
    try:
        latest = spark.sql(f"""
            SELECT MAX(_etl_processed_at) as last_update
            FROM {table}
        """).collect()[0][0]
        print(f"Silver {entity}: {latest}")
    except:
        pass

# %%
# CELL 6: Check Recent Time Entries
# Verify we have recent work data
if spark.catalog.tableExists("silver.silver_cw_timeentry"):
    recent = spark.sql("""
        SELECT 
            DATE(timeStart) as work_date,
            COUNT(*) as entries,
            SUM(actualHours) as hours,
            MAX(_etl_processed_at) as last_processed
        FROM silver.silver_cw_timeentry
        WHERE timeStart >= current_date() - INTERVAL 7 DAYS
        GROUP BY DATE(timeStart)
        ORDER BY work_date DESC
    """).collect()
    
    print("\n📅 Time Entries - Last 7 Days:")
    print("="*60)
    for row in recent:
        print(f"{row['work_date']}: {row['entries']:>4} entries, {row['hours']:>6.1f} hours (updated: {row['last_processed']})")

# %%
# CELL 7: Check Incremental vs Full Counts
# Compare record counts to ensure incremental is working
print("\n📈 Record Counts by Table:")
print("="*50)

for schema in ["bronze", "silver", "gold"]:
    print(f"\n{schema.upper()}:")
    try:
        tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
        for row in tables:
            if row.tableName.startswith(('bronze_cw_', 'silver_cw_', 'gold_')):
                count = spark.sql(f"SELECT COUNT(*) FROM {schema}.{row.tableName}").collect()[0][0]
                print(f"  {row.tableName}: {count:,}")
    except:
        pass

print(f"\n✅ Incremental update completed at {datetime.now()}")