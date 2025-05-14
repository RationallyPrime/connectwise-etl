#!/usr/bin/env python
# coding: utf-8
"""
Full Medallion Architecture Integration Test
Tests bronze, silver, and gold layers with both overwrite and append modes
"""

# ---- CELL 1: Install dependencies ----
# %pip install /lakehouse/default/Files/fabric_api-0.3.0-py3-none-any.whl
# %pip install sparkdantic

# ---- CELL 2: Environment setup ----
import os
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Set credentials from Key Vault (in production) or environment
os.environ["CW_AUTH_USERNAME"] = "thekking+yemGyHDPdJ1hpuqx"
os.environ["CW_AUTH_PASSWORD"] = "yMqpe26Jcu55FbQk"  
os.environ["CW_CLIENTID"] = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"

# ---- CELL 3: Initialize Spark and client ----
from pyspark.sql import SparkSession
from fabric_api.client import ConnectWiseClient

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
client = ConnectWiseClient()

# Test connectivity
response = client.get("/system/info")
print(f"API connected: {response.status_code == 200}")

# ---- CELL 4: Define test parameters ----
# Small test dataset
test_entities = ["Agreement", "TimeEntry", "ExpenseEntry", "ProductItem", "PostedInvoice", "UnpostedInvoice"]
page_size = 100
max_pages = 10  # 1000 records max per entity

# Date range for testing
start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
end_date = datetime.now().strftime("%Y-%m-%d")
conditions = f"lastUpdated>=[{start_date}] AND lastUpdated<[{end_date}]"

# Paths - Fabric doesn't like nested paths under Tables
bronze_path = "Tables/bronze"
silver_path = "Tables/silver"
gold_path = "Tables/gold"

# ---- CELL 5: Bronze layer - Overwrite mode ----
from fabric_api.pipeline import process_entity_to_bronze

print("=== BRONZE LAYER - OVERWRITE MODE ===")
bronze_results_overwrite = {}

for entity in test_entities:
    logger.info(f"Processing {entity} to bronze (overwrite)...")
    result = process_entity_to_bronze(
        entity_name=entity,
        client=client,
        conditions=conditions,
        page_size=page_size,
        max_pages=max_pages,
        bronze_path=bronze_path,
        mode="overwrite"
    )
    bronze_results_overwrite[entity] = result
    print(f"{entity}: {result['bronze_rows']} rows")

# ---- CELL 6: Bronze layer - Append mode ----
print("\n=== BRONZE LAYER - APPEND MODE ===")
bronze_results_append = {}

# Use a different date range for append test
append_start = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
append_conditions = f"lastUpdated>=[{append_start}] AND lastUpdated<[{end_date}]"

for entity in test_entities:
    logger.info(f"Processing {entity} to bronze (append)...")
    result = process_entity_to_bronze(
        entity_name=entity,
        client=client,
        conditions=append_conditions,
        page_size=page_size,
        max_pages=5,  # Smaller for append test
        bronze_path=bronze_path,
        mode="append"
    )
    bronze_results_append[entity] = result
    print(f"{entity}: +{result['bronze_rows']} rows")

# ---- CELL 7: Verify bronze tables ----
print("\n=== BRONZE LAYER VERIFICATION ===")
for entity in test_entities:
    table_name = f"test.bronze.cw_{entity.lower()}"
    count = spark.table(table_name).count()
    print(f"{table_name}: {count} total rows")

# ---- CELL 8: Silver layer - Overwrite mode ----
from fabric_api.pipeline import process_bronze_to_silver

print("\n=== SILVER LAYER - OVERWRITE MODE ===")
silver_results_overwrite = {}

for entity in test_entities:
    logger.info(f"Processing {entity} to silver (overwrite)...")
    result = process_bronze_to_silver(
        entity_name=entity,
        bronze_path=bronze_path,
        silver_path=silver_path,
        spark=spark
    )
    silver_results_overwrite[entity] = result
    print(f"{entity}: {result.get('silver_rows', 0)} rows")

# ---- CELL 9: Silver layer - Append mode ----
print("\n=== SILVER LAYER - APPEND MODE ===")
# Note: Silver typically overwrites, but we can test incremental processing

# First, let's add more data to bronze
for entity in test_entities[:3]:  # Just test first 3 entities
    process_entity_to_bronze(
        entity_name=entity,
        client=client,
        conditions=append_conditions,
        page_size=50,
        max_pages=2,
        bronze_path=bronze_path,
        mode="append"
    )

# Now process to silver
silver_results_append = {}
for entity in test_entities[:3]:
    logger.info(f"Processing {entity} to silver (incremental)...")
    result = process_bronze_to_silver(
        entity_name=entity,
        bronze_path=bronze_path,
        silver_path=silver_path,
        spark=spark
    )
    silver_results_append[entity] = result
    print(f"{entity}: {result.get('silver_rows', 0)} rows")

# ---- CELL 10: Verify silver tables ----
print("\n=== SILVER LAYER VERIFICATION ===")
for entity in test_entities:
    table_name = f"test.silver.{entity.lower()}"
    try:
        count = spark.table(table_name).count()
        print(f"{table_name}: {count} total rows")
    except:
        print(f"{table_name}: Table not found")

# ---- CELL 11: Gold layer - Overwrite mode ----
from fabric_api.gold.invoice_processing import run_gold_invoice_processing

print("\n=== GOLD LAYER - OVERWRITE MODE ===")
gold_results = run_gold_invoice_processing(
    silver_path=silver_path,
    gold_path=gold_path,
    spark=spark
)

for table, count in gold_results.items():
    print(f"Gold {table}: {count} rows")

# ---- CELL 12: Gold layer - Append simulation ----
print("\n=== GOLD LAYER - APPEND SIMULATION ===")
# Gold typically rebuilds, but we can test with filtered data

# First add more recent data to silver
recent_conditions = f"lastUpdated>=[{append_start}]"
for entity in ["Agreement", "TimeEntry", "PostedInvoice"]:
    process_entity_to_bronze(
        entity_name=entity,
        client=client,
        conditions=recent_conditions,
        page_size=50,
        max_pages=3,
        bronze_path=bronze_path,
        mode="append"
    )
    process_bronze_to_silver(
        entity_name=entity,
        bronze_path=bronze_path,
        silver_path=silver_path,
        spark=spark
    )

# Rebuild gold with new data
gold_results_rebuild = run_gold_invoice_processing(
    silver_path=silver_path,
    gold_path=gold_path,
    spark=spark
)

for table, count in gold_results_rebuild.items():
    print(f"Gold {table} (rebuild): {count} rows")

# ---- CELL 13: Verify gold tables ----
print("\n=== GOLD LAYER VERIFICATION ===")
gold_tables = ["invoice_headers", "invoice_lines", "expense_lines", "agreement_summary"]

for table in gold_tables:
    table_name = f"test.gold.{table}"
    try:
        count = spark.table(table_name).count()
        print(f"{table_name}: {count} total rows")
    except:
        print(f"{table_name}: Table not found")

# ---- CELL 14: Data quality checks ----
print("\n=== DATA QUALITY CHECKS ===")

# Check for nulls in key fields
headers_nulls = spark.table("test.gold.invoice_headers").filter("invoice_id IS NULL").count()
print(f"Invoice headers with null ID: {headers_nulls}")

# Check agreement hierarchy resolution
agreements_with_numbers = spark.table("test.gold.agreement_summary").filter("agreementNumber IS NOT NULL").count()
agreements_total = spark.table("test.gold.agreement_summary").count()
print(f"Agreements with numbers: {agreements_with_numbers}/{agreements_total}")

# Check time entry filtering (Tímapottur)
if spark.catalog.tableExists("test.gold.invoice_lines"):
    filtered_lines = spark.sql("""
        SELECT COUNT(*) as filtered_count
        FROM test.gold.invoice_lines
        WHERE agreement_type = 'Tímapottur'
    """).collect()[0][0]
    print(f"Time entries with Tímapottur (should be 0): {filtered_lines}")

# ---- CELL 15: Performance summary ----
print("\n=== PERFORMANCE SUMMARY ===")
print("Bronze layer:")
for entity, result in bronze_results_overwrite.items():
    print(f"  {entity}: {result['extracted']} extracted, {result['bronze_rows']} written")

print("\nSilver layer:")
for entity, result in silver_results_overwrite.items():
    if result.get('status') == 'success':
        print(f"  {entity}: {result['bronze_rows']} → {result['silver_rows']} rows")

print("\nGold layer:")
for table, count in gold_results.items():
    print(f"  {table}: {count} rows")

# ---- CELL 16: Cleanup (optional) ----
# Uncomment to clean up test tables
# spark.sql("DROP TABLE IF EXISTS test.bronze.cw_agreement")
# spark.sql("DROP TABLE IF EXISTS test.silver.agreement")
# spark.sql("DROP TABLE IF EXISTS test.gold.invoice_headers")
# ... etc