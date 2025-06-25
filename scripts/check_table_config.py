#!/usr/bin/env python3
"""
Check table configuration alignment between SILVER_CONFIG and actual lakehouse tables.
Run this as a single cell in Fabric notebook to diagnose config issues.
"""

from unified_etl_connectwise import ConnectWiseClient
from unified_etl_connectwise.config import SILVER_CONFIG
from pyspark.sql import functions as F

# Initialize spark
client = ConnectWiseClient()
spark = client.spark

print("=== Table Configuration Alignment Check ===\n")

# Get all tables from different schemas
print("Scanning for tables in different schemas...")
schemas_to_check = [
    ("default", ""),
    ("silver", "silver."),
    ("bronze", "bronze."),
    ("gold", "gold."),
]

all_tables = []
for schema_name, prefix in schemas_to_check:
    try:
        if prefix:
            tables = spark.sql(f"SHOW TABLES IN Lakehouse.{schema_name}")
        else:
            tables = spark.sql("SHOW TABLES")

        for row in tables.collect():
            table_name = row.tableName
            full_name = f"{prefix}{table_name}" if prefix else table_name
            all_tables.append(
                {
                    "schema": schema_name,
                    "table": table_name,
                    "full_name": full_name,
                    "catalog_path": f"Lakehouse.{schema_name}.{table_name}"
                    if prefix
                    else table_name,
                }
            )
    except Exception as e:
        print(f"  Could not list tables in {schema_name}: {e}")

# Organize by layer
bronze_tables = [t for t in all_tables if t["table"].startswith("bronze_cw_")]
silver_tables = [t for t in all_tables if t["table"].startswith("silver_cw_")]
gold_tables = [t for t in all_tables if t["table"].startswith("gold_")]

print(f"\nFound tables:")
print(f"  Bronze: {len(bronze_tables)}")
print(f"  Silver: {len(silver_tables)}")
print(f"  Gold: {len(gold_tables)}")

# Check SILVER_CONFIG alignment
print("\n=== SILVER_CONFIG Analysis ===")
print(f"Configured entities: {list(SILVER_CONFIG['entities'].keys())}")

issues = []
fixes = {}

for entity, config in SILVER_CONFIG["entities"].items():
    print(f"\n{entity}:")
    bronze_cfg = config["bronze_table"]
    silver_cfg = config["silver_table"]

    # Check Bronze
    bronze_found = any(t["table"] == bronze_cfg for t in bronze_tables)
    print(f"  Bronze: {bronze_cfg} {'✓' if bronze_found else '✗'}")

    # Check Silver - try both the config name and expected name
    silver_found = any(t["table"] == silver_cfg for t in silver_tables)
    expected_silver = f"silver_cw_{entity.lower()}"
    silver_expected_found = any(t["table"] == expected_silver for t in silver_tables)

    if not silver_found and silver_expected_found:
        print(f"  Silver: {silver_cfg} ✗ (but {expected_silver} exists!)")
        issues.append(f"{entity}: silver_table should be '{expected_silver}' not '{silver_cfg}'")
        fixes[entity] = {"silver_table": expected_silver}
    else:
        print(f"  Silver: {silver_cfg} {'✓' if silver_found else '✗'}")

# Check for tables not in config
print("\n=== Tables Not in Config ===")
configured_bronze = {config["bronze_table"] for config in SILVER_CONFIG["entities"].values()}
configured_silver = {config["silver_table"] for config in SILVER_CONFIG["entities"].values()}

actual_bronze = {t["table"] for t in bronze_tables}
actual_silver = {t["table"] for t in silver_tables}

untracked_bronze = actual_bronze - configured_bronze
untracked_silver = actual_silver - configured_silver

if untracked_bronze:
    print(f"\nBronze tables not in config: {untracked_bronze}")
    # Try to guess entity name
    for table in untracked_bronze:
        entity_guess = table.replace("bronze_cw_", "").title()
        print(f"  - {table} (entity name might be: {entity_guess})")

if untracked_silver:
    print(f"\nSilver tables not in config: {untracked_silver}")

# Check catalog paths
print("\n=== Catalog Path Testing ===")
test_tables = [
    ("bronze_cw_agreement", ["bronze_cw_agreement", "Lakehouse.dbo.bronze_cw_agreement"]),
    (
        "silver_cw_agreement",
        [
            "silver_cw_agreement",
            "Lakehouse.silver.silver_cw_agreement",
            "silver.silver_cw_agreement",
        ],
    ),
]

for table_name, paths_to_try in test_tables:
    print(f"\nTesting {table_name}:")
    for path in paths_to_try:
        try:
            count = spark.table(path).count()
            print(f"  ✓ {path} - {count} rows")
            break
        except:
            print(f"  ✗ {path}")

# Print fixes
if issues:
    print("\n=== Required Config Fixes ===")
    for issue in issues:
        print(f"  - {issue}")

    print("\n=== config.py Updates Needed ===")
    for entity, updates in fixes.items():
        print(f"\n# {entity}")
        for key, value in updates.items():
            print(f'  "{key}": "{value}",')

# Check refresh endpoints alignment
print("\n=== Refresh Endpoints Check ===")
refresh_endpoints = {
    "TimeEntry": "bronze_cw_timeentry",
    "Agreement": "bronze_cw_agreement",
    "Invoice": "bronze_cw_invoice",  # There's only one invoice table
    "ExpenseEntry": "bronze_cw_expenseentry",
    "ProductItem": "bronze_cw_productitem",
}

for entity, expected_table in refresh_endpoints.items():
    exists = expected_table in actual_bronze
    in_config = entity in SILVER_CONFIG["entities"]
    print(f"{entity}: Bronze {'✓' if exists else '✗'}, Config {'✓' if in_config else '✗'}")

    if not in_config:
        print(f"  WARNING: {entity} is fetched but not in SILVER_CONFIG!")

print("\n=== Summary ===")
print(f"Total issues found: {len(issues)}")
print(f"Tables are in schema: Lakehouse.silver for Silver layer")
print(f"Bronze tables appear to be in default schema")
