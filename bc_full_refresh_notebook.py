#!/usr/bin/env python
# coding: utf-8

# ## Business Central Full Refresh Notebook
# 
# Proper implementation following unified ETL framework philosophy

# In[1]:

# Cell 1: Install Required Wheels
print("📦 Installing unified ETL packages...")

# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install /lakehouse/default/Files/unified_etl_core-1.0.0-py3-none-any.whl
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install /lakehouse/default/Files/unified_etl_businesscentral-1.0.0-py3-none-any.whl

# Note: After running this cell, you may need to restart the kernel


# In[2]:

# Cell 2: Initialize Session and Configuration
import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("🚀 Starting Business Central Full Data Refresh...")
print(f"📅 Timestamp: {datetime.now().isoformat()}")

# Get active Spark session from Fabric
spark = SparkSession.getActiveSession()
if not spark:
    print("❌ No active Spark session found. This script must run in a Fabric notebook.")
    sys.exit(1)

# Import framework components
from unified_etl_core.main import run_etl_pipeline
from unified_etl_core.config import ETLConfig, LayerConfig, IntegrationConfig, SparkConfig, TableNamingConvention

# Create proper ETL configuration following fail-fast philosophy
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ALL parameters are REQUIRED - fail-fast philosophy
etl_config = ETLConfig(
    # Layer configurations - ALL REQUIRED
    bronze=LayerConfig(
        catalog="LH",
        schema="bronze",
        prefix="bronze_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    silver=LayerConfig(
        catalog="LH",
        schema="silver",
        prefix="silver_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    gold=LayerConfig(
        catalog="LH",
        schema="gold",
        prefix="gold_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    # Integration configurations - ALL REQUIRED
    integrations={
        "businesscentral": IntegrationConfig(
            name="businesscentral",
            abbreviation="bc",
            base_url="https://api.businesscentral.dynamics.com/",
            enabled=True
        )
    },
    # Spark configuration - REQUIRED
    spark=SparkConfig(
        app_name="bc_full_refresh",
        session_type="fabric",
        config_overrides={}
    ),
    # Global settings - ALL REQUIRED
    fail_on_error=True,
    audit_columns=True
)

print(f"✅ Configuration initialized")
print(f"   Batch ID: {batch_id}")


# In[3]:

# Cell 3: Skip Bronze Layer (BC2ADLS handles bronze extraction)
print("\n" + "="*50)
print("🥉 BRONZE LAYER: Skipping - BC2ADLS automatically lands bronze data")
print("="*50)

# The bronze layer is handled by BC2ADLS pipeline
# We only need to verify that tables exist before proceeding
print("\n🔍 Verifying Bronze Tables exist...")

# Sample verification - in production you might want to check all expected tables
sample_entities = ["Customer", "Vendor", "Item", "GLAccount", "GLEntry"]
bronze_verified = 0

for entity in sample_entities:
    try:
        # Check if bronze table exists with BC2ADLS naming pattern
        bronze_tables = spark.catalog.listTables("bronze")
        bc_tables = [t.name for t in bronze_tables if entity in t.name]
        if bc_tables:
            df = spark.table(f"bronze.{bc_tables[0]}")
            count = df.count()
            print(f"  ✅ {entity} (as {bc_tables[0]}): {count} rows")
            bronze_verified += 1
        else:
            print(f"  ❌ {entity}: No bronze table found")
    except Exception as e:
        print(f"  ❌ {entity}: Error - {e}")

if bronze_verified >= 3:
    print(f"\n✅ Bronze layer verified! BC2ADLS has landed data successfully")
else:
    print(f"\n⚠️ Bronze layer verification incomplete - check BC2ADLS pipeline")


# In[4]:

# Cell 4: Run Silver Layer Processing
print("\n" + "="*50)
print("🥈 SILVER LAYER: Processing Business Central data...")
print("="*50)

try:
    # Run silver layer processing using the framework
    run_etl_pipeline(
        config=etl_config,
        spark=spark,
        integrations=["businesscentral"],
        layers=["silver"],
        mode="full",
        lookback_days=30  # Required even for full mode
    )
    
    print("✅ Silver layer processing complete!")
    
except Exception as e:
    logger.error(f"Silver layer processing failed: {e}")
    print(f"❌ Silver Layer Error: {e}")
    raise


# In[5]:

# Cell 5: Run Gold Layer Processing
print("\n" + "="*50)
print("🥇 GOLD LAYER: Creating Business Central warehouse schema...")
print("="*50)

try:
    # Run gold layer processing using the framework
    run_etl_pipeline(
        config=etl_config,
        spark=spark,
        integrations=["businesscentral"],
        layers=["gold"],
        mode="full",
        lookback_days=30  # Required even for full mode
    )
    
    print("✅ Gold layer processing complete!")
    
except Exception as e:
    logger.error(f"Gold layer processing failed: {e}")
    print(f"❌ Gold Layer Error: {e}")
    raise


# In[6]:

# Cell 6: Final Summary & Validation
print("\n" + "="*50)
print("📊 BUSINESS CENTRAL REFRESH SUMMARY")
print("="*50)

try:
    # Count records in each layer
    print("\n🥉 Bronze Layer:")
    bronze_tables = spark.catalog.listTables("bronze")
    bc_bronze = [t.name for t in bronze_tables if "BC_" in t.name or any(entity in t.name for entity in ["Customer", "Vendor", "Item", "GLAccount", "GLEntry"])]
    for table in bc_bronze[:5]:  # Show first 5
        try:
            count = spark.table(f"bronze.{table}").count()
            print(f"  {table}: {count:,} rows")
        except:
            pass
    if len(bc_bronze) > 5:
        print(f"  ... and {len(bc_bronze) - 5} more tables")
    
    print("\n🥈 Silver Layer:")
    silver_tables = spark.catalog.listTables("silver")
    bc_silver = [t.name for t in silver_tables if t.name in ["Customer", "Vendor", "Item", "GLAccount", "GLEntry", "Dimension", "DimensionValue"]]
    for table in bc_silver:
        try:
            count = spark.table(f"silver.{table}").count()
            print(f"  {table}: {count:,} rows")
        except:
            pass
    
    print("\n🥇 Gold Layer:")
    gold_tables = spark.catalog.listTables("gold")
    
    # Show dimensions
    print("  Dimensions:")
    dims = [t.name for t in gold_tables if t.name.startswith("dim_")]
    for dim in dims:
        try:
            count = spark.table(f"gold.{dim}").count()
            print(f"    {dim}: {count:,} rows")
        except:
            pass
    
    # Show facts
    print("  Facts:")
    facts = [t.name for t in gold_tables if t.name.startswith("fact_")]
    for fact in facts:
        try:
            count = spark.table(f"gold.{fact}").count()
            print(f"    {fact}: {count:,} rows")
        except:
            pass
    
    print(f"\n🎯 Business Central Full Refresh Complete!")
    print(f"   Batch ID: {batch_id}")
    print(f"   Timestamp: {datetime.now().isoformat()}")
    
except Exception as e:
    print(f"⚠️ Summary generation had issues: {e}")

print("\n✨ Your Business Central data is now ready for analysis!")
print("   Next steps:")
print("   • Create Power BI reports on the Gold layer")
print("   • Set up incremental refresh schedules")
print("   • Monitor data quality metrics")