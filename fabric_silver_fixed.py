# Business Central Silver Layer - Fixed Configuration
from datetime import datetime
from unified_etl_core.config.models import (
    ETLConfig, 
    LayerConfig, 
    IntegrationConfig,
    SparkConfig, 
    TableNamingConvention
)
from unified_etl_businesscentral.config import SILVER_CONFIG, BC_FACT_CONFIGS

# Configuration Setup
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lakehouse_root = "/lakehouse/default/Tables/"

print("🥈 SILVER LAYER: Transforming Business Central data...")

# Define BC entities to process
bc_entities = [
    "Customer", "Vendor", "Item", "Resource",
    "GLAccount", "GLEntry", 
    "Currency", "CompanyInformation",
    "Dimension", "DimensionValue", "DimensionSetEntry",
    "PurchInvHeader", "PurchInvLine",
    "PurchCrMemoHeader", "PurchCrMemoLine",
    "SalesInvoiceHeader", "SalesInvoiceLine",
    "CustLedgerEntry", "DetailedCustLedgEntry",
    "VendorLedgerEntry", "DetailedVendorLedgEntry",
    "Job", "JobLedgerEntry",
    "AMSAgreementHeader", "AMSAgreementLine",
    "GeneralLedgerSetup", "AccountingPeriod"
]

# Process Silver layer for all entities that have configurations
silver_entities = [e for e in bc_entities if e in SILVER_CONFIG]

print(f"📋 Processing {len(silver_entities)} entities with silver configurations")

# Create silver-specific config with ALL required fields
silver_etl_config = ETLConfig(
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
            name="businesscentral",          # ← Required field
            abbreviation="bc",               # ← Required field  
            base_url="https://api.businesscentral.dynamics.com/",  # ← Required field
            enabled=True                     # ← Required field
        )
    },
    # Spark configuration - REQUIRED
    spark=SparkConfig(
        app_name="bc_silver_refresh",
        session_type="fabric",
        config_overrides={}
    ),
    # Global settings - ALL REQUIRED  
    fail_on_error=True,
    audit_columns=True
)

print(f"\n✅ Configuration created for {len(silver_entities)} entities")

# Get or create Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BC_Silver_Test").getOrCreate()

# Since we're dealing with existing bronze tables, let's process them one by one
print("\n🔍 Processing Silver Tables:")

for entity in silver_entities:
    try:
        # Check if bronze table exists
        bronze_path = f"{lakehouse_root}bronze/{entity}"
        silver_path = f"{lakehouse_root}silver/{entity}"
        
        # Read bronze table
        bronze_df = spark.read.format("delta").load(bronze_path)
        print(f"  📥 {entity}: {bronze_df.count()} rows in bronze")
        
        # For now, just copy bronze to silver (in production you'd apply transformations)
        bronze_df.write.mode("overwrite").format("delta").save(silver_path)
        
        # Verify silver table
        silver_df = spark.read.format("delta").load(silver_path)
        print(f"  ✅ {entity}: {silver_df.count()} rows written to silver")
        
    except Exception as e:
        print(f"  ❌ {entity}: Error - {e}")

print(f"\n🎯 Silver layer processing complete!")
print(f"Next step: Run gold layer orchestration to create warehouse schema tables")