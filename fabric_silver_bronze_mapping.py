# Business Central Silver Layer - With Bronze Table Mapping
from datetime import datetime
from unified_etl_core.config.models import (
    ETLConfig, 
    LayerConfig, 
    IntegrationConfig,
    SparkConfig, 
    TableNamingConvention
)
from unified_etl_businesscentral.config import SILVER_CONFIG

# Configuration Setup
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lakehouse_root = "/lakehouse/default/Tables/"

print("🥈 SILVER LAYER: Transforming Business Central data...")

# Bronze table mapping - bronze tables have numeric suffixes but silver configs don't
BRONZE_TABLE_MAPPING = {
    "Customer": "Customer18",
    "Vendor": "Vendor23", 
    "Item": "Item27",
    "Resource": "Resource156",
    "GLAccount": "GLAccount15",
    "GLEntry": "GLEntry17",
    "Currency": "Currency4",
    "CompanyInformation": "CompanyInformation79",
    "Dimension": "Dimension348",
    "DimensionValue": "DimensionValue349",
    "DimensionSetEntry": "DimensionSetEntry480",
    "SalesInvoiceHeader": "SalesInvoiceHeader112",
    "SalesInvoiceLine": "SalesInvoiceLine113",
    "CustLedgerEntry": "CustLedgerEntry21",
    "DetailedCustLedgEntry": "DetailedCustLedgEntry379",
    "VendorLedgerEntry": "VendorLedgerEntry25",
    "DetailedVendorLedgEntry": "DetailedVendorLedgEntry380",
    "Job": "Job167",
    "JobLedgerEntry": "JobLedgerEntry169",
    "GeneralLedgerSetup": "GeneralLedgerSetup98",
    "AccountingPeriod": "AccountingPeriod50"
}

# Process Silver layer for all entities that have configurations
silver_entities = [e for e in BRONZE_TABLE_MAPPING.keys() if e in SILVER_CONFIG]

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
            name="businesscentral",
            abbreviation="bc",
            base_url="https://api.businesscentral.dynamics.com/",
            enabled=True
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

# Process silver tables with correct bronze table names
print("\n🔍 Processing Silver Tables:")

for entity in silver_entities:
    try:
        # Get the bronze table name with suffix
        bronze_entity = BRONZE_TABLE_MAPPING[entity]
        bronze_path = f"{lakehouse_root}bronze/{bronze_entity}"
        silver_path = f"{lakehouse_root}silver/{entity}"  # Silver uses entity name without suffix
        
        # Read bronze table
        bronze_df = spark.read.format("delta").load(bronze_path)
        print(f"  📥 {entity} (from {bronze_entity}): {bronze_df.count()} rows in bronze")
        
        # For now, just copy bronze to silver (in production you'd apply transformations)
        bronze_df.write.mode("overwrite").format("delta").save(silver_path)
        
        # Verify silver table
        silver_df = spark.read.format("delta").load(silver_path)
        print(f"  ✅ {entity}: {silver_df.count()} rows written to silver")
        
    except Exception as e:
        print(f"  ❌ {entity}: Error - {e}")

print(f"\n🎯 Silver layer processing complete!")
print(f"Next step: Run gold layer orchestration to create warehouse schema tables")

# Show what's available for gold layer
print(f"\n📊 Available for Gold Layer:")
print(f"  Silver entities: {len(silver_entities)}")
for entity in silver_entities:
    config = SILVER_CONFIG[entity]
    print(f"  - {entity} → {config['gold_name']}")