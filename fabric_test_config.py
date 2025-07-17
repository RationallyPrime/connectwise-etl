# Business Central ETL Configuration for Fabric
from datetime import datetime
from unified_etl_core.config.models import (
    ETLConfig, 
    LayerConfig, 
    IntegrationConfig,
    SparkConfig, 
    TableNamingConvention
)

# Configuration Setup
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lakehouse_root = "/lakehouse/default/Tables/"

print(f"🔧 Configuration:")
print(f"  Batch ID: {batch_id}")
print(f"  Lakehouse Root: {lakehouse_root}")

# Define all BC entities to process
bc_entities = [
    # Core Entities
    "Customer", "Vendor", "Item", "Resource",
    "GLAccount", "GLEntry", 
    "Currency", "CompanyInformation",
    
    # Dimension Entities
    "Dimension", "DimensionValue", "DimensionSetEntry",
    
    # Purchase Documents
    "PurchInvHeader", "PurchInvLine",
    "PurchCrMemoHeader", "PurchCrMemoLine",
    
    # Sales Documents  
    "SalesInvoiceHeader", "SalesInvoiceLine",
    
    # Ledger Entries
    "CustLedgerEntry", "DetailedCustLedgEntry",
    "VendorLedgerEntry", "DetailedVendorLedgEntry",
    
    # Job/Project
    "Job", "JobLedgerEntry",
    
    # Agreement Management
    "AMSAgreementHeader", "AMSAgreementLine",
    
    # Setup Tables
    "GeneralLedgerSetup", "AccountingPeriod"
]

# Create ETL Configuration with all required fields
etl_config = ETLConfig(
    # Layer configurations - ALL REQUIRED
    bronze=LayerConfig(
        catalog="LH",
        schema="bronze",  # Changed from schema_name to schema
        prefix="bronze_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    silver=LayerConfig(
        catalog="LH",
        schema="silver",  # Changed from schema_name to schema
        prefix="silver_", 
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    gold=LayerConfig(
        catalog="LH",
        schema="gold",  # Changed from schema_name to schema
        prefix="gold_",
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    # Integration configurations - ALL REQUIRED
    integrations={
        "businesscentral": IntegrationConfig(
            name="businesscentral",
            abbreviation="bc",
            base_url="https://api.businesscentral.dynamics.com/",  # Placeholder URL
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

print(f"\n📊 Processing {len(bc_entities)} Business Central entities")

# Test Gold Layer Processing
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from unified_etl_businesscentral.orchestrate import process_bc_gold_layer
    
    # Get or create Spark session
    spark = SparkSession.builder.appName("BC_Gold_Test").getOrCreate()
    
    # Define paths
    bronze_path = f"{lakehouse_root}bronze"
    silver_path = f"{lakehouse_root}silver"
    gold_path = f"{lakehouse_root}gold"
    
    print(f"\n🚀 Processing Business Central Gold Layer")
    print(f"  Bronze Path: {bronze_path}")
    print(f"  Silver Path: {silver_path}")
    print(f"  Gold Path: {gold_path}")
    
    try:
        # Process gold layer
        gold_tables = process_bc_gold_layer(
            spark=spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
            gold_path=gold_path
        )
        
        print(f"\n✅ Successfully created {len(gold_tables)} gold tables:")
        for table in sorted(gold_tables):
            print(f"  - {table}")
            
    except Exception as e:
        print(f"\n❌ Error processing gold layer: {str(e)}")
        raise