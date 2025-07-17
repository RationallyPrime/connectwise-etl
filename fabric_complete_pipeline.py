# Complete BC Pipeline - Bronze → Silver → Gold
from datetime import datetime
from unified_etl_core.config.models import (
    ETLConfig, 
    LayerConfig, 
    IntegrationConfig,
    SparkConfig, 
    TableNamingConvention
)
from unified_etl_businesscentral.config import SILVER_CONFIG
from unified_etl_businesscentral.orchestrate import orchestrate_bc_gold_layer

# Configuration Setup
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lakehouse_root = "/lakehouse/default/Tables/"

print(f"🔧 Complete BC Pipeline:")
print(f"  Batch ID: {batch_id}")
print(f"  Lakehouse Root: {lakehouse_root}")

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

# Get or create Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BC_Complete_Pipeline").getOrCreate()

# STEP 1: Create Silver Layer Tables
print("\n" + "="*60)
print("🥈 STEP 1: Creating Silver Layer Tables")
print("="*60)

silver_entities = [e for e in BRONZE_TABLE_MAPPING.keys() if e in SILVER_CONFIG]
print(f"📋 Processing {len(silver_entities)} entities")

silver_created = []
silver_errors = []

for entity in silver_entities:
    try:
        # Get the bronze table name with suffix
        bronze_entity = BRONZE_TABLE_MAPPING[entity]
        bronze_path = f"{lakehouse_root}bronze/{bronze_entity}"
        silver_path = f"{lakehouse_root}silver/{entity}"  # Silver uses entity name without suffix
        
        # Read bronze table
        bronze_df = spark.read.format("delta").load(bronze_path)
        row_count = bronze_df.count()
        print(f"  📥 {entity} (from {bronze_entity}): {row_count} rows")
        
        # Basic transformation: add silver metadata
        from pyspark.sql.functions import current_timestamp, lit
        silver_df = bronze_df.withColumn("_etl_silver_processed_at", current_timestamp()) \
                            .withColumn("_etl_batch_id", lit(batch_id))
        
        # Write to silver layer
        silver_df.write.mode("overwrite").format("delta").save(silver_path)
        silver_created.append(entity)
        print(f"  ✅ {entity}: {row_count} rows written to silver")
        
    except Exception as e:
        print(f"  ❌ {entity}: Error - {e}")
        silver_errors.append(f"{entity}: {str(e)}")

print(f"\n📊 Silver Layer Results:")
print(f"  ✅ Created: {len(silver_created)} tables")
print(f"  ❌ Errors: {len(silver_errors)} tables")
if silver_errors:
    print("  Error details:")
    for error in silver_errors:
        print(f"    - {error}")

# STEP 2: Create Gold Layer Tables
print("\n" + "="*60)
print("🥇 STEP 2: Creating Gold Layer Tables")
print("="*60)

if silver_created:
    try:
        # Define paths
        bronze_path = f"{lakehouse_root}bronze"
        silver_path = f"{lakehouse_root}silver"
        gold_path = f"{lakehouse_root}gold"
        
        print(f"  Bronze Path: {bronze_path}")
        print(f"  Silver Path: {silver_path}")
        print(f"  Gold Path: {gold_path}")
        
        # Process gold layer
        stats = orchestrate_bc_gold_layer(
            spark=spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
            gold_path=gold_path,
            batch_id=batch_id
        )
        
        print(f"\n✅ Gold Layer Processing Complete:")
        print(f"  Dimensions created: {stats['dimensions_created']}")
        print(f"  Facts created: {stats['facts_created']}")
        print(f"  Total tables: {stats['dimensions_created'] + stats['facts_created']}")
        
        if stats['errors']:
            print(f"\n⚠️  Warnings/Errors encountered:")
            for error in stats['errors']:
                print(f"  - {error}")
                
    except Exception as e:
        print(f"\n❌ Error processing gold layer: {str(e)}")
        import traceback
        traceback.print_exc()
else:
    print("❌ No silver tables created - skipping gold layer processing")

# STEP 3: Verify Final Results
print("\n" + "="*60)
print("🔍 STEP 3: Verifying Final Results")
print("="*60)

print("\n📊 Final Warehouse Schema Tables:")

# Check key gold tables
gold_tables_to_check = [
    "dim_Customer", "dim_Vendor", "dim_Item", "dim_GLAccount", 
    "dim_Currency", "dim_Company", "dim_Resource",
    "dim_Date", "dim_GD1", "dim_GD2", "dim_GD3",
    "fact_Purchase", "fact_GLEntry", "fact_SalesInvoiceLines"
]

gold_available = []
for table in gold_tables_to_check:
    try:
        gold_df = spark.read.format("delta").load(f"{gold_path}/{table}")
        count = gold_df.count()
        gold_available.append(table)
        print(f"  ✅ {table}: {count} rows")
    except Exception:
        print(f"  ❌ {table}: Not found")

print(f"\n🎯 Pipeline Complete!")
print(f"  Silver tables: {len(silver_created)}")
print(f"  Gold tables: {len(gold_available)}")
print(f"  Ready for warehouse analysis!")