# Business Central ETL - Using Improved Framework Architecture
# Updated to use the unified ETL pipeline with BC2ADLS Bronze processing

from datetime import datetime
from pyspark.sql import SparkSession

print("🚀 Starting Business Central ETL using improved framework architecture...")
print(f"📅 Timestamp: {datetime.now().isoformat()}")

# Get active Spark session from Fabric
spark = SparkSession.getActiveSession()
if not spark:
    print("❌ No active Spark session found. This script must run in a Fabric notebook.")
    exit(1)

print("\n🏗️ Framework Architecture Overview:")
print("   Bronze: BC2ADLS data → Pydantic validation → SparkDantic schemas")
print("   Silver: Framework transformations with apply_silver_transformations()")  
print("   Gold: Individual fact creators following ConnectWise patterns")

try:
    # Import the unified ETL pipeline
    from unified_etl_core.main import run_etl_pipeline
    from unified_etl_core.config import ETLConfig, LayerConfig, IntegrationConfig, SparkConfig
    from unified_etl_core.config.models import TableNamingConvention
    
    print("   ✅ Imported unified ETL pipeline components")
    
    print("\n🔧 Configuring ETL pipeline...")
    
    # Create ETL configuration for Business Central
    config = ETLConfig(
        bronze=LayerConfig(
            catalog="lakehouse",
            schema="bronze", 
            prefix="",  # BC2ADLS tables have no prefix (customer18, item27, etc.)
            naming_convention=TableNamingConvention.CAMELCASE
        ),
        silver=LayerConfig(
            catalog="lakehouse",
            schema="silver",
            prefix="silver_", 
            naming_convention=TableNamingConvention.UNDERSCORE
        ),
        gold=LayerConfig(
            catalog="lakehouse", 
            schema="gold",
            prefix="gold_",
            naming_convention=TableNamingConvention.UNDERSCORE
        ),
        integrations={
            "businesscentral": IntegrationConfig(
                name="businesscentral",
                abbreviation="bc", 
                base_url="https://api.businesscentral.dynamics.com/v2.0",
                enabled=True
            )
        },
        spark=SparkConfig(
            app_name="BC-ETL-Pipeline",
            session_type="fabric",
            config_overrides={}
        ),
        fail_on_error=True,
        audit_columns=True
    )
    
    print("   ✅ ETL configuration created")
    
    print("\n🔍 Detecting available integrations...")
    
    # Check if Business Central integration is available
    from unified_etl_core.integrations import detect_available_integrations
    
    integrations = detect_available_integrations()
    if not integrations.get("businesscentral", {}).get("available"):
        print("❌ Business Central integration not available")
        print("   Make sure unified-etl-businesscentral package is installed")
        exit(1)
        
    bc_info = integrations["businesscentral"]
    print(f"   ✅ Business Central integration detected")
    print(f"   - Extractor: {bc_info['extractor'].__name__}")
    print(f"   - Models: {len(bc_info['models'])} entities")
    print(f"   - Entity configs: {len(bc_info['entity_configs'])} configs")
    
    print("\n📊 Business Central entities available:")
    for i, entity_name in enumerate(list(bc_info['models'].keys())[:8]):
        print(f"   {i+1}. {entity_name}")
    if len(bc_info['models']) > 8:
        print(f"   ... and {len(bc_info['models']) - 8} more")
    
    print(f"\n⚙️ BC2ADLS Configuration:")
    import os
    bc2adls_path = os.environ.get("BC2ADLS_PATH", "/lakehouse/default/Files/bc2adls")
    print(f"   BC2ADLS Path: {bc2adls_path}")
    print(f"   (Set BC2ADLS_PATH environment variable if different)")
    
    print("\n🚀 Running unified ETL pipeline for Business Central...")
    print("   This uses the improved architecture with:")
    print("   - Bronze: BC2ADLS processor with Pydantic validation") 
    print("   - Silver: Core framework transformations using SparkDantic schemas")
    print("   - Gold: Individual fact creators (no orchestration)")
    print("   - Fixed: Consistent with ConnectWise - both use modern EntityConfig system")
    
    # Run the complete ETL pipeline
    run_etl_pipeline(
        config=config,
        spark=spark,
        integrations=["businesscentral"],
        layers=["bronze", "silver", "gold"],
        mode="full",
        lookback_days=30
    )
    
    print("\n✅ Business Central ETL pipeline completed successfully!")
    
    print("\n📋 Pipeline Summary:")
    print("   ✅ Bronze: BC2ADLS data processed with Pydantic validation")
    print("   ✅ Silver: Framework transformations applied")
    print("   ✅ Gold: Fact tables and dimensions created")
    
    print(f"\n⏰ Completion time: {datetime.now().isoformat()}")
    
    print("\n🔍 Verifying created tables...")
    
    # List some of the created tables
    catalog_name = spark.catalog.currentCatalog()
    
    # Check bronze tables (BC2ADLS naming: customer18, item27, etc.)
    try:
        all_bronze = [t.name for t in spark.catalog.listTables("bronze")]
        # Look for common BC table patterns
        bc_patterns = ["customer", "item", "vendor", "glaccount", "glentry"]
        bronze_tables = [t for t in all_bronze if any(t.lower().startswith(pattern) for pattern in bc_patterns)]
        print(f"   Bronze tables: {len(bronze_tables)} BC tables found")
        for table in bronze_tables[:3]:
            count = spark.table(f"bronze.{table}").count()
            print(f"     - {table}: {count} rows")
    except Exception as e:
        print(f"   Bronze tables: Could not verify ({e})")
    
    # Check silver tables  
    try:
        silver_tables = [t.name for t in spark.catalog.listTables("silver") if t.name.startswith("silver_bc_")]
        print(f"   Silver tables: {len(silver_tables)} created")
        for table in silver_tables[:3]:
            count = spark.table(f"silver.{table}").count()
            print(f"     - {table}: {count} rows")
    except Exception as e:
        print(f"   Silver tables: Could not verify ({e})")
        
    # Check gold tables
    try:
        gold_tables = [t.name for t in spark.catalog.listTables("gold") if t.name.startswith(("dim_", "fact_"))]
        print(f"   Gold tables: {len(gold_tables)} created")
        for table in gold_tables[:5]:
            count = spark.table(f"gold.{table}").count()
            print(f"     - {table}: {count} rows")
    except Exception as e:
        print(f"   Gold tables: Could not verify ({e})")
    
except Exception as e:
    print(f"❌ Pipeline Error: {e}")
    import traceback
    traceback.print_exc()
    raise

print("\n✨ Business Central data pipeline complete!")
print("   Your data is now ready for analysis with:")
print("   - Validated raw data in Bronze layer")
print("   - Standardized entities in Silver layer") 
print("   - Analytics-ready facts and dimensions in Gold layer")
print("\n🔗 Architecture Benefits:")
print("   ✅ Schema-first with SparkDantic models")
print("   ✅ Structured logging and error handling") 
print("   ✅ Framework consistency with ConnectWise")
print("   ✅ Individual fact creators (no monolithic orchestration)")
print("   ✅ Fail-fast validation and processing")