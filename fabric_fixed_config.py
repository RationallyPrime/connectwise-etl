# Business Central ETL with Fixed Table Naming
# Cell 1: Install packages (if needed)
# %pip install --force-reinstall unified_etl_businesscentral-1.0.0-py3-none-any.whl

# Cell 2: Configuration with correct table naming
from datetime import datetime
from unified_etl_core.config.models import ETLConfig, LayerConfig, IntegrationConfig, SparkConfig, TableNamingConvention
from unified_etl_core.main import run_etl_pipeline
from pyspark.sql import SparkSession

batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
spark = SparkSession.builder.appName("BC_ETL").getOrCreate()

# Fixed config - BC tables don't use prefixes
config = ETLConfig(
    bronze=LayerConfig(
        catalog="LH", 
        schema="bronze", 
        prefix="",  # No prefix for BC bronze tables
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    silver=LayerConfig(
        catalog="LH", 
        schema="silver", 
        prefix="",  # No prefix for BC silver tables
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    gold=LayerConfig(
        catalog="LH", 
        schema="gold", 
        prefix="",  # No prefix for BC gold tables
        naming_convention=TableNamingConvention.CAMELCASE
    ),
    integrations={
        "businesscentral": IntegrationConfig(
            name="businesscentral",
            abbreviation="",  # No abbreviation for BC tables
            base_url="https://api.businesscentral.dynamics.com/",
            enabled=True
        )
    },
    spark=SparkConfig(
        app_name="bc_etl",
        session_type="fabric",
        config_overrides={}
    ),
    fail_on_error=True,
    audit_columns=True
)

# Cell 3: Test table naming
print("🔍 Testing Table Naming:")
print(f"Bronze customer: {config.get_table_name('bronze', 'businesscentral', 'customer')}")
print(f"Silver customer: {config.get_table_name('silver', 'businesscentral', 'customer')}")
print(f"Gold customer: {config.get_table_name('gold', 'businesscentral', 'customer')}")

# Cell 4: Run ETL Pipeline
print(f"\n🚀 Running Business Central ETL Pipeline")
print(f"  Batch ID: {batch_id}")
print(f"  Mode: full (schema overwrite)")
print(f"  Layers: silver, gold")

result = run_etl_pipeline(
    config=config,
    spark=spark,
    integrations=["businesscentral"],
    layers=["silver", "gold"],  # Bronze auto-updates from BC2ADLS
    mode="full",  # Full refresh with schema overwrite
    lookback_days=30  # Required positive value
)

print(f"✅ ETL Pipeline Complete!")
print(f"Result: {result}")

# Cell 5: Verify Results
print("\n🔍 Verifying Results:")

# Check key silver tables
silver_tables = ["customer", "vendor", "item", "glaccount", "glentry"]
for table in silver_tables:
    try:
        df = spark.table(f"LH.silver.{table}")
        print(f"  ✅ Silver {table}: {df.count()} rows")
    except Exception as e:
        print(f"  ❌ Silver {table}: {e}")

# Check key gold tables  
gold_tables = ["dim_customer", "dim_vendor", "dim_item", "dim_glaccount", "fact_glentry"]
for table in gold_tables:
    try:
        df = spark.table(f"LH.gold.{table}")
        print(f"  ✅ Gold {table}: {df.count()} rows")
    except Exception as e:
        print(f"  ❌ Gold {table}: {e}")