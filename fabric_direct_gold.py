# Direct Business Central Gold Layer Creation
# Cell 1: Install packages
# %pip install unified_etl_core-1.0.0-py3-none-any.whl
# %pip install unified_etl_businesscentral-1.0.0-py3-none-any.whl

# Cell 2: Direct gold layer creation
from datetime import datetime
from pyspark.sql import SparkSession
from unified_etl_businesscentral.orchestrate import orchestrate_bc_gold_layer

# Simple approach - just call the BC orchestration directly
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
spark = SparkSession.builder.appName("BC_Gold_Direct").getOrCreate()

print(f"🔧 Direct Gold Layer Creation:")
print(f"  Batch ID: {batch_id}")

# Call the orchestration function directly
try:
    stats = orchestrate_bc_gold_layer(
        spark=spark,
        bronze_path="abfss://doesnt-matter",  # Not used since we read from tables
        silver_path="abfss://doesnt-matter",  # Not used since we read from tables  
        gold_path="/lakehouse/default/Tables/gold",
        batch_id=batch_id
    )
    
    print(f"✅ Gold Layer Complete:")
    print(f"  Dimensions: {stats['dimensions_created']}")
    print(f"  Facts: {stats['facts_created']}")
    print(f"  Errors: {len(stats['errors'])}")
    
    if stats['errors']:
        for error in stats['errors']:
            print(f"  ⚠️  {error}")
            
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()