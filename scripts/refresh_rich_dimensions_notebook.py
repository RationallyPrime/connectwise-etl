# Cell 1: Rich Dimension Refresh for ConnectWise
# Run this in a Fabric notebook after installing the wheels

import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("🚀 Starting ConnectWise Rich Dimension Refresh...")

# Get active Spark session
spark = SparkSession.getActiveSession()
if not spark:
    print("❌ No active Spark session found. This script must run in a Fabric notebook.")
    exit(1)

# Import the rich dimension function
from unified_etl_connectwise.dimension_config import refresh_connectwise_dimensions

try:
    # This will create all 17 rich dimensions with business context
    dimensions = refresh_connectwise_dimensions(spark)
    
    print(f"\n✅ Successfully created {len(dimensions)} rich dimensions!")
    
    # Show detailed summary
    print("\n📊 Rich Dimension Summary:")
    for dim_name, dim_df in dimensions.items():
        count = dim_df.count()
        columns = dim_df.columns
        print(f"  {dim_name}: {count} rows, {len(columns)} columns")
        
        # Show rich attributes (beyond basic Key/Code)
        rich_attrs = [col for col in columns if col not in [f"{dim_name.replace('dim', '')}Key", f"{dim_name.replace('dim', '')}Code", "usage_count", "isActive", "effectiveDate", "endDate", "_etl_gold_processed_at", "_etl_source", "_etl_batch_id"]]
        if rich_attrs:
            print(f"    Rich attributes: {', '.join(rich_attrs)}")
    
    # Validate key dimensions
    print("\n🔍 Validating Key Dimensions:")
    
    # Check AgreementType for Tímapottur detection
    try:
        agreement_type_df = spark.table("Lakehouse.gold.dimAgreementType")
        timapottur_count = agreement_type_df.filter("isTimapottur = true").count()
        print(f"  ✅ dimAgreementType: {timapottur_count} Tímapottur agreements detected")
    except Exception as e:
        print(f"  ❌ dimAgreementType validation failed: {e}")
    
    # Check BillableStatus categories
    try:
        billable_df = spark.table("Lakehouse.gold.dimBillableStatus")
        billable_categories = billable_df.select("billableCategory").distinct().collect()
        categories = [row.billableCategory for row in billable_categories]
        print(f"  ✅ dimBillableStatus: Categories {categories}")
    except Exception as e:
        print(f"  ❌ dimBillableStatus validation failed: {e}")
    
    # Check TimeEntryStatus workflow
    try:
        status_df = spark.table("Lakehouse.gold.dimTimeEntryStatus")
        status_categories = status_df.select("statusCategory").distinct().collect()
        categories = [row.statusCategory for row in status_categories]
        print(f"  ✅ dimTimeEntryStatus: Status categories {categories}")
    except Exception as e:
        print(f"  ❌ dimTimeEntryStatus validation failed: {e}")
    
    # Check Member dimension
    try:
        member_df = spark.table("Lakehouse.gold.dimMember")
        has_capacity_count = member_df.filter("hasCapacity = true").count()
        print(f"  ✅ dimMember: {has_capacity_count} members with capacity > 0")
    except Exception as e:
        print(f"  ❌ dimMember validation failed: {e}")
    
    print("\n🎯 Rich Dimension Refresh Complete!")
    print("Your dimensions now include:")
    print("  • Icelandic business logic (Tímapottur detection)")
    print("  • 11-state approval workflow categories")
    print("  • Utilization analysis flags")
    print("  • Billing status categorization")
    print("  • Company/Member/Department hierarchies")
    print("  • All in proper camelCase naming!")

except Exception as e:
    logger.error(f"Rich dimension refresh failed: {e}")
    print(f"❌ Error: {e}")
    raise

# Cell 2: Optional - Create Facts After Dimensions
print("\n" + "="*50)
print("🔄 Optional: Now create facts using rich dimensions...")

# Uncomment to run fact creation after dimensions
# from unified_etl_core.main import run_etl_pipeline
# 
# entity_configs = {
#     "timeentry": {
#         "source": "connectwise",
#         "surrogate_keys": [{"name": "TimeentrySK", "business_keys": ["id"]}],
#         "business_keys": [{"name": "TimeentryBusinessKey", "source_columns": ["id"]}],
#         "calculated_columns": {}
#     }
# }
# 
# run_etl_pipeline(
#     integrations=["connectwise"],
#     layers=["gold"],
#     mode="full",
#     config={"entities": entity_configs}
# )