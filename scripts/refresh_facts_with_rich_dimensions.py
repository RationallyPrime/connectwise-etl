# Cell 1: Refresh Facts with New Rich Dimensions
# Run this in a Fabric notebook after creating rich dimensions and installing updated wheels

import logging

from unified_etl_core.main import run_etl_pipeline

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

print("🚀 Starting fact table refresh with new rich dimension keys...")

# Full entity configs needed for fact creation
entity_configs = {
    "agreement": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "AgreementSK", "business_keys": ["id"]}],
        "business_keys": [{"name": "AgreementBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {
            "estimated_monthly_revenue": "CASE WHEN applicationUnits = 'Amount' THEN COALESCE(applicationLimit, 0) ELSE 0 END"
        }
    },
    "invoice": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "InvoiceSK", "business_keys": ["id"]}],
        "business_keys": [{"name": "InvoiceBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {}
    },
    "timeentry": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "TimeentrySK", "business_keys": ["id"]}],
        "business_keys": [{"name": "TimeentryBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {}
    },
    "expenseentry": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "ExpenseentrySK", "business_keys": ["id"]}],
        "business_keys": [{"name": "ExpenseentryBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {}
    },
    "productitem": {
        "source": "connectwise",
        "surrogate_keys": [{"name": "ProductitemSK", "business_keys": ["id"]}],
        "business_keys": [{"name": "ProductitemBusinessKey", "source_columns": ["id"]}],
        "calculated_columns": {}
    }
}

# This will create/update fact tables with all the new dimension keys
run_etl_pipeline(
    integrations=["connectwise"],
    layers=["gold"],
    mode="full",
    config={"entities": entity_configs}
)

print("\n✅ Fact table refresh complete!")

# Show fact table summary
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
if spark:
    try:
        # Check fact tables
        fact_tables = [
            "gold_cw_fact_timeentry",
            "gold_cw_fact_invoice_line",
            "gold_cw_fact_expenseentry"
        ]

        print("\n📊 Fact Table Summary:")
        for table in fact_tables:
            try:
                df = spark.table(f"Lakehouse.gold.{table}")
                count = df.count()

                # Count dimension keys
                dim_keys = [col for col in df.columns if col.endswith("Key")]

                print(f"\n{table}:")
                print(f"  Total rows: {count:,}")
                print(f"  Dimension keys: {len(dim_keys)}")
                print(f"    {', '.join(sorted(dim_keys))}")

                # Sample validation - check a few dimension keys are populated
                sample = df.filter("TimeentrySK IS NOT NULL").limit(5).select(*dim_keys).collect()
                null_counts = {}
                for key in dim_keys:
                    null_count = df.filter(f"{key} IS NULL").count()
                    if null_count > 0:
                        null_counts[key] = null_count

                if null_counts:
                    print("  ⚠️ Missing dimension keys:")
                    for key, count in null_counts.items():
                        print(f"    {key}: {count:,} nulls")
                else:
                    print("  ✅ All dimension keys populated!")

            except Exception as e:
                print(f"\n{table}: Error - {e}")

    except Exception as e:
        print(f"Could not show fact summary: {e}")

print("\n🎯 Your fact tables now include:")
print("  • Member and Company dimension keys")
print("  • Rich status and billing categorizations")
print("  • Agreement type with Icelandic business logic")
print("  • All workflow state dimensions")
print("  • Proper CamelCase surrogate keys throughout!")
