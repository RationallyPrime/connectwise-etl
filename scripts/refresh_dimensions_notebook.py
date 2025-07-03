# Cell 1: Full Gold Layer Processing (Dimensions + Facts)
# Run this in a Fabric notebook after installing the wheels

import logging
from unified_etl_core.main import run_etl_pipeline

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

print("Starting full gold layer processing (dimensions + facts)...")

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

# This will:
# 1. Create all dimensions from silver tables first
# 2. Create fact tables using the dimensions
# 3. Create any additional dimensions from calculated columns
run_etl_pipeline(
    integrations=["connectwise"],
    layers=["gold"],
    mode="full",
    config={"entities": entity_configs}
)

print("\n✅ Dimension refresh complete!")

# Show summary
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
if spark:
    try:
        dim_tables = spark.sql("SHOW TABLES FROM Lakehouse.gold").filter("tableName LIKE 'dim_%'").collect()
        print(f"\nCreated {len(dim_tables)} dimension tables:")
        for table in dim_tables:
            count = spark.table(f"Lakehouse.gold.{table.tableName}").count()
            print(f"  {table.tableName}: {count} rows")
    except Exception as e:
        print(f"Could not show dimension summary: {e}")