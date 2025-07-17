# Fabric Deployment Guide

## Built Wheels (2025-07-17)

- `unified_etl_core-1.0.0-py3-none-any.whl` (47.5 KB)
- `unified_etl_businesscentral-1.0.0-py3-none-any.whl` (71.8 KB)
- `unified_etl_connectwise-1.0.0-py3-none-any.whl` (29.4 KB)

## Upload to Fabric

1. Upload all three wheels to your Fabric environment
2. Install in this order:
   ```python
   %pip install unified_etl_core-1.0.0-py3-none-any.whl
   %pip install unified_etl_businesscentral-1.0.0-py3-none-any.whl
   %pip install unified_etl_connectwise-1.0.0-py3-none-any.whl
   ```

## Test Business Central Gold Layer

```python
from unified_etl_businesscentral.orchestrate import process_bc_gold_layer
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("BC_Gold_Test").getOrCreate()

# Process gold layer
gold_tables = process_bc_gold_layer(
    spark=spark,
    bronze_path="abfss://your-container@your-storage.dfs.core.windows.net/bronze",
    silver_path="abfss://your-container@your-storage.dfs.core.windows.net/silver", 
    gold_path="abfss://your-container@your-storage.dfs.core.windows.net/gold"
)

# Check results
print(f"Created {len(gold_tables)} gold tables:")
for table in gold_tables:
    print(f"  - {table}")
```

## Expected Gold Tables

### Dimensions (15)
- dim_Customer
- dim_Vendor  
- dim_Item
- dim_GLAccount (with hierarchy)
- dim_Currency
- dim_Company
- dim_Resource
- dim_Date
- dim_DueDate
- dim_GD1 through dim_GD8

### Facts (4)
- fact_Purchase
- fact_Agreement
- fact_SalesInvoiceLines
- fact_GLEntry

### Bridge Tables (1)
- dim_DimensionBridge

## Troubleshooting

If you encounter issues:
1. Verify bronze/silver data exists
2. Check Spark configuration
3. Review logs for specific errors
4. Ensure all dependencies are installed