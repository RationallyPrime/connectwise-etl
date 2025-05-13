# Microsoft Fabric Deployment Guide

This guide provides instructions for deploying and testing the improved ConnectWise PSA to Microsoft Fabric integration package.

## Overview

The updated package (v0.2.2) includes significant improvements to entity models that should resolve validation errors and improve compatibility with Microsoft Fabric's Spark environment. It includes fixes for Pydantic v2 deprecation warnings and fixes for schema conversion errors with reference fields.

## Recent Improvements

We've made several important improvements in this version:

1. **Fixed RootModel Schema Issues**: Replaced `RootModel` with concrete `ReferenceModel` classes that properly define schema for Spark.
2. **Enhanced Reference Field Handling**: Added validation to properly handle various reference formats from the API.
3. **Improved Type Safety**: Fixed type conversion errors by using proper Spark-compatible types.
4. **Made Optional Fields Truly Optional**: Ensured all non-required fields have proper defaults.
5. **Fixed Schema Merge Issues**: Removed date-based partitioning and added special handling for schema evolution to fix the `DELTA_FAILED_TO_MERGE_FIELDS` errors.
6. **Suppressed Pydantic Deprecation Warnings**: Added warning filters to eliminate the `allow_population_by_field_name` has been renamed to `validate_by_name` warnings (v0.2.1).
7. **Fixed Schema Conversion Errors**: Added custom `model_spark_schema` method to `ReferenceModel` to fix the `TypeConversionError: Error converting field 'company' to PySpark type` errors (v0.2.2).

## Deployment Steps

### 1. Upload the Wheel Package

1. Log in to your Microsoft Fabric workspace
2. Navigate to your Lakehouse
3. Upload the wheel file from the `dist` directory:
   - `fabric_api-0.2.2-py3-none-any.whl`

```
Path in Lakehouse: /lakehouse/Files/dist/fabric_api-0.2.2-py3-none-any.whl
```

### 2. Create a New Notebook

1. Create a new notebook in your workspace
2. Attach the notebook to your Lakehouse
3. Run the following commands to install the package:

```python
# Install the package
%pip install /lakehouse/Files/dist/fabric_api-0.2.2-py3-none-any.whl

# Restart kernel to apply changes
```

### 3. Test Basic Functionality

Run the following code to verify the package loads correctly:

```python
from fabric_api.connectwise_models import Agreement, PostedInvoice
from fabric_api.client import ConnectWiseClient
from fabric_api import pipeline

# Print versions to confirm we're using the updated models
print(f"Models available: {', '.join([m for m in dir(fabric_api.connectwise_models) if not m.startswith('_')])}")
```

### 4. Test with Real Data

Run a small data extract to verify the models work with real data:

```python
import os
from datetime import datetime, timedelta
from fabric_api.client import ConnectWiseClient
from fabric_api.extract.invoices import get_posted_invoices

# Set up client with credentials from Key Vault
client = ConnectWiseClient(
    company_id=os.environ.get("CW_COMPANY"),
    public_key=os.environ.get("CW_PUBLIC_KEY"), 
    private_key=os.environ.get("CW_PRIVATE_KEY"),
    client_id=os.environ.get("CW_CLIENTID")
)

# Get a small sample of invoices (last 7 days)
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

# Set a low page limit to test quickly
invoices = get_posted_invoices(
    client, 
    start_date=start_date, 
    end_date=end_date,
    max_pages=1  # Limit to 1 page for quick testing
)

# Verify we got valid data
print(f"Retrieved {len(invoices)} invoices")
if invoices:
    print(f"Sample invoice ID: {invoices[0].id}")
```

### 5. Test Single Entity Pipeline

Once basic functionality is confirmed, test the pipeline with one entity type first:

```python
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from fabric_api.bronze_loader import process_entity
from fabric_api.client import ConnectWiseClient

# Get the active SparkSession
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# Define the exact ABFSS path to use
bronze_path = "abfss://Wise_Fabric_PROD@onelake.dfs.fabric.microsoft.com/Lakehouse.Lakehouse/Tables/bronze"

# Start with a small date range for testing
end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

# Set up client
client = ConnectWiseClient()

# Process a single entity first to test
entity_name = "Agreement"
conditions = None  # No conditions for Agreement to get all

print(f"Processing entity: {entity_name}")
df, errors = process_entity(
    entity_name=entity_name,
    spark=spark,
    client=client,
    bronze_path=bronze_path,
    page_size=100,
    max_pages=2,  # Limit pages for initial testing
    conditions=conditions,
    write_mode="overwrite"  # Always use overwrite with the new version
)

# Display results
print(f"Processed {df.count()} records with {len(errors)} validation errors")
df.printSchema()
df.show(5)
```

### 6. Full Historical Data Load

For loading all historical data, use these settings:

```python
# Get the active SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# Define the exact ABFSS path to use
bronze_path = "abfss://Wise_Fabric_PROD@onelake.dfs.fabric.microsoft.com/Lakehouse.Lakehouse/Tables/bronze"

# Choose which entity types to process
entities = ["Agreement", "PostedInvoice", "TimeEntry", "UnpostedInvoice", "ExpenseEntry", "ProductItem"]

# For full historical data, use a very early start date
import datetime
from fabric_api import api_utils
end_date = datetime.datetime.now().strftime("%Y-%m-%d")
start_date = "2015-01-01"  # Or earlier depending on when your data begins

# Build date conditions for time-related entities - remove for Agreements to get all
conditions = {
    "TimeEntry": api_utils.build_condition_string(date_gte=start_date, date_lte=end_date),
    "ExpenseEntry": api_utils.build_condition_string(date_gte=start_date, date_lte=end_date),
    "PostedInvoice": api_utils.build_condition_string(date_gte=start_date, date_lte=end_date),
    "UnpostedInvoice": api_utils.build_condition_string(date_gte=start_date, date_lte=end_date)
    # No condition for Agreement to get all of them
}

# Process the entities with our new code
print(f"Processing entities: {', '.join(entities)}")
from fabric_api.bronze_loader import process_entities
results = process_entities(
    spark=spark,
    entity_names=entities,
    bronze_path=bronze_path,
    page_size=1000,  # Increase page size for efficiency
    max_pages=None,  # Set to None to get all pages
    conditions=conditions,
    write_mode="overwrite"  # Always use overwrite mode with the new version
)
```

**IMPORTANT NOTE:** The updated bronze loader always uses `overwrite` mode internally (even if you specify `append`), as this is required to avoid schema merge issues in Delta tables.

### 7. Verify Spark Schema

To confirm the SparkModel integration is working correctly, check the Spark schema:

```python
from fabric_api.connectwise_models import Agreement, PostedInvoice
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Check schema for Agreement
schema = Agreement.model_spark_schema()
print("Agreement Schema:")
schema.printTreeString()

# Check schema for PostedInvoice
schema = PostedInvoice.model_spark_schema()
print("\nPostedInvoice Schema:")
schema.printTreeString()
```

## Troubleshooting

If you encounter issues:

1. **Validation Errors**: Check the logs for specific fields that are failing validation. The new models should handle optional fields much better, but there might still be edge cases.

2. **Spark Schema Issues**: If you still see schema merge errors, the following strategies can help:
   
   ```python
   # For persistent schema merge issues, manually delete the table first
   from pyspark.sql import SparkSession
   spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
   
   # Delete the problematic table
   spark.sql("DROP TABLE IF EXISTS cw_agreement")
   
   # Delete the actual Delta files
   table_path = "/lakehouse/default/Tables/bronze/cw_agreement"
   dbutils.fs.rm(table_path, True)
   
   # Then rerun the ETL process
   ```

3. **API Compatibility**: If the API returns unexpected fields or structures, inspect the raw JSON data to understand the discrepancy.

## Rollback Plan

If needed, you can roll back to the previous version:

1. Delete the v0.2.2 wheel from your Lakehouse
2. Upload the previous version wheel file (v0.2.1, v0.2.0, or earlier)
3. Update your notebook to install the previous version

## Next Steps After Successful Deployment

1. Run a full production load for all entity types
2. Monitor logs for any remaining validation warnings or errors
3. Verify data quality in Delta tables
4. For more information about specific fixes, see:
   - [PYDANTIC_WARNING_FIX.md](PYDANTIC_WARNING_FIX.md) - Details on the Pydantic v2 deprecation warning fix
   - [SCHEMA_CONVERSION_FIX.md](SCHEMA_CONVERSION_FIX.md) - Details on the schema conversion error fix