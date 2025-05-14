#!/usr/bin/env python
"""
Example notebook code for running the bronze-to-silver pipeline in Microsoft Fabric.

This example demonstrates:
1. Setting up the environment
2. Running the bronze extraction
3. Transforming bronze to silver
4. Running the full pipeline
"""

# %% [markdown]
# # ConnectWise Bronze-to-Silver Pipeline Example
# 
# This notebook demonstrates how to use the enhanced pipeline that implements
# a proper bronze-to-silver data flow.

# %% Cell 1: Install the package
# %pip install /lakehouse/default/Files/fabric_api-0.1.0-py3-none-any.whl

# %% Cell 2: Import and setup
from fabric_api import (
    ConnectWiseClient,
    run_full_pipeline,
    run_daily_pipeline,
    process_entity_to_bronze,
    process_bronze_to_silver
)
from fabric_api.core import configure_logging

# Set up logging
configure_logging(
    level="INFO",
    file_path="/lakehouse/default/Files/logs/bronze_silver_etl.log"
)

# Create client
client = ConnectWiseClient()

# %% Cell 3: Run bronze extraction only
# Extract raw data to bronze layer
bronze_results = {}
entities = ["Agreement", "TimeEntry", "ExpenseEntry"]

for entity in entities:
    print(f"Extracting {entity} to bronze...")
    result = process_entity_to_bronze(
        entity_name=entity,
        client=client,
        max_pages=5,  # Limit for testing
        bronze_path="/lakehouse/default/Tables/bronze",
        mode="overwrite"
    )
    bronze_results[entity] = result
    print(f"  - Extracted {result['bronze_rows']} rows")

# %% Cell 4: Transform bronze to silver
# Transform each entity from bronze to silver
silver_results = {}

for entity in entities:
    if bronze_results[entity]["bronze_rows"] > 0:
        print(f"Transforming {entity} from bronze to silver...")
        result = process_bronze_to_silver(
            entity_name=entity,
            bronze_path="/lakehouse/default/Tables/bronze",
            silver_path="/lakehouse/default/Tables/silver"
        )
        silver_results[entity] = result
        print(f"  - Transformed {result['silver_rows']} rows")
    else:
        print(f"Skipping {entity} - no data in bronze")

# %% Cell 5: Run full pipeline (bronze + silver)
# Run the complete pipeline for all entities
results = run_full_pipeline(
    entity_names=["Agreement", "TimeEntry", "ExpenseEntry", "PostedInvoice"],
    client=client,
    max_pages=10,
    bronze_path="/lakehouse/default/Tables/bronze",
    silver_path="/lakehouse/default/Tables/silver",
    mode="overwrite"
)

# Display results
for entity, result in results.items():
    print(f"\n{entity}:")
    print(f"  Bronze: {result['bronze']['bronze_rows']} rows")
    print(f"  Silver: {result['silver']['silver_rows']} rows")

# %% Cell 6: Run daily incremental load
# Run daily pipeline for incremental loads
daily_results = run_daily_pipeline(
    entity_names=["TimeEntry", "ExpenseEntry"],  # Just time and expenses for daily
    client=client,
    bronze_path="/lakehouse/default/Tables/bronze",
    silver_path="/lakehouse/default/Tables/silver"
)

# %% Cell 7: Query the silver layer
# Example queries against the silver layer
spark.sql("""
SELECT 
    id,
    companyId,
    company_name,
    agreementStatus,
    billAmount,
    startDate,
    endDate
FROM silver.agreement
WHERE agreementStatus = 'Active'
LIMIT 10
""").show()

# Show time entries with flattened fields
spark.sql("""
SELECT 
    id,
    memberId,
    member_name,
    companyId, 
    company_name,
    actualHours,
    billableOption,
    DATE(timeStart) as workDate
FROM silver.timeentry
WHERE billableOption = 'Billable'
ORDER BY timeStart DESC
LIMIT 20
""").show()

# %% [markdown]
# ## Pipeline Configuration
# 
# The pipeline uses column pruning configurations defined in `COLUMN_PRUNE_CONFIG`
# to keep only necessary columns for each entity. This reduces storage and improves
# query performance in the silver layer.
# 
# ### Key Features:
# 1. **Bronze Layer**: Raw data exactly as received from the API
# 2. **Silver Layer**: Cleaned, flattened, and pruned data ready for analytics
# 3. **Column Pruning**: Only keeps business-relevant columns
# 4. **Field Flattening**: Nested structures are flattened (e.g., company.id -> company_id)
# 5. **Future Enhancement**: Invoice splitting into header/line tables

# %% Cell 8: Monitor pipeline performance
# Check row counts and processing times using Spark
from pyspark.sql import Row

# Create a summary using Spark
summary_rows = []
for entity, result in results.items():
    summary_rows.append(Row(
        Entity=entity,
        BronzeRows=result['bronze']['bronze_rows'],
        SilverRows=result['silver'].get('silver_rows', 0),
        Status=result['silver'].get('status', 'N/A')
    ))

summary_df = spark.createDataFrame(summary_rows)
summary_df.show()

# Save summary to a table for tracking
summary_df.write.mode("overwrite").saveAsTable("pipeline_summary")