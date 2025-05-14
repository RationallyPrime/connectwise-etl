# ConnectWise to Microsoft Fabric ETL Example

# %%
# Import required libraries
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

# Get the active Spark session (provided by the Fabric notebook environment)
spark = SparkSession.getActiveSession()
print(f"Using Spark {spark.version}")

# %%
# Import the main components from fabric_api
# %%
# Configure logging
import logging

from fabric_api import (
    # Client
    ConnectWiseClient,
    # Extract
    extract_entity,
    # Transform
    flatten_all_nested_structures,
    run_daily_etl,
    # Complete pipeline
    run_incremental_etl,
    # Storage
    write_to_delta,
)

logging.basicConfig(level=logging.INFO)

# %%
# Option 1: Run incremental ETL for the last 7 days
# This is the simplest approach for regular data loads

results = run_incremental_etl(
    lookback_days=7,  # Last 7 days
    entity_names=["TimeEntry", "ExpenseEntry", "PostedInvoice"],
    max_pages=20  # Limit for this example
)

# Show results
for entity_name, (df, rows, errors) in results.items():
    print(f"{entity_name}: {rows} rows loaded, {errors} validation errors")
    # Show sample data
    if not df.isEmpty():
        display(df.limit(5))

# %%
# Option 2: Manual pipeline - demonstrate more control
# Create ConnectWise client
client = ConnectWiseClient()

# Calculate date range for the last 30 days
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

# Format dates for API
date_condition = f"lastUpdated>=[{start_date.strftime('%Y-%m-%d')}] AND lastUpdated<=[{end_date.strftime('%Y-%m-%d')}]"

# Extract TimeEntry data with validation
print("Extracting TimeEntry data...")
entries, errors = extract_entity(
    client=client,
    entity_name="TimeEntry",
    conditions=date_condition,
    max_pages=10,
    return_validated=True
)

# Create DataFrame from validated models
if entries:
    from fabric_api import dataframe_from_models

    print(f"Creating DataFrame from {len(entries)} validated TimeEntry records")
    df = dataframe_from_models(spark, entries, "TimeEntry")

    # Flatten any nested structures
    print("Flattening nested structures...")
    flat_df = flatten_all_nested_structures(df)

    # Write to Delta table
    print("Writing to Delta table...")
    path, row_count = write_to_delta(
        df=flat_df,
        entity_name="TimeEntry",
        mode="append"
    )

    print(f"Successfully wrote {row_count} rows to {path}")

    # Show sample data
    display(flat_df.limit(5))
else:
    print(f"No valid TimeEntry records found. {len(errors)} validation errors.")

# %%
# Run daily ETL (yesterday's data only)
print("Running daily ETL for yesterday's data...")
daily_results = run_daily_etl(
    entity_names=["Agreement", "TimeEntry", "ExpenseEntry"],
    days_back=1  # Yesterday only
)

# Show results
for entity_name, (df, rows, errors) in daily_results.items():
    print(f"{entity_name}: {rows} rows loaded, {errors} validation errors")

# %%
# Query the data using Spark SQL
print("Running SQL query on the data...")
sql_query = """
SELECT
    id,
    timeStart,
    timeEnd,
    hours,
    etl_timestamp,
    etl_entity
FROM
    cw_time_entry
WHERE
    hours > 1
LIMIT 10
"""

result_df = spark.sql(sql_query)
display(result_df)
