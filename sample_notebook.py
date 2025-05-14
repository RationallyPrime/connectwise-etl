# Sample Notebook for ConnectWise to Fabric ETL
# %%
# Import the SparkSession from pyspark
from pyspark.sql import SparkSession

# Get the active Spark session from the notebook
spark = SparkSession.getActiveSession()
print(f"Using Spark version: {spark.version}")

# %%
# Import core ETL components
from fabric_api.pipeline_new import process_entity, run_incremental_etl

# %%
# Run ETL for a single entity
# This will fetch TimeEntry data from the last 7 days
result = process_entity(
    entity_name="TimeEntry",
    conditions="lastUpdated>=[2023-05-01] AND lastUpdated<[2023-05-08]",
    page_size=100,
    max_pages=10  # Limit for testing
)

print(f"Processed {result['entity']} data:")
print(f"- Extracted records: {result['extracted']}")
print(f"- Valid records: {result['validated']}")
print(f"- Validation errors: {result['errors']}")
print(f"- Rows written: {result['rows_written']}")
print(f"- Output path: {result['path']}")

# %%
# Run incremental ETL for multiple entities based on date range
results = run_incremental_etl(
    start_date="2023-05-01",
    end_date="2023-05-08",
    entity_names=["Agreement", "TimeEntry", "ExpenseEntry"],
    page_size=100,
    max_pages=10  # Limit for testing
)

# Print summary
print("ETL Results Summary:")
for entity, result in results.items():
    print(f"{entity}: {result['rows_written']} rows written, {result['errors']} validation errors")

# %%
# Query the data using Spark SQL
table_name = "cw_time_entry"
query = f"""
SELECT
    id,
    date,
    hours,
    company_name,
    member_identifier
FROM {table_name}
WHERE date >= '2023-05-01'
LIMIT 10
"""
df = spark.sql(query)
display(df)  # Display results in the notebook
