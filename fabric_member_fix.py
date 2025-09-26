"""
Fabric notebook cell to extract Member data without field selection.
"""

# %%
# Cell: Extract Member data without specifying fields
from connectwise_etl.client import ConnectWiseClient
from connectwise_etl.models import Member
from connectwise_etl.models.registry import models
import logging

# Set logging to see what's happening
logging.basicConfig(level=logging.INFO)

# Rebuild model to resolve forward references
Member.model_rebuild()

# Initialize client (uses env vars from Key Vault)
client = ConnectWiseClient()

# Extract Member data WITHOUT field selection (let API return defaults)
print("Starting Member extraction without field selection...")

# Call paginate directly without fields parameter
raw_data = client.paginate(
    endpoint="/system/members",
    entity_name="Member",
    fields=None,  # Don't specify fields - use API defaults
    page_size=100,
    max_pages=None
)

print(f"Retrieved {len(raw_data)} members from API")

# Validate and create DataFrame
if raw_data:
    # Create Spark DataFrame from raw data
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(raw_data)

    # Write to Bronze layer
    table_name = "bronze.bronze_cw_member"
    print(f"Writing {df.count()} members to {table_name}")

    df.write.mode("overwrite").saveAsTable(table_name)
    print(f"✅ Successfully wrote Member data to {table_name}")

    # Show sample
    spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").show()
    spark.sql(f"SELECT id, identifier, firstName, lastName FROM {table_name} LIMIT 5").show()
else:
    print("❌ No data retrieved from Member endpoint")