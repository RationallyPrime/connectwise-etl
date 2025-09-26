"""
Fabric notebook to run Member extraction only.
Upload this as a .py file to Fabric and it will be treated as a notebook.
"""

# %%
# Cell 1: Install wheel if needed
# %pip install /lakehouse/default/Files/dist/connectwise_etl-1.0.0-py3-none-any.whl --force-reinstall

# %%
# Cell 2: Extract Member data only
from connectwise_etl.client import ConnectWiseClient
from connectwise_etl.models import Member
from connectwise_etl.models.registry import models

# Rebuild model to resolve forward references
Member.model_rebuild()

# Initialize client (uses env vars from Key Vault)
client = ConnectWiseClient()

# Extract Member data
print("Starting Member extraction...")
df = client.extract(
    endpoint="/system/members",
    model_class=Member,
    page_size=100,
    max_pages=None  # Get all
)

# Write to Bronze layer
table_name = "bronze.bronze_cw_member"
print(f"Writing {df.count()} members to {table_name}")

df.write.mode("overwrite").saveAsTable(table_name)
print(f"✅ Successfully wrote Member data to {table_name}")

# %%
# Cell 3: Verify the data
spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").show()
spark.sql(f"SELECT id, identifier, name FROM {table_name} LIMIT 10").show()