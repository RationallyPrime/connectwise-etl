#!/usr/bin/env python
# coding: utf-8

# ## Notebook 2(1)
# 
# New notebook

# In[ ]:


# ---- CELL 1: Install the package ----
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install /lakehouse/default/Files/fabric_api-0.2.2-py3-none-any.whl
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install sparkdantic


# In[2]:


# ---- CELL 2: Configure environment and logging ----
import os
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

os.environ["CW_AUTH_USERNAME"] = "thekking+yemGyHDPdJ1hpuqx"
os.environ["CW_AUTH_PASSWORD"] = "yMqpe26Jcu55FbQk"  
os.environ["CW_CLIENTID"] = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"


# In[ ]:


# ---- CELL 3: Initialize PySpark session ----
from pyspark.sql import SparkSession

# Get active Spark session in Fabric
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# Display Spark version for verification
print(f"Using Spark version: {spark.version}")


# In[ ]:


# ---- CELL 4: Test imports and client connection ----
from fabric_api.client import ConnectWiseClient
from fabric_api.connectwise_models import (
    Agreement,
    TimeEntry,
    ExpenseEntry,
    Invoice,
    UnpostedInvoice,
    ProductItem
)

# Test client connection
client = ConnectWiseClient()

# Try a simple API call to check connectivity
response = client.get("/system/info")
print(f"API connection status: {response.status_code}")
print("API response:")
import json
print(json.dumps(response.json(), indent=2))


# In[ ]:


# ---- CELL 6: Option 2 - Process specific entities ----
from fabric_api.pipeline import process_entity
from datetime import datetime, timedelta

# Define parameters
entity_name = "TimeEntry"  # Choose from: Agreement, TimeEntry, ExpenseEntry, Invoice,
ProductItem
start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
end_date = datetime.now().strftime("%Y-%m-%d")
conditions = f"lastUpdated>=[{start_date}] AND lastUpdated<[{end_date}]"

# Use direct lakehouse folder path (not abfss:// URL)
lakehouse_root = "Tables/test"  # Let Fabric determine the full path

print(f"Processing {entity_name} from {start_date} to {end_date}...")

# Process the entity
result = process_entity(
    entity_name=entity_name,
    base_path=lakehouse_root,
    conditions=conditions,
    page_size=100,
    max_pages=1,  # Limit for testing
    mode="append"  # Use 'overwrite' for testing, 'append' for production
)

# Show results
print(f"Results for {entity_name}:")
print(f"- Extracted: {result['extracted']} records")
print(f"- Validated: {result['validated']} records")
print(f"- Errors: {result['errors']}")
print(f"- Rows written: {result['rows_written']}")
print(f"- Output path: {result['path']}")


# In[ ]:


# ---- CELL 7: Option 3 - Low-level API using extract functions ----
from fabric_api.extract.time import fetch_time_entries_raw
from fabric_api.connectwise_models import TimeEntry
from fabric_api.core.api_utils import build_condition_string

# Fetch raw time entries (using client we already created)
print("Fetching raw time entries...")
# Use the correct date field for time entries
conditions = f"dateStart>=[{start_date}] AND dateStart<[{end_date}]"

raw_entries = fetch_time_entries_raw(
    client=client,
    page_size=1000,
    max_pages=1,  # Limit for testing
    conditions=conditions
)

print(f"Fetched {len(raw_entries)} raw time entries")

# Validate using models
valid_entries = []
validation_errors = []

for entry in raw_entries:
    try:
        # Validate using Pydantic model
        validated = TimeEntry.model_validate(entry)
        valid_entries.append(validated)
    except Exception as e:
        validation_errors.append({"id": entry.get("id"), "error": str(e)})

print(f"Validated {len(valid_entries)} entries with {len(validation_errors)} errors")

# Display first entry if available
if valid_entries:
    print("\nSample validated entry:")
    print(valid_entries[0].model_dump_json(indent=2))

