#!/usr/bin/env python
"""
Test script to simulate the ETL process in a simplified way.
"""

import os
import tempfile

from pyspark.sql import SparkSession

# Import from our fixed package
from unified_etl.connectwise_models import Agreement

print("=== Testing ETL Process with Fixed Models ===")

# Create a SparkSession
print("Initializing SparkSession...")
spark = SparkSession.builder.appName("TestETL").master("local[*]").getOrCreate()

# Sample data for testing
sample_agreements = [
    {
        "id": 1,
        "name": "Support Agreement 1",
        "type": {"id": 1, "name": "Support"},
        "company": {"id": 101, "name": "Acme Inc"},
        "contact": {"id": 201, "name": "John Doe"},
    },
    {
        "id": 2,
        "name": "Maintenance Agreement 2",
        "type": {"id": 2, "name": "Maintenance"},
        "company": {"id": 102, "name": "Globex Corp"},
        "contact": {"id": 202, "name": "Jane Smith"},
    },
]

# Validate and convert the data
print("\nValidating Agreement data...")
validated_agreements = []
for item in sample_agreements:
    try:
        model = Agreement(**item)
        validated_agreements.append(model)
        print(f"✅ Validated: {model.name}")
    except Exception as e:
        print(f"❌ Validation failed for {item['name']}: {e!s}")

# Attempt to create a schema
print("\nGenerating Spark schema from model...")
try:
    schema = Agreement.model_spark_schema()
    print(f"✅ Generated schema with {len(schema.fields)} fields")
except Exception as e:
    print(f"❌ Schema generation failed: {e!s}")

# Convert models to dictionaries for DataFrame creation
print("\nConverting models to dictionaries...")
dict_data = [model.model_dump(by_alias=True) for model in validated_agreements]

# Create a DataFrame
print("\nCreating DataFrame...")
try:
    # Use the schema explicitly
    df = spark.createDataFrame(dict_data, schema)
    print(f"✅ Created DataFrame with {df.count()} rows")

    # Show schema structure
    print("\nDataFrame schema:")
    df.printSchema()

    # Create a temporary directory for testing Delta write
    with tempfile.TemporaryDirectory() as tmpdir:
        delta_path = os.path.join(tmpdir, "test_delta")

        print(f"\nWriting to Delta table at {delta_path}...")
        try:
            # Use Delta format for writing
            df.write.format("delta").mode("overwrite").save(delta_path)
            print("✅ Successfully wrote to Delta table")

            # Read it back
            df_read = spark.read.format("delta").load(delta_path)
            print(f"✅ Successfully read back {df_read.count()} rows from Delta table")
        except Exception as e:
            print(f"❌ Delta write/read failed: {e!s}")

except Exception as e:
    print(f"❌ DataFrame creation failed: {e!s}")

# Stop SparkSession
spark.stop()
print("\nETL process test completed!")
