#!/usr/bin/env python
"""
Test script to verify the ReferenceModel schema fix works.
"""
import sys

from pyspark.sql import SparkSession

print(f"Python version: {sys.version}")
print("Testing schema conversion for ReferenceModel fix...")

# Import our models
from fabric_api.connectwise_models import Agreement
from fabric_api.connectwise_models.reference_model import ReferenceModel

print("✅ Imports successful")

# Test schema generation for ReferenceModel
print("\nTesting ReferenceModel schema generation:")
reference_schema = ReferenceModel.model_spark_schema()
print(f"Reference schema fields: {[f.name for f in reference_schema.fields]}")

# Test schema generation for Agreement which has nested ReferenceModel fields
print("\nTesting Agreement schema generation:")
try:
    agreement_schema = Agreement.model_spark_schema()
    print(f"Successfully generated Agreement schema with {len(agreement_schema.fields)} fields")

    # Check if the company field exists in the schema
    company_field = next((f for f in agreement_schema.fields if f.name == "company"), None)
    if company_field:
        print(f"Company field type: {company_field.dataType}")
    else:
        print("Company field not found in schema!")

    # Try creating a DataFrame with the schema
    print("\nTesting DataFrame creation with schema:")
    spark = SparkSession.builder.getOrCreate()

    # Create a sample Agreement object
    agreement = Agreement(
        id=1,
        name="Test Agreement",
        type={"id": 1, "name": "Test Type"},
        company={"id": 1, "name": "Test Company"},
        contact={"id": 1, "name": "Test Contact"}
    )

    # Convert to dict and create DataFrame
    agreement_dict = agreement.model_dump(by_alias=True)
    df = spark.createDataFrame([agreement_dict], agreement_schema)

    print("Successfully created DataFrame with schema")
    print(f"DataFrame schema: {df.schema}")
    df.show(truncate=False)

except Exception as e:
    print(f"❌ Error generating schema: {e!s}")
    import traceback
    traceback.print_exc()

print("\nTest completed!")
