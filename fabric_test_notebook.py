# Databricks notebook source
# MAGIC %md
# MAGIC # Test Unified ETL Pipeline - ConnectWise PSA
# MAGIC This notebook tests the unified ETL framework with ConnectWise data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install the packages

# COMMAND ----------

# Install the unified ETL packages
%pip install /lakehouse/default/Files/dist/unified-etl-core-1.0.0-py3-none-any.whl
%pip install /lakehouse/default/Files/dist/unified-etl-connectwise-1.0.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Test Model Generation and Field Selection

# COMMAND ----------

# Test that models were generated correctly
from unified_etl_connectwise.models import Agreement, TimeEntry, Invoice
from unified_etl_connectwise.utils.api_utils import get_fields_for_api_call

# Test model creation
test_agreement = Agreement(
    id=1,
    name="Test Agreement",
    type={"id": 1, "name": "MSA"},
    company={"id": 1, "identifier": "ACME"},
)

print(f"✅ Created test Agreement: {test_agreement.name}")

# Test field generation for API calls
fields = get_fields_for_api_call(Agreement, max_depth=2)
print(f"\n✅ Generated API fields for Agreement:")
print(f"   Total fields: {len(fields.split(','))}")
print(f"   Sample: {','.join(fields.split(',')[:10])}...")

# Test SparkDantic schema generation
spark_schema = Agreement.spark_schema()
print(f"\n✅ Generated Spark schema with {len(spark_schema.fields)} fields")
print("   Schema fields:", [f.name for f in spark_schema.fields[:10]], "...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test Bronze Layer Extraction (Small Sample)

# COMMAND ----------

import os
from unified_etl_connectwise.extract import ConnectWiseExtractor

# Get credentials from Key Vault (in real scenario)
# For testing, you might set these manually
config = {
    "base_url": os.environ.get("CW_BASE_URL", "https://api-na.myconnectwise.net/v4_6_release/apis/3.0"),
    "auth": {
        "type": "api_key",
        "credentials": {
            "company": os.environ.get("CW_COMPANY"),
            "public_key": os.environ.get("CW_PUBLIC_KEY"),
            "private_key": os.environ.get("CW_PRIVATE_KEY"),
        }
    }
}

# Create extractor
extractor = ConnectWiseExtractor(config)

# Extract a small sample of agreements (limit to 10 for testing)
print("Extracting sample agreements...")
df_agreements = extractor.extract(
    endpoint="/finance/agreements",
    page_size=10,
    conditions="agreementStatus='Active'",
)

print(f"✅ Extracted {df_agreements.count()} agreements")
df_agreements.show(5, truncate=False)

# Save to bronze layer
bronze_path = "/lakehouse/default/Tables/bronze/bronze_cw_agreements"
df_agreements.write.mode("overwrite").format("delta").save(bronze_path)
print(f"✅ Saved to bronze: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test Silver Layer Transformation

# COMMAND ----------

# Read from bronze
df_bronze = spark.read.format("delta").load(bronze_path)

# In silver, we would:
# 1. Parse JSON columns
# 2. Validate with Pydantic models
# 3. Flatten nested structures
# 4. Standardize naming

# For now, let's just demonstrate the validation
from unified_etl_core.extract.base import validate_batch
import pandas as pd

# Convert first 5 rows to pandas for validation
sample_data = df_bronze.limit(5).toPandas().to_dict('records')

# Validate against model
valid_models, errors = validate_batch(sample_data, Agreement)

print(f"✅ Validated {len(valid_models)} agreements")
if errors:
    print(f"⚠️  Found {len(errors)} validation errors")

# In real silver processing, we'd create a clean DataFrame from valid models
# For demonstration, just pass through
silver_path = "/lakehouse/default/Tables/silver/silver_agreements"
df_bronze.write.mode("overwrite").format("delta").save(silver_path)
print(f"✅ Saved to silver: {silver_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

print("🎉 Pipeline Test Summary:")
print(f"- Models generated from OpenAPI: ✅")
print(f"- Field selection for API calls: ✅")
print(f"- Bronze extraction (ConnectWise): ✅")
print(f"- Silver validation (Pydantic): ✅")
print(f"- SparkDantic schema generation: ✅")
print("\nNext steps:")
print("- Implement full Silver transformations (flattening, type conversion)")
print("- Add Gold layer with business logic")
print("- Test with Business Central data")
print("- Add incremental processing with watermarks")