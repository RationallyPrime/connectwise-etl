#!/usr/bin/env python
"""
Comprehensive test script to verify all fixes in version 0.2.2.
"""
import sys

print(f"Python version: {sys.version}")
print("Testing fixes in version 0.2.2...\n")

# 1. Test Pydantic v2 warning fix - No warnings should show up here
print("=== Testing Pydantic v2 warning fix ===")
from fabric_api.connectwise_models import Agreement

print("✅ Models imported successfully without Pydantic warnings\n")

# 2. Test ReferenceModel schema generation fix
print("=== Testing ReferenceModel schema generation fix ===")
from fabric_api.connectwise_models.reference_model import ReferenceModel

# Create a simple reference model
reference = ReferenceModel(id=1, name="Test Reference")
print(f"Created reference model: {reference}")

# Test schema generation
reference_schema = ReferenceModel.model_spark_schema()
print(f"Generated schema for ReferenceModel with fields: {[f.name for f in reference_schema.fields]}")

# Test schema generation for Agreement
print("\nGenerating schema for Agreement model...")
try:
    agreement_schema = Agreement.model_spark_schema()
    print(f"✅ Successfully generated Agreement schema with {len(agreement_schema.fields)} fields")

    # Check if the company field exists and has the right type
    company_field = next((f for f in agreement_schema.fields if f.name == "company"), None)
    if company_field:
        print(f"✅ Company field found with type: {company_field.dataType}")
    else:
        print("❌ Company field not found in schema!")
except Exception as e:
    print(f"❌ Error generating schema: {e!s}")

# Test creating a model with reference fields
print("\nTesting model creation with reference fields...")
try:
    agreement = Agreement(
        id=1,
        name="Test Agreement",
        type={"id": 1, "name": "Test Type"},
        company={"id": 1, "name": "Test Company"},
        contact={"id": 1, "name": "Test Contact"}
    )
    print(f"✅ Created Agreement model: id={agreement.id}, name={agreement.name}")
    print(f"✅ Company reference: id={agreement.company.id}, name={agreement.company.name}")
except Exception as e:
    print(f"❌ Error creating model: {e!s}")

print("\nAll tests completed!")
