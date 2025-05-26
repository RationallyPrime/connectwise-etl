#!/usr/bin/env python
"""Test model generation from PSA OpenAPI schema."""

from unified_etl_core.generators.generate_psa_models import generate_psa_models

if __name__ == "__main__":
    success = generate_psa_models(
        openapi_schema_path_str="/home/hakonf/psa-unified-clean/PSA_OpenAPI_schema.json",
        output_dir_str="/home/hakonf/psa-unified-clean/packages/unified-etl-connectwise/src/unified_etl_connectwise/models/",
        entities_to_include=["Agreement", "TimeEntry", "Invoice"],
        reference_models_to_include=["AgreementReference", "CompanyReference"],
    )
    
    if success:
        print("✅ Model generation succeeded!")
    else:
        print("❌ Model generation failed!")