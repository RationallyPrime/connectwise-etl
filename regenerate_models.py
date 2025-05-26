#!/usr/bin/env python
"""Regenerate PSA models with all required entities."""

from unified_etl_core.generators.generate_psa_models import generate_psa_models

if __name__ == "__main__":
    # Generate models for all required entities
    success = generate_psa_models(
        openapi_schema_path_str="/home/hakonf/psa-unified-clean/PSA_OpenAPI_schema.json",
        output_dir_str="/home/hakonf/psa-unified-clean/packages/unified-etl-connectwise/src/unified_etl_connectwise/models/",
        entities_to_include=[
            "Agreement",
            "TimeEntry", 
            "ExpenseEntry",
            "ProductItem",
            "Invoice",  # For both posted and unposted
        ],
        reference_models_to_include=[
            "AgreementReference",
            "CompanyReference",
            "MemberReference",
            "WorkTypeReference",
            "ProjectReference",
            "ExpenseTypeReference",
            "ProductReference",
        ],
    )
    
    if success:
        print("✅ Model regeneration succeeded!")
        print("   Included: Agreement, TimeEntry, ExpenseEntry, ProductItem, Invoice")
    else:
        print("❌ Model regeneration failed!")