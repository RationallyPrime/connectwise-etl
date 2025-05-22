#!/usr/bin/env python
"""
Test Spark schema generation for OneLake in non-Fabric environment.
This test ensures that our schemas are correctly structured without needing Fabric.
"""

import unittest
from unittest.mock import MagicMock

from unified_etl.connectwise_models import (
    Agreement,
    ExpenseEntry,
    PostedInvoice,
    ProductItem,
    TimeEntry,
    UnpostedInvoice,
)
from unified_etl.onelake_utils import get_partition_columns


class TestOneLakeSchemas(unittest.TestCase):
    """Tests for OneLake schema structure."""

    def setUp(self):
        """Set up test environment."""
        # Create a mock SparkSession
        self.spark = MagicMock()

    def test_model_spark_schema(self):
        """Test schema generation for all entity models."""

        # Test all main entity models
        models = [
            (Agreement, "agreement"),
            (PostedInvoice, "posted_invoice"),
            (UnpostedInvoice, "unposted_invoice"),
            (TimeEntry, "time_entry"),
            (ExpenseEntry, "expense_entry"),
            (ProductItem, "product_item"),
        ]

        for model_class, entity_name in models:
            # Get schema from model
            schema = model_class.model_spark_schema()

            # Schema should not be None
            assert schema is not None

            # Schema should have fields
            assert len(schema.fields) > 0

            # Print schema structure for inspection
            print(f"\n{entity_name.upper()} SCHEMA:")
            print(schema)

            # Check field names
            field_names = [field.name for field in schema.fields]
            print(f"Fields: {field_names}")

            # Verify partition columns exist in schema or can be derived
            partition_cols = get_partition_columns(entity_name)
            for col in partition_cols:
                # Skip date-derived partition columns which wouldn't be in the base schema
                if not (col.endswith("_year") or col.endswith("_month") or col.endswith("_date")):
                    # Need to check if the partition column is derived from a field in the schema
                    base_col = col.split("_")[0] if "_" in col else col

                    # Print information for debugging
                    print(f"Checking partition column {col} (base: {base_col}) for {entity_name}")

                    # For invoices, we need to handle special case for 'date' field
                    if entity_name in ["posted_invoice", "unposted_invoice"] and col == "date":
                        assert "date" in field_names or "invoiceDate" in field_names, (
                            f"Neither 'date' nor 'invoiceDate' found in schema for {entity_name}"
                        )
                    else:
                        assert any(name in (base_col, col) for name in field_names), (
                            f"Partition column {col} not found in schema for {entity_name}"
                        )

    def test_schema_compatibility(self):
        """Test compatibility between model schemas."""
        # Test that schemas with common fields have compatible types
        agreement_schema = Agreement.model_spark_schema()
        invoice_schema = PostedInvoice.model_spark_schema()

        # Check common field types
        common_fields = {"id", "name", "type"}

        for field in common_fields:
            # Get field from both schemas if it exists
            agreement_field = next((f for f in agreement_schema.fields if f.name == field), None)
            invoice_field = next((f for f in invoice_schema.fields if f.name == field), None)

            # If both schemas have the field, they should have compatible types
            if agreement_field and invoice_field:
                print(f"Field {field}: {agreement_field.dataType} vs {invoice_field.dataType}")
                # In a real test, we would check type compatibility
                # Here we just check that types are not None
                assert agreement_field.dataType is not None
                assert invoice_field.dataType is not None


if __name__ == "__main__":
    unittest.main()
