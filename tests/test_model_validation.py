#!/usr/bin/env python
"""Test model validation locally before Fabric deployment."""

import pytest
from pydantic import ValidationError
from datetime import datetime

# Mock the models for testing (in real scenario, import from generated models)
try:
    from unified_etl.connectwise_models import TimeEntry, Agreement, PostedInvoice
    from unified_etl.bc_models import Customer, GLEntry, Job
except ImportError:
    # Create mock models for testing
    from sparkdantic import SparkModel
    from pydantic import Field

    class TimeEntry(SparkModel):
        """Mock TimeEntry model for testing."""

        id: int | None = None
        memberId: int | None = None
        invoiceId: int | None = None
        agreementId: int | None = None
        actualHours: float | None = None
        hourlyRate: float | None = None
        workTypeId: int | None = None
        timeStart: datetime | None = None
        notes: str | None = None

    class Agreement(SparkModel):
        """Mock Agreement model for testing."""

        id: int | None = None
        name: str | None = None
        agreementNumber: str | None = None
        typeName: str | None = None
        companyId: int | None = None
        startDate: datetime | None = None
        endDate: datetime | None = None

    class Customer(SparkModel):
        """Mock Customer model for testing."""

        No: str | None = Field(default=None, alias="No-18")
        Name: str | None = Field(default=None, alias="Name-18")
        CustomerPostingGroup: str | None = None
        Company: str | None = Field(default=None, alias="$Company")

    class GLEntry(SparkModel):
        """Mock GLEntry model for testing."""

        EntryNo: int | None = Field(default=None, alias="EntryNo-17")
        PostingDate: datetime | None = None
        Amount: float | None = None
        DocumentNo: str | None = None
        GLAccountNo: str | None = None
        DimensionSetID: int | None = None


class TestPSAModels:
    """Test PSA model validation."""

    def test_time_entry_validation(self):
        """Test TimeEntry model validation."""
        # Valid data
        valid_time = TimeEntry(id=123, memberId=456, actualHours=8.5, hourlyRate=150.0)
        assert valid_time.id == 123
        assert valid_time.actualHours == 8.5

        # Calculate revenue
        if valid_time.actualHours and valid_time.hourlyRate:
            revenue = valid_time.actualHours * valid_time.hourlyRate
            assert revenue == 1275.0

    def test_time_entry_invalid_types(self):
        """Test TimeEntry with invalid types."""
        # This should raise validation error with strict typing
        with pytest.raises(ValidationError):
            TimeEntry(
                id="not_a_number",  # Should be int
                memberId=456,
            )

    def test_agreement_validation(self):
        """Test Agreement model validation."""
        valid_agreement = Agreement(
            id=789,
            name="Test Agreement",
            agreementNumber="AGR-2024-001",
            typeName="Managed Services",
            companyId=123,
        )
        assert valid_agreement.agreementNumber == "AGR-2024-001"
        assert valid_agreement.companyId == 123

    def test_optional_fields(self):
        """Test that all fields are optional."""
        # Should create with minimal data
        minimal_time = TimeEntry(id=1)
        assert minimal_time.id == 1
        assert minimal_time.memberId is None
        assert minimal_time.actualHours is None


class TestBCModels:
    """Test BC model validation."""

    def test_customer_validation(self):
        """Test Customer model validation."""
        valid_customer = Customer(
            No="CUST001", Name="Test Customer", CustomerPostingGroup="DOMESTIC", Company="CRONUS"
        )
        assert valid_customer.No == "CUST001"
        assert valid_customer.Company == "CRONUS"

    def test_customer_aliases(self):
        """Test Customer field aliases from CDM."""
        # Test that aliases work
        customer_fields = Customer.model_fields
        assert customer_fields["No"].alias == "No-18"
        assert customer_fields["Company"].alias == "$Company"

    def test_gl_entry_validation(self):
        """Test GLEntry model validation."""
        valid_gl = GLEntry(
            EntryNo=12345,
            PostingDate=datetime(2024, 1, 15),
            Amount=1500.50,
            DocumentNo="INV-001",
            GLAccountNo="1100",
        )
        assert valid_gl.EntryNo == 12345
        assert valid_gl.Amount == 1500.50


class TestSparkSchemaGeneration:
    """Test Spark schema generation from models."""

    def test_time_entry_schema(self):
        """Test TimeEntry Spark schema generation."""
        schema = TimeEntry.model_spark_schema()

        # Check schema structure
        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "memberId" in field_names
        assert "actualHours" in field_names

        # Check field types
        id_field = next(f for f in schema.fields if f.name == "id")
        assert str(id_field.dataType) in ["IntegerType", "IntegerType()"]

        hours_field = next(f for f in schema.fields if f.name == "actualHours")
        assert str(hours_field.dataType) in [
            "DoubleType",
            "DoubleType()",
            "FloatType",
            "FloatType()",
        ]

    def test_customer_schema(self):
        """Test Customer Spark schema generation."""
        schema = Customer.model_spark_schema()

        field_names = [f.name for f in schema.fields]
        assert "No" in field_names
        assert "Name" in field_names
        assert "Company" in field_names

        # Check string type
        no_field = next(f for f in schema.fields if f.name == "No")
        assert str(no_field.dataType) in ["StringType", "StringType()"]


class TestModelCompatibility:
    """Test model compatibility between systems."""

    def test_cross_system_field_mapping(self):
        """Test field mapping between PSA and BC."""
        # PSA TimeEntry has memberId
        psa_fields = set(TimeEntry.model_fields.keys())
        assert "memberId" in psa_fields

        # BC doesn't have memberId but might have Resource
        bc_customer_fields = set(Customer.model_fields.keys())
        assert "No" in bc_customer_fields

        # These would map in conformed dimensions
        mapping = {
            "psa_member_id": "memberId",  # From TimeEntry
            "bc_customer_no": "No",  # From Customer
            "unified_key": "generated",  # Surrogate key
        }

        assert mapping["psa_member_id"] in psa_fields
        assert mapping["bc_customer_no"] in bc_customer_fields


class TestDataValidation:
    """Test data validation scenarios."""

    def test_batch_validation(self):
        """Test validating batches of data."""
        # Sample data that might come from API
        raw_time_entries = [
            {"id": 1, "memberId": 100, "actualHours": 8.0},
            {"id": 2, "memberId": 101, "actualHours": 7.5},
            {"id": "invalid", "memberId": 102, "actualHours": 6.0},  # Invalid ID
        ]

        valid_entries = []
        invalid_entries = []

        for entry in raw_time_entries:
            try:
                valid_entry = TimeEntry(**entry)
                valid_entries.append(valid_entry)
            except ValidationError as e:
                invalid_entries.append({"data": entry, "error": str(e)})

        assert len(valid_entries) == 2
        assert len(invalid_entries) == 1

    def test_nested_data_validation(self):
        """Test validation of nested structures."""
        # Sample data with nested references
        raw_agreement = {
            "id": 1,
            "name": "Test Agreement",
            "agreementNumber": "AGR-001",
            "company": {"id": 100, "identifier": "COMP100", "name": "Test Company"},
        }

        # Flatten for model validation
        flattened = {
            "id": raw_agreement["id"],
            "name": raw_agreement["name"],
            "agreementNumber": raw_agreement["agreementNumber"],
            "companyId": raw_agreement["company"]["id"],
        }

        valid_agreement = Agreement(**flattened)
        assert valid_agreement.companyId == 100


class TestSchemaEvolution:
    """Test handling schema changes."""

    def test_adding_optional_field(self):
        """Test backward compatibility when adding fields."""
        # Original data
        original_data = {"id": 1, "memberId": 100, "actualHours": 8.0}

        # Should work with current model
        time_entry = TimeEntry(**original_data)
        assert time_entry.id == 1

        # If model adds new optional field, old data still works
        # This simulates schema evolution
        time_entry.newOptionalField = None  # type: ignore
        assert hasattr(time_entry, "newOptionalField")

    def test_field_type_changes(self):
        """Test handling field type changes."""
        # Data with string that can be converted to int
        flexible_data = {
            "id": "123",  # String instead of int
            "memberId": 456,
            "actualHours": "8.5",  # String instead of float
        }

        # Pydantic should handle type coercion
        time_entry = TimeEntry(**flexible_data)
        assert time_entry.id == 123  # Converted to int
        assert time_entry.actualHours == 8.5  # Converted to float


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
