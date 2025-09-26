"""Unit tests for ConnectWise data models."""

import pytest
from enum import Enum
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

try:
    from sparkdantic import SparkModel
except ImportError:
    pytest.skip("sparkdantic not installed", allow_module_level=True)


class CustomFieldValueType(str, Enum):
    """Custom field value types in ConnectWise."""

    TextArea = "TextArea"
    Text = "Text"
    Number = "Number"
    Date = "Date"
    Checkbox = "Checkbox"


class ConfigurationTypeQuestionEntryType(str, Enum):
    """Configuration question entry types."""

    Date = "Date"
    EntryField = "EntryField"
    Checkbox = "Checkbox"
    Number = "Number"


class CustomFieldValue(SparkModel):
    """ConnectWise custom field value model."""

    id: int | None = None
    caption: str | None = None
    type: CustomFieldValueType | None = None
    entryMethod: ConfigurationTypeQuestionEntryType | None = None
    numberOfDecimals: int | None = None
    value: str | dict[str, Any] | None = None
    connectWiseId: str | None = None


class TestModel(SparkModel):
    """Test model with custom fields."""

    id: int | None = None
    name: str | None = None
    customFields: list[CustomFieldValue] | None = None


class TestCustomFieldModels:
    """Test custom field value models and enum handling."""

    def test_custom_field_value_creation(self):
        """Test creating a CustomFieldValue instance."""
        field = CustomFieldValue(
            id=1,
            caption="Test Field",
            type=CustomFieldValueType.Text,
            entryMethod=ConfigurationTypeQuestionEntryType.EntryField,
            value="Test Value",
        )

        assert field.id == 1
        assert field.caption == "Test Field"
        assert field.type == CustomFieldValueType.Text
        assert field.value == "Test Value"

    def test_custom_field_enum_serialization(self):
        """Test that enums serialize correctly."""
        field = CustomFieldValue(
            type=CustomFieldValueType.TextArea, entryMethod=ConfigurationTypeQuestionEntryType.Date
        )

        # Enums should serialize to their string values
        assert field.type == "TextArea"
        assert field.entryMethod == "Date"

    def test_model_with_custom_fields_list(self):
        """Test model containing list of custom fields."""
        model = TestModel(
            id=100,
            name="Test Entity",
            customFields=[
                CustomFieldValue(
                    id=1, caption="Field 1", type=CustomFieldValueType.Text, value="Value 1"
                ),
                CustomFieldValue(
                    id=2, caption="Field 2", type=CustomFieldValueType.Number, value="42"
                ),
            ],
        )

        assert model.id == 100
        assert len(model.customFields) == 2
        assert model.customFields[0].caption == "Field 1"
        assert model.customFields[1].value == "42"

    def test_custom_field_with_dict_value(self):
        """Test custom field with dictionary value."""
        field = CustomFieldValue(
            id=3,
            caption="Complex Field",
            type=CustomFieldValueType.Text,
            value={"nested": "data", "count": 123},
        )

        assert isinstance(field.value, dict)
        assert field.value["nested"] == "data"
        assert field.value["count"] == 123

    def test_spark_schema_generation(self, spark):
        """Test that SparkDantic generates correct Spark schemas."""
        # Get the Spark schema from the model
        schema = TestModel.spark_schema()

        # Verify it's a valid StructType
        assert isinstance(schema, StructType)

        # Check that main fields exist
        field_names = [field.name for field in schema.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "customFields" in field_names

        # Create a DataFrame with the schema
        data = [
            (1, "Test", None),
            (2, "Test2", None),
        ]
        df = spark.createDataFrame(data, schema)

        # Should be able to create and count
        assert df.count() == 2

    def test_nullable_fields(self):
        """Test that all fields are properly nullable."""
        # Create model with all None values
        field = CustomFieldValue()

        assert field.id is None
        assert field.caption is None
        assert field.type is None
        assert field.entryMethod is None
        assert field.numberOfDecimals is None
        assert field.value is None
        assert field.connectWiseId is None

    def test_all_enum_values(self):
        """Test all enum values work correctly."""
        # Test all CustomFieldValueType values
        for enum_val in CustomFieldValueType:
            field = CustomFieldValue(type=enum_val)
            assert field.type == enum_val.value

        # Test all ConfigurationTypeQuestionEntryType values
        for enum_val in ConfigurationTypeQuestionEntryType:
            field = CustomFieldValue(entryMethod=enum_val)
            assert field.entryMethod == enum_val.value
