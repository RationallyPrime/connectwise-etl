#!/usr/bin/env python3
"""Test with the exact CustomFieldValue structure."""

from enum import Enum
from typing import Any
from sparkdantic import SparkModel

class CustomFieldValueType(str, Enum):
    TextArea = 'TextArea'
    Text = 'Text'

class ConfigurationTypeQuestionEntryType(str, Enum):
    Date = 'Date'
    EntryField = 'EntryField'

# Test the exact CustomFieldValue structure
class CustomFieldValue(SparkModel):
    id: int | None = None
    caption: str | None = None
    type: CustomFieldValueType | None = None
    entryMethod: ConfigurationTypeQuestionEntryType | None = None
    numberOfDecimals: int | None = None
    value: str | dict[str, Any] | None = None
    connectWiseId: str | None = None

# Test with a model that has customFields
class TestModel(SparkModel):
    id: int | None = None
    name: str | None = None
    customFields: list[CustomFieldValue] | None = None

print("Testing CustomFieldValue alone:")
try:
    schema = CustomFieldValue.model_spark_schema()
    print(f"  ✅ CustomFieldValue schema works")
except Exception as e:
    print(f"  ❌ CustomFieldValue failed: {e}")

print("\nTesting Model with customFields list:")
try:
    schema = TestModel.model_spark_schema()
    print(f"  ✅ Model with customFields list works")
except Exception as e:
    print(f"  ❌ Model with customFields list failed: {e}")
    import traceback
    traceback.print_exc()