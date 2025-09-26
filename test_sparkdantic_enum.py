#!/usr/bin/env python3
"""Test SparkDantic with the exact types from CustomFieldValue."""

from enum import Enum
from typing import Any
from sparkdantic import SparkModel

class CustomFieldValueType(str, Enum):
    TextArea = 'TextArea'
    Text = 'Text'

class TestModelA(SparkModel):
    """Test with optional enum - same as CustomFieldValue.type"""
    type: CustomFieldValueType | None = None

class TestModelB(SparkModel):
    """Test with required enum"""
    type: CustomFieldValueType

class TestModelC(SparkModel):
    """Test with optional string"""
    type: str | None = None

# Test what works
for model, name in [
    (TestModelA, "Optional Enum (like CustomFieldValue.type)"),
    (TestModelB, "Required Enum"),
    (TestModelC, "Optional String"),
]:
    print(f"\nTesting {name}:")
    try:
        schema = model.model_spark_schema()
        print(f"  ✅ Success")
    except Exception as e:
        print(f"  ❌ Failed: {e}")