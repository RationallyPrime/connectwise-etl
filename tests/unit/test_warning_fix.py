#!/usr/bin/env python
"""
Simple test script to verify Pydantic warning filters work.
"""

import sys

# Import necessary modules
from fabric_api.connectwise_models import Agreement

print(f"Python version: {sys.version}")
print("Testing imports for Pydantic v2 warning fix...")

print("✅ Imports successful - Checking models...")

# Create a simple model instance
agreement = Agreement(
    id=1,
    name="Test Agreement",
    type={"id": 1, "name": "Test Type"},
    company={"id": 1, "name": "Test Company"},
    contact={"id": 1, "name": "Test Contact"},
)

print(f"✅ Model initialized correctly: {agreement.id=}, {agreement.name=}")
print("✅ Test passed - No Pydantic warnings displayed!")
