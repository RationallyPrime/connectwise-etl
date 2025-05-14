#!/usr/bin/env python
"""Test module for model generation functionality."""

import os
import json
import tempfile
import shutil
from pathlib import Path

import pytest
from pydantic import BaseModel

from fabric_api.core.models import (
    load_schema, 
    extract_entity_schema,
    extract_reference_model,
    prepare_unified_schema,
    generate_models,
    ENTITY_TYPES,
    REFERENCE_MODELS
)

# Test data directory
TEST_DATA_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / "debug_output"

def test_load_schema():
    """Test loading an OpenAPI schema."""
    # Create a simple test schema
    test_schema = {
        "openapi": "3.0.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {"/test": {}},
        "components": {"schemas": {"Test": {"type": "object"}}}
    }
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as tmp:
        json.dump(test_schema, tmp)
        tmp.flush()
        
        # Load the schema
        schema = load_schema(tmp.name)
        
        # Verify schema was loaded correctly
        assert schema["openapi"] == "3.0.0"
        assert schema["info"]["title"] == "Test API"
        assert "/test" in schema["paths"]
        assert "Test" in schema["components"]["schemas"]

def test_extract_entity_schema():
    """Test extracting an entity schema from the OpenAPI components."""
    # Create a test schema with entities
    test_schema = {
        "components": {
            "schemas": {
                "Test": {"type": "object", "properties": {"id": {"type": "integer"}}},
                "finance.Invoice": {"type": "object", "properties": {"amount": {"type": "number"}}},
                "Invoice": {"type": "object", "properties": {"name": {"type": "string"}}}
            }
        }
    }
    
    # Test direct lookup
    entity = extract_entity_schema(test_schema, "Test")
    assert entity["type"] == "object"
    assert "id" in entity["properties"]
    
    # Test module.Entity lookup
    entity = extract_entity_schema(test_schema, "finance.Invoice")
    assert entity["type"] == "object"
    assert "amount" in entity["properties"]
    
    # Test fallback to entity name
    entity = extract_entity_schema(test_schema, "finance.OtherEntity")
    assert entity is None

def test_extract_reference_model():
    """Test extracting a reference model from the OpenAPI components."""
    # Create a test schema with reference models
    test_schema = {
        "components": {
            "schemas": {
                "ActivityReference": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer", "format": "int32", "nullable": True},
                        "name": {"type": "string"}
                    }
                }
            }
        }
    }
    
    # Test extracting ActivityReference
    ref_model = extract_reference_model(test_schema, "ActivityReference")
    assert ref_model["type"] == "object"
    assert "id" in ref_model["properties"]
    assert "name" in ref_model["properties"]
    
    # Test extracting non-existent model
    ref_model = extract_reference_model(test_schema, "NonExistentReference")
    assert ref_model is None

def test_prepare_unified_schema():
    """Test preparing a unified schema with selected entities and reference models."""
    # Create a test schema with entities and reference models
    test_schema = {
        "components": {
            "schemas": {
                "ActivityReference": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer", "nullable": True},
                        "name": {"type": "string"}
                    }
                },
                "Agreement": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string"},
                        "customFields": {"type": "object"},
                        "_info": {"type": "object"}
                    },
                    "required": ["name"]
                },
                "finance.Invoice": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "amount": {"type": "number"},
                        "dateRemoved": {"type": "string", "format": "date-time"}
                    }
                }
            }
        }
    }
    
    # Create temp entity types for testing
    entity_types = {
        "Agreement": {
            "schema_path": "Agreement",
            "output_file": "agreement.py",
        },
        "Invoice": {
            "schema_path": "finance.Invoice",
            "output_file": "invoice.py",
        }
    }
    
    # Monkey patch ENTITY_TYPES for testing
    original_entity_types = ENTITY_TYPES.copy()
    import fabric_api.core.models
    fabric_api.core.models.ENTITY_TYPES = entity_types
    
    try:
        # Prepare unified schema with both entities
        unified_schema = prepare_unified_schema(test_schema)
        
        # Verify reference models are included
        assert "ActivityReference" in unified_schema["components"]["schemas"]
        
        # Verify entities are included and cleaned
        assert "Agreement" in unified_schema["components"]["schemas"]
        agreement = unified_schema["components"]["schemas"]["Agreement"]
        assert "customFields" not in agreement["properties"]
        assert "_info" not in agreement["properties"]
        assert agreement["properties"]["id"]["nullable"] is True
        
        # Invoice (from finance.Invoice)
        assert "Invoice" in unified_schema["components"]["schemas"]
        invoice = unified_schema["components"]["schemas"]["Invoice"]
        assert "dateRemoved" not in invoice["properties"]
        assert invoice["properties"]["id"]["nullable"] is True
        
        # Test filtering with specific entity
        unified_schema = prepare_unified_schema(test_schema, ["Agreement"])
        assert "Agreement" in unified_schema["components"]["schemas"]
        assert "Invoice" not in unified_schema["components"]["schemas"]
    finally:
        # Restore original ENTITY_TYPES
        fabric_api.core.models.ENTITY_TYPES = original_entity_types

def test_generate_models_to_temp_dir():
    """Test generating models to a temporary directory."""
    # Skip if the schema file doesn't exist
    schema_file = "PSA_OpenAPI_schema.json"
    if not os.path.exists(schema_file):
        pytest.skip(f"Schema file {schema_file} not found")
    
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate models for a specific entity
        result = generate_models(schema_file, temp_dir, ["Agreement"])
        
        # Verify success
        assert result is True
        
        # Check that files were created
        assert os.path.exists(os.path.join(temp_dir, "agreement.py"))
        assert os.path.exists(os.path.join(temp_dir, "__init__.py"))