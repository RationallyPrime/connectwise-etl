#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to create a temporary OpenAPI schema from our sample entities.
This is used for regenerating models when we don't have access to the full OpenAPI schema.
"""

import json
import os
from pathlib import Path

# Directory with the sample entities
SAMPLES_DIR = "entity_samples"

# Output schema file
OUTPUT_FILE = "debug_output/temp_schema.json"

def create_property_schema(value, description=""):
    """Create schema property from sample value."""
    if isinstance(value, str):
        return {"type": "string", "description": description}
    elif isinstance(value, int):
        return {"type": "integer", "description": description}
    elif isinstance(value, float):
        return {"type": "number", "description": description}
    elif isinstance(value, bool):
        return {"type": "boolean", "description": description}
    elif isinstance(value, dict):
        if value.get("id") and (value.get("name") or value.get("identifier")):
            # This is likely a reference field
            return {"type": "object", "description": description}
        else:
            # Regular object
            properties = {}
            for k, v in value.items():
                properties[k] = create_property_schema(v)
            return {
                "type": "object",
                "properties": properties,
                "description": description
            }
    elif isinstance(value, list):
        if value:
            items = create_property_schema(value[0])
        else:
            items = {}
        return {
            "type": "array",
            "items": items,
            "description": description
        }
    else:
        return {"type": "string", "description": description}

def create_openapi_schema():
    """Create OpenAPI schema from sample entities."""
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    # Initialize the schema
    schema = {
        "openapi": "3.0.0",
        "info": {
            "title": "ConnectWise API",
            "version": "1.0.0"
        },
        "paths": {},
        "components": {
            "schemas": {}
        }
    }
    
    # Process each sample file
    for file_path in Path(SAMPLES_DIR).glob("*.json"):
        entity_name = file_path.stem
        
        # Skip if not a main entity
        if entity_name not in ["Agreement", "PostedInvoice", "UnpostedInvoice", "TimeEntry", "ExpenseEntry", "ProductItem"]:
            continue
            
        print(f"Processing {entity_name}...")
        
        # Load the sample
        with open(file_path, "r") as f:
            sample_data = json.load(f)
        
        # Extract required fields (exclude _info and properties with None values)
        required_fields = []
        for key, value in sample_data.items():
            if key != "_info" and value is not None:
                required_fields.append(key)
        
        # Create schema
        entity_schema = {
            "type": "object",
            "properties": {},
            "required": required_fields
        }
        
        # Add properties
        for key, value in sample_data.items():
            if key != "_info":  # Skip _info
                entity_schema["properties"][key] = create_property_schema(value)
        
        # Add to schema components
        schema["components"]["schemas"][entity_name] = entity_schema
    
    # Write schema to file
    with open(OUTPUT_FILE, "w") as f:
        json.dump(schema, f, indent=2)
    
    print(f"Created schema at {OUTPUT_FILE}")

if __name__ == "__main__":
    create_openapi_schema()