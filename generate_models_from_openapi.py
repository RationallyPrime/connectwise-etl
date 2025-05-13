#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to generate Pydantic models from OpenAPI schemas using datamodel-code-generator.
This replaces the older JSON-sample based approach with direct schema generation.
"""

import os
import sys
import logging
import argparse
import subprocess
import tempfile
import shutil
import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional, List

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Default OpenAPI schema URL for ConnectWise Manage
DEFAULT_SCHEMA_URL = "https://api-na.myconnectwise.net/v4_6_release/apis/3.0/system/info/api/swaggerdoc"

# Entity type mapping - what entities we want to generate models for
ENTITY_TYPES = {
    "Agreement": {
        "schema_path": "finance.Agreement",
        "output_file": "agreement.py",
    },
    "TimeEntry": {
        "schema_path": "time.TimeEntry",
        "output_file": "time_entry.py",
    },
    "ExpenseEntry": {
        "schema_path": "expense.ExpenseEntry",
        "output_file": "expense_entry.py",
    },
    "Invoice": {
        "schema_path": "finance.Invoice",
        "output_file": "invoice.py",
    },
    "PostedInvoice": {  # This is actually the same as Invoice in the API
        "schema_path": "finance.Invoice",
        "output_file": "posted_invoice.py",
    },
    "UnpostedInvoice": {
        "schema_path": "finance.UnpostedInvoice",
        "output_file": "unposted_invoice.py",
    },
    "ProductItem": {
        "schema_path": "procurement.ProductItem",
        "output_file": "product_item.py",
    }
}

# Fields to exclude from generated models
EXCLUDE_FIELDS = [
    "_info",          # Metadata links we don't need
    "customFields",   # Custom fields can vary and add complexity
    "dateRemoved",    # Unused field
]

def fetch_openapi_schema(url: str = DEFAULT_SCHEMA_URL, output_file: Optional[str] = None) -> dict:
    """
    Fetches the OpenAPI schema from a URL using curl.
    
    Args:
        url: URL to fetch the schema from
        output_file: If provided, saves the schema to this file
    
    Returns:
        Parsed OpenAPI schema as a dictionary
    """
    logger.info(f"Fetching OpenAPI schema from {url}")
    
    if output_file:
        # Save to file directly
        subprocess.run(["curl", "-s", url, "-o", output_file], check=True)
        
        # Read the file content
        with open(output_file, "r") as f:
            if output_file.endswith(".yaml") or output_file.endswith(".yml"):
                schema = yaml.safe_load(f)
            else:
                schema = json.load(f)
    else:
        # Fetch and parse directly
        result = subprocess.run(["curl", "-s", url], capture_output=True, text=True, check=True)
        schema = json.loads(result.stdout)
    
    logger.info(f"Successfully fetched schema with {len(schema.get('paths', {}))} paths")
    return schema

def load_schema_from_file(file_path: str) -> dict:
    """
    Loads an OpenAPI schema from a file (JSON or YAML).
    
    Args:
        file_path: Path to the schema file
    
    Returns:
        Parsed schema as a dictionary
    """
    logger.info(f"Loading schema from {file_path}")
    
    with open(file_path, "r") as f:
        if file_path.endswith(".yaml") or file_path.endswith(".yml"):
            schema = yaml.safe_load(f)
        else:
            schema = json.load(f)
    
    logger.info(f"Successfully loaded schema with {len(schema.get('paths', {}))} paths")
    return schema

def extract_entity_schema(schema: dict, entity_path: str) -> Optional[dict]:
    """
    Extracts a specific entity schema from the full OpenAPI schema.
    
    Args:
        schema: Full OpenAPI schema
        entity_path: Path to the entity schema (e.g. "finance.Agreement")
    
    Returns:
        Extracted schema or None if not found
    """
    # Check in components/schemas
    components = schema.get("components", {}).get("schemas", {})
    
    # Handle dotted paths (module.Entity)
    if "." in entity_path:
        module, entity = entity_path.split(".", 1)
        # Try different formats the schema might use
        candidates = [
            f"{module}.{entity}",
            entity,
            f"{module}{entity}",
            f"{entity}Model"
        ]
        
        for candidate in candidates:
            if candidate in components:
                return components[candidate]
    else:
        # Direct lookup
        if entity_path in components:
            return components[entity_path]
    
    logger.warning(f"Could not find schema for {entity_path}")
    return None

def prepare_schema_for_entity(schema: dict, entity_name: str, entity_config: dict) -> Optional[dict]:
    """
    Prepares a schema for a specific entity by extracting and cleaning it.

    Args:
        schema: Full OpenAPI schema
        entity_name: Name of the entity
        entity_config: Configuration for the entity

    Returns:
        Prepared schema or None if not found
    """
    entity_schema = extract_entity_schema(schema, entity_config["schema_path"])
    if not entity_schema:
        return None

    # Create a minimal schema document focused on just this entity
    minimal_schema = {
        "openapi": "3.0.0",
        "info": {"title": f"{entity_name} API", "version": "1.0.0"},
        "paths": {},
        "components": {"schemas": {entity_name: entity_schema}}
    }

    # Remove excluded fields
    if "properties" in entity_schema:
        for field in EXCLUDE_FIELDS:
            if field in entity_schema["properties"]:
                del entity_schema["properties"][field]

    # Make all non-required fields optional
    required_fields = entity_schema.get("required", [])

    # Iterate through all properties
    if "properties" in entity_schema:
        for field_name, field_props in entity_schema["properties"].items():
            # Skip required fields
            if field_name in required_fields:
                continue

            # Make nested object fields nullable
            if field_props.get("type") == "object":
                field_props["nullable"] = True

            # Make simple fields nullable (this helps datamodel-codegen make them Optional)
            elif "nullable" not in field_props:
                field_props["nullable"] = True

    return minimal_schema

def post_process_model_file(file_path: str) -> None:
    """
    Post-process the generated model file to fix Pydantic v2 compatibility issues.

    Args:
        file_path: Path to the generated model file
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Fix __root__ models to use RootModel
    if '__root__: Any' in content:
        # Add import for RootModel
        import_section = 'from __future__ import annotations\n\nfrom datetime import datetime\nfrom enum import Enum\nfrom typing import Any\n\nfrom pydantic import Field, RootModel\nfrom sparkdantic import SparkModel'
        content = content.replace('from pydantic import Field\nfrom sparkdantic import SparkModel', import_section)

        # Replace __root__ models with RootModel
        lines = content.splitlines()
        new_lines = []
        skip_next = False

        for i, line in enumerate(lines):
            if skip_next:
                skip_next = False
                continue

            if '__root__: Any' in line and i > 0 and 'class Config:' in lines[i-3]:
                # Get class name
                class_name = lines[i-4].strip().split('(')[0].split(' ')[1]
                # Create RootModel-based class
                new_lines.append(f"class {class_name}(RootModel, SparkModel):")
                new_lines.append("    class Config:")
                new_lines.append("        validate_by_name = True")
                new_lines.append("")
                new_lines.append("    root: Any")
                new_lines.append("")
                # Skip the next line (which is blank)
                skip_next = True
            else:
                new_lines.append(line)

        content = '\n'.join(new_lines)

    # Update Config for main models
    content = content.replace('allow_population_by_field_name = True', 'validate_by_name = True')

    # Add warning filter at the top of the file
    if "import warnings" not in content:
        content = content.replace('from __future__ import annotations',
                                'from __future__ import annotations\n\nimport warnings\n\n# Filter Pydantic deprecation warnings for Config options\nwarnings.filterwarnings("ignore", message="Valid config keys have changed in V2")')

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info(f"Post-processed model file: {file_path}")

def generate_models(
    schema: dict, 
    output_dir: str, 
    target_entities: Optional[List[str]] = None
) -> bool:
    """
    Generate Pydantic models from an OpenAPI schema.
    
    Args:
        schema: OpenAPI schema
        output_dir: Directory to write models to
        target_entities: Optional list of entities to generate (if None, generate all)
    
    Returns:
        True if successful, False otherwise
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each entity type
    success = True
    processed_entities = []
    
    for entity_name, config in ENTITY_TYPES.items():
        if target_entities and entity_name not in target_entities:
            logger.info(f"Skipping {entity_name} (not in target list)")
            continue
        
        logger.info(f"Processing {entity_name}...")
        
        # Prepare schema for this entity
        entity_schema = prepare_schema_for_entity(schema, entity_name, config)
        if not entity_schema:
            logger.error(f"Failed to extract schema for {entity_name}")
            success = False
            continue
        
        # Write entity schema to a temporary file
        with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as tmp_schema:
            json.dump(entity_schema, tmp_schema)
            tmp_schema.flush()
            
            output_path = os.path.join(output_dir, config["output_file"])
            
            # Run datamodel-codegen to generate the model
            cmd = [
                "datamodel-codegen",
                "--input", tmp_schema.name,
                "--input-file-type", "openapi",
                "--output", output_path,
                "--class-name", entity_name,
                "--base-class", "sparkdantic.SparkModel",
                "--field-constraints",
                "--use-schema-description",
                "--target-python-version", "3.10",
                "--use-subclass-enum",
                "--use-standard-collections",
                "--capitalize-enum-members",
                "--field-include-all-keys",
                "--allow-population-by-field-name",
                "--strict-nullable",
                "--use-union-operator"  # Use X | None instead of Optional[X]
                # Removed --snake-case-field to keep camelCase field names
            ]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                # Post-process the generated file to fix Pydantic v2 compatibility issues
                post_process_model_file(output_path)
                
                logger.info(f"✅ Successfully generated model for {entity_name}")
                processed_entities.append(entity_name)
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Error generating model for {entity_name}: {e}")
                logger.error(f"Command output: {e.stderr}")
                success = False
    
    # Create __init__.py file
    init_path = os.path.join(output_dir, "__init__.py")
    with open(init_path, 'w', encoding='utf-8') as f:
        f.write('#!/usr/bin/env python\n')
        f.write('# -*- coding: utf-8 -*-\n')
        f.write('"""\n')
        f.write('ConnectWise API models generated directly from OpenAPI schema.\n')
        f.write('"""\n\n')
        f.write('import warnings\n\n')
        f.write('# Filter Pydantic deprecation warnings for Config options\n')
        f.write('warnings.filterwarnings("ignore", message="Valid config keys have changed in V2")\n\n')
        f.write('from typing import List, Any\n')
        f.write('from pydantic import RootModel\n')
        f.write('from sparkdantic import SparkModel\n\n')
        
        # Import statements for each processed entity
        for entity_name in processed_entities:
            module_name = ENTITY_TYPES[entity_name]["output_file"].replace(".py", "")
            f.write(f'from .{module_name} import {entity_name}\n')
        
        f.write('\n')
        
        # __all__ list
        f.write('__all__ = [\n')
        for entity_name in processed_entities:
            f.write(f'    "{entity_name}",\n')
        f.write(']\n')
    
    logger.info(f"Created package __init__.py at {init_path}")
    
    return success

def main():
    parser = argparse.ArgumentParser(description='Generate Pydantic models from OpenAPI schema')
    
    # Input options - either URL or file
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--url', help='URL to fetch OpenAPI schema from', default=DEFAULT_SCHEMA_URL)
    input_group.add_argument('--file', help='Path to local OpenAPI schema file (JSON or YAML)')
    
    # Output options
    parser.add_argument('--output-dir', help='Directory to write models to', default='fabric_api/connectwise_models')
    parser.add_argument('--save-schema', help='Save fetched schema to this file')
    
    # Entity options
    parser.add_argument('--entities', nargs='+', help='Specific entities to generate (space-separated)')
    
    args = parser.parse_args()
    
    try:
        # Get the schema
        if args.file:
            schema = load_schema_from_file(args.file)
        else:
            schema = fetch_openapi_schema(args.url, args.save_schema)
        
        # Generate models
        success = generate_models(schema, args.output_dir, args.entities)
        
        # Exit with appropriate status code
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.exception(f"Error generating models: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()