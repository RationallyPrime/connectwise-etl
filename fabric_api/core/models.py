"""
Module for generating Pydantic models from OpenAPI schemas using datamodel-code-generator.
This module uses the configuration defined in pyproject.toml for datamodel-code-generator.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from typing import Any

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Default schema file for ConnectWise Manage
DEFAULT_SCHEMA_FILE = "PSA_OpenAPI_schema.json"

# Entity type mapping
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

# Reference models to include in all generation
REFERENCE_MODELS = [
    "ActivityReference",
]

# Fields to exclude from generated models
EXCLUDE_FIELDS = [
    "_info",          # Metadata links we don't need
    "customFields",   # Custom fields can vary and add complexity
    "dateRemoved",    # Unused field
]

def load_schema(file_path: str) -> dict[str, Any]:
    """Load OpenAPI schema from file."""
    logger.info(f"Loading schema from {file_path}")
    with open(file_path) as f:
        schema = json.load(f)
    logger.info(f"Successfully loaded schema with {len(schema.get('paths', {}))} paths")
    return schema

def extract_entity_schema(schema: dict[str, Any], entity_path: str) -> dict[str, Any] | None:
    """Extract an entity schema from the OpenAPI components."""
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

def extract_reference_model(schema: dict[str, Any], model_name: str) -> dict[str, Any] | None:
    """Extract a reference model from the OpenAPI components."""
    components = schema.get("components", {}).get("schemas", {})
    if model_name in components:
        return components[model_name]
    logger.warning(f"Could not find reference model schema for {model_name}")
    return None

def prepare_unified_schema(schema: dict[str, Any], entities: list[str] | None = None) -> dict[str, Any]:
    """Create a unified schema with selected entities and required reference models."""
    # Create a schema just for our selected entities
    unified_schema = {
        "openapi": "3.0.0",
        "info": {"title": "ConnectWise PSA API", "version": "1.0.0"},
        "paths": {},
        "components": {"schemas": {}}
    }

    entity_set = set(entities) if entities else None

    # First, add all needed reference models
    for ref_model_name in REFERENCE_MODELS:
        logger.info(f"Processing reference model {ref_model_name}...")
        ref_schema = extract_reference_model(schema, ref_model_name)
        if ref_schema:
            # Add to unified schema
            unified_schema["components"]["schemas"][ref_model_name] = ref_schema
            logger.info(f"Added reference model {ref_model_name}")
        else:
            logger.error(f"Failed to extract schema for reference model {ref_model_name}")

    # Process each entity
    for entity_name, config in ENTITY_TYPES.items():
        if entity_set and entity_name not in entity_set:
            logger.info(f"Skipping {entity_name} (not in target list)")
            continue

        logger.info(f"Processing {entity_name}...")

        # Extract entity schema
        entity_schema = extract_entity_schema(schema, config["schema_path"])
        if not entity_schema:
            logger.error(f"Failed to extract schema for {entity_name}")
            continue

        # Clean up schema
        if "properties" in entity_schema:
            # Remove excluded fields
            for field in EXCLUDE_FIELDS:
                if field in entity_schema["properties"]:
                    del entity_schema["properties"][field]

            # Make all non-required fields nullable
            required_fields = entity_schema.get("required", [])
            for field_name, field_props in entity_schema["properties"].items():
                if field_name not in required_fields and "nullable" not in field_props:
                    field_props["nullable"] = True

        # Add to unified schema
        unified_schema["components"]["schemas"][entity_name] = entity_schema

    return unified_schema

def generate_models(schema_file: str, output_dir: str, entities: list[str] | None = None) -> bool:
    """Generate models from OpenAPI schema using datamodel-code-generator."""
    # Load schema
    schema = load_schema(schema_file)

    # Create unified schema
    unified_schema = prepare_unified_schema(schema, entities)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Write schema to temp file
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as tmp_file:
        json.dump(unified_schema, tmp_file)
        tmp_file.flush()

        # Dump schema to a debug file for inspection if needed
        debug_schema_path = os.path.join(os.path.dirname(output_dir), "debug_output", "temp_schema.json")
        os.makedirs(os.path.dirname(debug_schema_path), exist_ok=True)
        with open(debug_schema_path, 'w') as f:
            json.dump(unified_schema, f, indent=2)
        logger.info(f"Saved debug schema to {debug_schema_path}")

        # Process each entity
        success = True
        processed_entities = []

        for entity_name, config in ENTITY_TYPES.items():
            if entities and entity_name not in entities:
                continue

            output_path = os.path.join(output_dir, config["output_file"])

            # Run datamodel-codegen with pyproject.toml config
            cmd = [
                "datamodel-codegen",
                "--input", tmp_file.name,
                "--output", output_path,
                "--class-name", entity_name
            ]

            try:
                subprocess.run(cmd, capture_output=True, text=True, check=True)
                logger.info(f"✅ Successfully generated model for {entity_name}")
                processed_entities.append(entity_name)
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Error generating model for {entity_name}: {e}")
                logger.error(f"Command output: {e.stderr}")
                success = False

    # Create __init__.py file
    init_path = os.path.join(output_dir, "__init__.py")
    with open(init_path, 'w', encoding='utf-8') as f:
        f.write('"""ConnectWise API models generated from OpenAPI schema.\n\n')
        f.write('Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n')
        f.write('"""\n\n')
        f.write('from typing import Any, Dict, List, Optional\n')
        f.write('from pydantic import RootModel, Field\n')
        f.write('from sparkdantic import SparkModel\n\n')

        # Import statements for entities
        for entity_name in processed_entities:
            module_name = ENTITY_TYPES[entity_name]["output_file"].replace(".py", "")
            f.write(f'from .{module_name} import {entity_name}\n')

        # Include special imports for ActivityReference
        # We need to import it from each module where it's defined
        f.write('\n# Reference Models\n')
        f.write('from .agreement import ActivityReference\n')

        f.write('\n')

        # __all__ list
        f.write('__all__ = [\n')
        for entity_name in processed_entities:
            f.write(f'    "{entity_name}",\n')

        # Add reference models to __all__
        f.write('    # Reference models\n')
        for ref_model in REFERENCE_MODELS:
            f.write(f'    "{ref_model}",\n')

        f.write(']\n')

    logger.info(f"Created package __init__.py at {init_path}")

    return success

def main():
    parser = argparse.ArgumentParser(description='Generate Pydantic models from OpenAPI schema')
    parser.add_argument('--file', help='Path to OpenAPI schema file', default=DEFAULT_SCHEMA_FILE)
    parser.add_argument('--output-dir', help='Output directory for models', default='fabric_api/connectwise_models')
    parser.add_argument('--entities', nargs='+', help='Specific entities to generate (space-separated)')

    args = parser.parse_args()

    try:
        # Generate models
        success = generate_models(args.file, args.output_dir, args.entities)

        # Exit with appropriate status code
        sys.exit(0 if success else 1)

    except Exception as e:
        logger.exception(f"Error generating models: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
