#!/usr/bin/env python
"""
Utility script to regenerate models from a subset of the OpenAPI schema.

This script:
1. Creates a subset schema with only the models we need and their dependencies
2. Generates a single models.py file with all models to avoid circular imports
3. Updates __init__.py to import from models.py

Usage:
    python regenerate_models.py [--entities ENTITY1 ENTITY2 ...]

Example:
    python regenerate_models.py
    python regenerate_models.py --entities Agreement TimeEntry
"""

import argparse
import json
import logging
import os
import subprocess
import tempfile
from typing import Any

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Default entity models to include
DEFAULT_ENTITIES = {
    "Agreement": {"schema_path": "Agreement"},
    "TimeEntry": {"schema_path": "TimeEntry"},
    "ExpenseEntry": {"schema_path": "ExpenseEntry"},
    "Invoice": {"schema_path": "Invoice"},
    "UnpostedInvoice": {"schema_path": "UnpostedInvoice"},
    "ProductItem": {"schema_path": "ProductItem"},
}

# Reference models to always include
REFERENCE_MODELS = [
    "ActivityReference",
    "AgreementReference",
    "AgreementTypeReference",
    "BatchReference",
]


def load_schema(schema_path: str) -> dict[str, Any]:
    """Load an OpenAPI schema from a file path."""
    with open(schema_path) as f:
        return json.load(f)


def find_references(schema_obj: dict[str, Any], refs: set[str]) -> set[str]:
    """Find all $ref references in a schema object."""
    if isinstance(schema_obj, dict):
        for k, v in schema_obj.items():
            if k == "$ref" and isinstance(v, str) and v.startswith("#/components/schemas/"):
                # Extract the model name from the reference
                ref_name = v.split("/")[-1]
                refs.add(ref_name)
            elif isinstance(v, dict | list):
                refs = refs.union(find_references(v, refs))
    elif isinstance(schema_obj, list):
        for item in schema_obj:
            if isinstance(item, dict | list):
                refs = refs.union(find_references(item, refs))
    return refs


def extract_model_with_dependencies(
    schema: dict[str, Any], model_names: list[str]
) -> dict[str, Any]:
    """
    Extract specified models and their dependencies from an OpenAPI schema.

    Args:
        schema: The full OpenAPI schema
        model_names: List of model names to extract

    Returns:
        A new schema containing only the specified models and their dependencies
    """
    # Create a new schema with the same structure
    subset_schema = {
        "openapi": schema.get("openapi", "3.0.0"),
        "info": schema.get("info", {"title": "Subset Schema", "version": "1.0.0"}),
        "paths": {},
        "components": {"schemas": {}},
    }

    # Start with the specified models
    models_to_include = set(model_names)
    processed_models = set()

    # Process models and their dependencies
    while models_to_include.difference(processed_models):
        # Get next model to process
        model_name = next(iter(models_to_include.difference(processed_models)))

        # Skip if model doesn't exist in the original schema
        if model_name not in schema.get("components", {}).get("schemas", {}):
            logger.warning(f"Model '{model_name}' not found in the schema!")
            processed_models.add(model_name)
            continue

        # Add the model to the subset schema
        model_schema = schema["components"]["schemas"][model_name]
        subset_schema["components"]["schemas"][model_name] = model_schema

        # Find references in this model
        refs = find_references(model_schema, set())
        models_to_include.update(refs)

        # Mark this model as processed
        processed_models.add(model_name)

    # Log the models included in the subset schema
    included_models = set(subset_schema["components"]["schemas"].keys())
    logger.info(f"Generated subset schema with {len(included_models)} models")

    return subset_schema


def create_init_file(output_dir: str, entity_names: list[str]) -> None:
    """Create the __init__.py file that imports from models.py."""
    init_path = os.path.join(output_dir, "__init__.py")

    with open(init_path, "w", encoding="utf-8") as f:
        f.write('"""ConnectWise API models generated from OpenAPI schema.\n\n')
        f.write("Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n")
        f.write('"""\n\n')

        # Import all models from models.py
        f.write("# Import all models from the single models.py file\n")
        f.write("from .models import (\n")

        # Reference models
        f.write("    # Reference models\n")
        for ref_model in REFERENCE_MODELS:
            f.write(f"    {ref_model},\n")

        f.write("    \n")
        f.write("    # Entity models\n")

        # Entity models
        for entity_name in entity_names:
            f.write(f"    {entity_name},\n")
            if entity_name == "Invoice":
                f.write("    PostedInvoice,\n")

        f.write(")\n\n")

        # __all__ list
        f.write("__all__ = [\n")
        f.write("    # Reference models\n")
        for ref_model in REFERENCE_MODELS:
            f.write(f'    "{ref_model}",\n')

        f.write("    \n")
        f.write("    # Entity models\n")
        for entity_name in entity_names:
            f.write(f'    "{entity_name}",\n')
            if entity_name == "Invoice":
                f.write('    "PostedInvoice",\n')

        f.write("]\n")

    logger.info(f"✅ Created __init__.py at {init_path}")


def generate_models(entities: dict[str, dict[str, str]], schema_file: str, output_dir: str) -> None:
    """Generate models using datamodel-code-generator from pyproject.toml config."""
    # Load the schema
    logger.info(f"Loading schema from {schema_file}")
    schema = load_schema(schema_file)

    # Add reference models and entity models to the list of models to include
    models_to_include = list(entities.keys()) + REFERENCE_MODELS

    # Create subset schema
    logger.info(f"Creating subset schema with {len(models_to_include)} models...")
    subset_schema = extract_model_with_dependencies(schema, models_to_include)

    # Write debug schema for inspection
    debug_dir = "debug_output"
    os.makedirs(debug_dir, exist_ok=True)
    with open(os.path.join(debug_dir, "temp_schema.json"), "w") as f:
        json.dump(subset_schema, f, indent=2)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Create a single models.py file
    models_file = os.path.join(output_dir, "models.py")

    # Start with the header
    with open(models_file, "w", encoding="utf-8") as f:
        f.write('"""\n')
        f.write("ConnectWise API models generated from OpenAPI schema.\n\n")
        f.write("Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n")
        f.write("This file contains all models to avoid circular imports.\n")
        f.write('"""\n\n')
        f.write("from __future__ import annotations\n\n")
        f.write("from datetime import datetime\n")
        f.write("from typing import Any, Dict, List, Optional, Literal\n")
        f.write("from uuid import UUID\n\n")
        f.write("from pydantic import Field\n")
        f.write("from sparkdantic import SparkModel\n\n")

        # Add reference models section
        f.write("#############################################\n")
        f.write("# Reference Models\n")
        f.write("#############################################\n\n")

        # Define the base reference models
        f.write("class ActivityReference(SparkModel):\n")
        f.write('    """Base reference model for ConnectWise entities."""\n')
        f.write("    id: int | None = None\n")
        f.write("    name: str | None = None\n")
        f.write("    _info: dict[str, str] | None = None\n\n")

        f.write("class AgreementReference(ActivityReference):\n")
        f.write('    """Reference model for Agreement entities."""\n')
        f.write("    type: str | None = None\n")
        f.write("    chargeFirmFlag: bool | None = None\n\n")

        f.write("class AgreementTypeReference(ActivityReference):\n")
        f.write('    """Reference model for AgreementType entities."""\n')
        f.write("    pass\n\n")

        f.write("class BatchReference(AgreementTypeReference):\n")
        f.write('    """Reference model for Batch entities."""\n')
        f.write("    pass\n\n")

    # Write subset schema to temp file for model generation
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as tmp:
        json.dump(subset_schema, tmp)
        tmp.flush()

        # Use datamodel-codegen to generate a temporary file with all models
        temp_output = os.path.join(debug_dir, "temp_models.py")
        cmd = [
            "datamodel-codegen",
            "--input",
            tmp.name,
            "--output",
            temp_output,
        ]

        # Run the command
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info("✅ Generated temporary models file")

            # Read the generated file
            with open(temp_output) as f:
                content = f.read()

            # Process the content to extract model definitions
            lines = content.split("\n")
            model_lines = []
            skip_imports = True
            skip_refs = False
            in_custom_field_value = False
            fix_value_field = False

            # Process lines to remove imports and handle duplicated reference models
            for line in lines:
                # Skip import lines at the top (including from __future__ import)
                if skip_imports and (
                    line.startswith("from ") or line.startswith("import ") or line == ""
                ):
                    continue
                else:
                    skip_imports = False

                # Check if we're in the CustomFieldValue class
                if "class CustomFieldValue(" in line:
                    in_custom_field_value = True
                elif in_custom_field_value and line.strip() == "":
                    in_custom_field_value = False

                # Check if this is the value field in CustomFieldValue that needs fixing
                if in_custom_field_value and "value:" in line and "dict[str, Any]" in line:
                    # Fix the overly restrictive type
                    line = line.replace("dict[str, Any]", "Any")
                    # Add a comment explaining the change
                    line += "  # API inconsistency: Sometimes returns strings or other non-dict values"
                    fix_value_field = True

                # Skip reference model definitions that we already added
                if any(f"class {ref_model}(" in line for ref_model in REFERENCE_MODELS):
                    skip_refs = True
                elif skip_refs and line.strip() == "":
                    skip_refs = False
                    continue

                # Handle any reference model aliases (like ActivityReference = ProjectReference)
                skip_line = False
                for ref_model in REFERENCE_MODELS:
                    if line.strip().startswith(f"{ref_model} = "):
                        model_lines.append(f"# {ref_model} (already defined above)")
                        skip_line = True
                        break

                if not skip_refs and not skip_line:
                    # Skip duplicate import statements that might appear in the middle of the file
                    if not (line.startswith("from ") or line.startswith("import ")):
                        model_lines.append(line)

            if fix_value_field:
                logger.info("✅ Fixed overly restrictive type for CustomFieldValue.value field")

            # Add entity models section
            with open(models_file, "a", encoding="utf-8") as f:
                f.write("#############################################\n")
                f.write("# Entity Models\n")
                f.write("#############################################\n\n")
                f.write("\n".join(model_lines))

                # Add PostedInvoice alias
                f.write("\n\n# PostedInvoice is an alias of Invoice\n")
                f.write("PostedInvoice = Invoice")

            # Clean up temporary file
            os.remove(temp_output)

        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Error generating models: {e}")
            logger.error(f"Command output: {e.stderr}")

    # Create __init__.py file
    create_init_file(output_dir, list(entities.keys()))

    logger.info("✅ Model generation complete!")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Generate Pydantic models from OpenAPI schema")
    parser.add_argument(
        "--schema", default="PSA_OpenAPI_schema.json", help="Path to the OpenAPI schema"
    )
    parser.add_argument(
        "--output-dir",
        default="fabric_api/connectwise_models",
        help="Directory to output the generated models",
    )
    parser.add_argument("--entities", nargs="+", help="List of entities to generate models for")
    return parser.parse_args()


def main():
    args = parse_args()

    # Get entities to generate
    if args.entities:
        entities = {}
        for entity_name in args.entities:
            if entity_name in DEFAULT_ENTITIES:
                entities[entity_name] = DEFAULT_ENTITIES[entity_name]
            else:
                logger.warning(f"Unknown entity: {entity_name}, using default configuration")
                entities[entity_name] = {"schema_path": entity_name}
    else:
        entities = DEFAULT_ENTITIES

    # Generate models
    generate_models(entities, args.schema, args.output_dir)


if __name__ == "__main__":
    main()
