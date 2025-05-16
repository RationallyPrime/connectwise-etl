"""
Business Central CDM model generator using datamodel-code-generator.
This module generates models from CDM JSON schemas with proper CamelCase field naming.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_cdm_schema(cdm_file: Path) -> tuple[dict[str, Any], str]:
    """Process a CDM JSON file and prepare it for model generation."""
    with open(cdm_file) as f:
        cdm_data = json.load(f)

    # Extract entity from definitions list
    definitions = cdm_data.get("definitions", [])
    if not definitions:
        raise ValueError(f"No definitions found in {cdm_file}")

    # Get the first (and usually only) entity definition
    entity = definitions[0]

    # Create OpenAPI-compatible schema
    entity_name = entity.get("entityName", "Entity").replace("-", "")

    # Convert attributes to OpenAPI properties
    properties = {}
    required = []

    for attr in entity.get("hasAttributes", []):
        field_name = attr["name"]

        # Remove numeric suffix for the property name but keep original for alias
        base_name = field_name.split("-")[0] if "-" in field_name else field_name

        # Determine type
        data_format = attr.get("dataFormat", "String")
        field_type = "string"
        field_format = None

        if data_format == "Int32":
            field_type = "integer"
            field_format = "int32"
        elif data_format == "Int64":
            field_type = "integer"
            field_format = "int64"
        elif data_format == "Decimal":
            field_type = "number"
            field_format = "double"
        elif data_format == "Date":
            field_type = "string"
            field_format = "date"
        elif data_format in ["DateTime", "DateTime2"]:
            field_type = "string"
            field_format = "date-time"
        elif data_format == "Boolean":
            field_type = "boolean"

        # Create property definition
        prop_def = {
            "type": field_type,
            "description": attr.get("displayName", ""),
            "title": field_name,  # Store original name as title
            "nullable": True,  # Make all fields nullable by default
        }

        if field_format:
            prop_def["format"] = field_format

        # Add max length if available
        if "maximumLength" in attr:
            prop_def["maxLength"] = attr["maximumLength"]

        # Add to properties using CamelCase name
        properties[base_name] = prop_def

        # No fields are required in CDM

    # Create the schema definition
    schema_def = {
        "type": "object",
        "properties": properties,
        "description": entity.get("description", ""),
        "title": entity_name,
    }

    return schema_def, entity_name


def generate_bc_models(input_dir: Path, output_dir: Path) -> bool:
    """Generate BC models from CDM JSON files."""

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all CDM JSON files
    cdm_files = list(input_dir.glob("*.cdm.json"))
    if not cdm_files:
        logger.error(f"No CDM files found in {input_dir}")
        return False

    logger.info(f"Found {len(cdm_files)} CDM schema files")

    # Process each file and create individual models
    model_files = []

    for cdm_file in sorted(cdm_files):
        logger.info(f"Processing {cdm_file.name}...")

        try:
            # Process CDM schema
            schema_def, entity_name = process_cdm_schema(cdm_file)

            # Create a simple OpenAPI schema for this model
            openapi_schema = {
                "openapi": "3.0.0",
                "info": {"title": f"{entity_name} Model", "version": "1.0.0"},
                "components": {"schemas": {entity_name: schema_def}},
            }

            # Write to temp file and generate model
            with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as tmp_file:
                json.dump(openapi_schema, tmp_file, indent=2)
                tmp_file.flush()

                # Output path for this model
                output_file = output_dir / f"{entity_name}.py"

                # Use the existing cdm_model_generator script
                cmd = [
                    "python",
                    "fabric_api/cdm_model_generator.py",
                    tmp_file.name,
                    "--output",
                    str(output_file),
                ]

                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    logger.info(f"✅ Generated {entity_name}")
                    model_files.append(entity_name)
                except subprocess.CalledProcessError as e:
                    logger.error(f"❌ Error generating {entity_name}: {e}")
                    logger.error(f"Output: {e.stderr}")
                finally:
                    os.unlink(tmp_file.name)

        except Exception as e:
            logger.error(f"Error processing {cdm_file.name}: {e}")

    # Now combine all models into a single file using the original generator
    cmd = [
        "python",
        "fabric_api/generate_cdm_models.py",
        "--input-dir",
        str(input_dir),
        "--output",
        str(output_dir / "models.py"),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("✅ Combined all models into models.py")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Error combining models: {e}")
        logger.error(f"Output: {e.stderr}")

    # Create __init__.py
    init_path = output_dir / "__init__.py"
    with open(init_path, "w") as f:
        f.write('"""Business Central CDM models."""\n\n')
        f.write("from .models import *\n")

    return True


def main():
    parser = argparse.ArgumentParser(description="Generate BC models from CDM schemas")
    parser.add_argument(
        "--input-dir",
        help="Directory containing CDM JSON files",
        default="BC_ETL-main/BC_ETL-main/cdm_manifests",
    )
    parser.add_argument(
        "--output-dir", help="Output directory for models", default="fabric_api/bc_models_camelcase"
    )

    args = parser.parse_args()

    try:
        # Convert to Path objects
        input_dir = Path(args.input_dir)
        output_dir = Path(args.output_dir)

        # Generate models
        success = generate_bc_models(input_dir, output_dir)

        # Exit with appropriate status code
        sys.exit(0 if success else 1)

    except Exception as e:
        logger.exception(f"Error generating models: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
