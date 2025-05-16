#!/usr/bin/env python
"""
Generate all CDM models with CamelCase field names from a directory of CDM schema files.

This script reads all CDM JSON schema files in a directory and generates
a single models.py file containing all the Pydantic models with CamelCase fields.

Usage:
    python generate_bc_models_camelcase.py [--input-dir <dir>] [--output <file>]

Example:
    python generate_bc_models_camelcase.py --input-dir cdm_manifests --output bc_models.py
"""

import argparse
import json
import re
from pathlib import Path
from typing import Any

# Import the CDM model generator functions
from cdm_model_generator import (
    get_python_type,
    parse_cdm_name,
)


def format_field_name_camelcase(name: str) -> str:
    """Format CDM field name to a valid Python field name, keeping CamelCase."""
    field_name, _ = parse_cdm_name(name)

    # Handle special field names
    if field_name == "$Company":
        return "Company"
    elif field_name == "$systemId" or field_name == "systemId":
        return "SystemId"
    elif field_name.startswith("$"):
        # Remove $ prefix but keep the case
        return field_name[1:]

    # Return the field name without numeric suffix, keeping CamelCase
    return field_name


def process_attribute_camelcase(attr: dict[str, Any]) -> dict[str, Any]:
    """Process a CDM attribute into field information with CamelCase naming."""
    name = attr.get("name", "")
    data_format = attr.get("dataFormat", "String")
    display_name = attr.get("displayName", "")
    max_length = attr.get("maximumLength")

    # Extract numeric scale if present
    scale = None
    if attr.get("appliedTraits"):
        for trait in attr["appliedTraits"]:
            if trait.get("traitReference") == "is.dataFormat.numeric.shaped":
                if trait.get("arguments"):
                    for arg in trait["arguments"]:
                        if arg.get("name") == "scale":
                            scale = arg.get("value")

    field_name = format_field_name_camelcase(name)
    python_type = get_python_type(data_format)

    return {
        "field_name": field_name,
        "cdm_name": name,
        "python_type": python_type,
        "display_name": display_name,
        "max_length": max_length,
        "scale": scale,
        "data_format": data_format,
    }


def generate_model_class_camelcase(entity: dict[str, Any]) -> str:
    """Generate Python class code for a CDM entity with CamelCase fields."""
    entity_name = entity.get("entityName", "")
    display_name = entity.get("displayName", "")
    description = entity.get("description", "")
    attributes = entity.get("hasAttributes", [])

    # Clean up entity name (remove version number)
    class_name_match = re.match(r"^([A-Za-z]+).*", entity_name)
    if class_name_match:
        class_name = class_name_match.group(1)
    else:
        class_name = entity_name.replace("-", "")

    # Generate class definition
    lines = []
    lines.append(f"class {class_name}(SparkModel):")

    # Add docstring
    if description:
        lines.append(f'    """{description}')
    else:
        lines.append(f'    """{display_name or class_name}')

    if display_name and display_name != description:
        lines.append("    ")
        lines.append(f"    Display Name: {display_name}")

    lines.append(f"    Entity Name: {entity_name}")
    lines.append('    """')
    lines.append("")

    # Track field names to prevent duplicates
    used_field_names = set()

    # Generate fields
    for attr in attributes:
        field_info = process_attribute_camelcase(attr)

        # Handle duplicate field names by adding a suffix
        field_name = field_info["field_name"]
        if field_name in used_field_names:
            # If it's a duplicate, use the CDM suffix
            cdm_suffix = (
                field_info["cdm_name"].split("-")[-1] if "-" in field_info["cdm_name"] else ""
            )
            if cdm_suffix.isdigit():
                field_name = f"{field_name}_{cdm_suffix}"
            else:
                # Use a counter if no numeric suffix
                counter = 2
                while f"{field_name}_{counter}" in used_field_names:
                    counter += 1
                field_name = f"{field_name}_{counter}"

        used_field_names.add(field_name)
        field_info["field_name"] = field_name

        # Build field definition
        field_line = f"    {field_info['field_name']}: {field_info['python_type']} = Field("

        # Set default value
        field_line += "default=None"

        # Add alias if different from field name
        if field_info["cdm_name"] != field_info["field_name"]:
            field_line += f', alias="{field_info["cdm_name"]}"'

        # Add description
        description_parts = []
        if field_info["display_name"]:
            description_parts.append(field_info["display_name"])
        if field_info["max_length"]:
            description_parts.append(f"Max length: {field_info['max_length']}")

        if description_parts:
            desc_text = ". ".join(description_parts)
            field_line += f', description="{desc_text}"'

        field_line += ")"
        lines.append(field_line)

    return "\n".join(lines)


def process_cdm_directory(input_dir: Path, output_path: Path) -> None:
    """Process all CDM schema files in a directory and generate a single models file with CamelCase fields."""

    # Find all CDM JSON files
    cdm_files = list(input_dir.glob("*.cdm.json"))

    if not cdm_files:
        print(f"No CDM files found in {input_dir}")
        return

    print(f"Found {len(cdm_files)} CDM schema files")

    # Collect all models
    all_models = []
    model_names = []

    for cdm_file in sorted(cdm_files):
        print(f"Processing {cdm_file.name}...")

        try:
            with open(cdm_file, encoding="utf-8") as f:
                cdm_data = json.load(f)

            # Extract entity definitions
            definitions = cdm_data.get("definitions", [])

            for definition in definitions:
                if "entityName" in definition:
                    # Generate model class
                    model_code = generate_model_class_camelcase(definition)
                    all_models.append(model_code)

                    # Extract class name for imports
                    entity_name = definition.get("entityName", "")
                    class_name_match = re.match(r"^([A-Za-z]+).*", entity_name)
                    if class_name_match:
                        class_name = class_name_match.group(1)
                    else:
                        class_name = entity_name.replace("-", "")
                    model_names.append(class_name)

        except Exception as e:
            print(f"Error processing {cdm_file.name}: {e}")
            continue

    # Generate the output file
    with open(output_path, "w", encoding="utf-8") as f:
        # Write header
        f.write('"""\n')
        f.write("CDM models generated from CDM JSON schema files.\n")
        f.write("\n")
        f.write("Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n")
        f.write('"""\n\n')

        # Write imports
        f.write("from __future__ import annotations\n\n")
        f.write("import datetime\n")
        f.write("from typing import Any\n\n")
        f.write("from pydantic import Field\n")
        f.write("from sparkdantic import SparkModel\n\n\n")

        # Write all model classes
        for model in all_models:
            f.write(model)
            f.write("\n\n")

    print(f"✅ Generated {len(all_models)} models in {output_path}")

    # Create __init__.py file
    init_path = output_path.parent / "__init__.py"
    with open(init_path, "w", encoding="utf-8") as f:
        f.write('"""\n')
        f.write("BC CDM models.\n")
        f.write('"""\n\n')

        # Import all models from the models file
        models_module = output_path.stem
        f.write(f"from .{models_module} import *\n\n")

        # Generate __all__ list
        f.write("__all__ = [\n")
        for name in sorted(model_names):
            f.write(f'    "{name}",\n')
        f.write("]\n")

    print(f"✅ Created __init__.py at {init_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate CDM models with CamelCase field names from CDM JSON schema files"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("cdm_manifests"),
        help="Directory containing CDM JSON schema files",
    )
    parser.add_argument(
        "--output", type=Path, default=Path("bc_models.py"), help="Output file for generated models"
    )

    args = parser.parse_args()

    # Ensure input directory exists
    if not args.input_dir.exists():
        print(f"Error: Input directory '{args.input_dir}' does not exist")
        return 1

    # Ensure output directory exists
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Process CDM files
    process_cdm_directory(args.input_dir, args.output)

    return 0


if __name__ == "__main__":
    exit(main())
