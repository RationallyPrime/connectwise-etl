#!/usr/bin/env python
"""
CDM Model Generator - Generates Pydantic models from CDM JSON schema files.

This script reads CDM (Common Data Model) JSON schema files and generates
Pydantic models with SparkModel inheritance for use in Microsoft Fabric.

Usage:
    python cdm_model_generator.py <schema_file> [--output <output_file>]

Example:
    python cdm_model_generator.py Currency-4.cdm.json --output currency_model.py
"""

import argparse
import json
import re
from pathlib import Path
from typing import Any

# Mapping of CDM data formats to Python types
CDM_TYPE_MAPPINGS = {
    "String": "str",
    "Int32": "int",
    "Int64": "int",
    "Decimal": "float",
    "Date": "datetime.date",
    "DateTime": "datetime.datetime",
    "Boolean": "bool",
    "Guid": "str",  # Using str for GUID
}


def parse_cdm_name(name: str) -> tuple[str, str]:
    """
    Parse CDM field name to extract the field name and number.

    Args:
        name: CDM field name (e.g., "EntryNo-1", "GLAccountNo-3")

    Returns:
        Tuple of (field_name, field_number)
    """
    match = re.match(r"^(.+)-(\d+)$", name)
    if match:
        field_name = match.group(1)
        field_number = match.group(2)
        return field_name, field_number
    else:
        # Handle special fields without numbers
        return name, ""


def to_snake_case(name: str) -> str:
    """Convert PascalCase to snake_case."""
    # Insert underscore before capital letters (except the first)
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Insert underscore before capital letter sequences
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1)
    # Handle GL and ID specially
    s3 = re.sub(r"\bGL\b", "gl", s2)
    s3 = re.sub(r"\bID\b", "id", s3)
    # Convert to lowercase
    return s3.lower()


def format_field_name(name: str) -> str:
    """Format CDM field name to a valid Python field name."""
    field_name, _ = parse_cdm_name(name)

    # Handle special field names
    if field_name == "$Company":
        return "company"
    elif field_name == "$systemId" or field_name == "systemId":
        return "system_id"

    # Convert to snake_case
    return to_snake_case(field_name)


def get_python_type(cdm_type: str, optional: bool = True) -> str:
    """Convert CDM data format to Python type annotation."""
    base_type = CDM_TYPE_MAPPINGS.get(cdm_type, "Any")

    if optional:
        return f"{base_type} | None"
    else:
        return base_type


def process_attribute(attr: dict[str, Any]) -> dict[str, Any]:
    """Process a CDM attribute into field information."""
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

    field_name = format_field_name(name)
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


def generate_model_class(entity: dict[str, Any]) -> str:
    """Generate Python class code for a CDM entity."""
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
        field_info = process_attribute(attr)

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
        field_line = f"    {field_info['field_name']}: {field_info['python_type']}"

        # Add Field with metadata
        field_args = []

        # Always use None as default for optional fields
        if " | None" in field_info["python_type"]:
            field_args.append("default=None")

        # Add alias if field name differs from CDM name
        if field_info["field_name"] != field_info["cdm_name"]:
            field_args.append(f'alias="{field_info["cdm_name"]}"')

        # Add description
        desc_parts = []
        if field_info["display_name"]:
            desc_parts.append(field_info["display_name"])
        if field_info["max_length"]:
            desc_parts.append(f"Max length: {field_info['max_length']}")
        if field_info["scale"]:
            desc_parts.append(f"Scale: {field_info['scale']}")

        if desc_parts:
            field_args.append(f'description="{". ".join(desc_parts)}"')

        # Add Field if there are any arguments
        if field_args:
            field_line += f" = Field({', '.join(field_args)})"

        lines.append(field_line)

    # Add blank line after class
    lines.append("")

    return "\n".join(lines)


def generate_models_file(schema_path: Path, output_path: Path | None = None) -> None:
    """Generate models.py file from CDM schema."""
    # Read the CDM schema
    with open(schema_path) as f:
        schema = json.load(f)

    # Default output path if not specified
    if output_path is None:
        output_path = Path(schema_path.stem + "_model.py")

    # Generate the file
    with open(output_path, "w") as f:
        # Write header
        f.write('"""\n')
        f.write(f"CDM models generated from {schema_path.name}\n\n")
        f.write("Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n")
        f.write('"""\n\n')

        # Write imports
        f.write("from __future__ import annotations\n\n")
        f.write("import datetime\n")
        f.write("from typing import Any\n\n")
        f.write("from pydantic import Field\n")
        f.write("from sparkdantic import SparkModel\n\n\n")

        # Process each entity definition
        definitions = schema.get("definitions", [])
        for entity in definitions:
            model_code = generate_model_class(entity)
            f.write(model_code)
            f.write("\n")

    print(f"✅ Generated models in {output_path}")


def main():
    """Main entry point for the CDM model generator."""
    parser = argparse.ArgumentParser(
        description="Generate Pydantic models from CDM JSON schema files"
    )
    parser.add_argument(
        "schema_file", type=Path, help="Path to CDM JSON schema file (e.g., Currency-4.cdm.json)"
    )
    parser.add_argument(
        "--output", "-o", type=Path, help="Output file path (defaults to <schema_name>_model.py)"
    )

    args = parser.parse_args()

    # Validate input file exists
    if not args.schema_file.exists():
        print(f"Error: Schema file not found: {args.schema_file}")
        return 1

    # Generate the models
    try:
        generate_models_file(args.schema_file, args.output)
        return 0
    except Exception as e:
        print(f"Error generating models: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
