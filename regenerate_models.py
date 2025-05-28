#!/usr/bin/env python
"""Model regeneration handling BC CDM files and other integrations."""

import json
import subprocess
import sys
from pathlib import Path


def cdm_to_jsonschema(cdm_data: dict) -> dict:
    """Convert CDM format to proper JSON Schema format for datamodel-codegen."""
    if "definitions" not in cdm_data or not cdm_data["definitions"]:
        raise ValueError("Invalid CDM file: missing definitions")

    entity = cdm_data["definitions"][0]  # First (and usually only) entity
    entity_name = entity["entityName"]

    # Clean class name (remove version numbers)
    import re

    class_name_match = re.match(r"^([A-Za-z]+).*", entity_name)
    clean_name = class_name_match.group(1) if class_name_match else entity_name.replace("-", "")

    # Map CDM data types to JSON Schema types
    type_mapping = {
        "String": "string",
        "Int32": "integer",
        "Int64": "integer",
        "Decimal": "number",
        "Date": "string",
        "DateTime": "string",
        "Guid": "string",
        "Boolean": "boolean",
    }

    properties = {}

    for attr in entity.get("hasAttributes", []):
        cdm_field_name = attr["name"]
        # Remove numeric suffixes like "-1", "-2000000000"
        clean_field_name = cdm_field_name.split("-")[0]

        # Handle special field names
        if clean_field_name == "$Company":
            python_field_name = "Company"
        elif clean_field_name == "$systemId" or clean_field_name == "systemId":
            python_field_name = "SystemId"
        elif clean_field_name.startswith("$"):
            python_field_name = clean_field_name[1:]  # Remove $ but keep case
        else:
            python_field_name = clean_field_name

        data_format = attr.get("dataFormat", "String")
        json_type = type_mapping.get(data_format, "string")

        field_def = {
            "type": [json_type, "null"],  # Make all fields nullable
            "title": attr.get("displayName", python_field_name),
        }

        # Add length constraints for strings
        if attr.get("maximumLength") and json_type == "string":
            field_def["maxLength"] = attr["maximumLength"]

        # Handle decimal precision from appliedTraits
        if data_format == "Decimal" and attr.get("appliedTraits"):
            for trait in attr["appliedTraits"]:
                if trait.get("traitReference") == "is.dataFormat.numeric.shaped":
                    for arg in trait.get("arguments", []):
                        if arg.get("name") == "scale":
                            scale_val = int(arg["value"])
                            field_def["multipleOf"] = 1 / (10**scale_val)

        # Use alias if field name was changed
        if python_field_name != cdm_field_name:
            # Store the CDM name as custom property for alias generation
            field_def["x-cdm-name"] = cdm_field_name

        properties[python_field_name] = field_def

    # Create JSON Schema
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": clean_name,
        "description": entity.get("description", f"BC {clean_name} entity"),
        "properties": properties,
        "additionalProperties": False,
    }

    return schema


def generate_bc_models(cdm_dir: str, output_file: str, package_dir: str):
    """Generate BC models from CDM directory using datamodel-codegen with proper JSON Schema conversion."""
    cdm_path = Path(cdm_dir)
    output_path = Path(output_file).absolute()

    if not cdm_path.exists():
        print(f"Error: CDM directory {cdm_path} not found")
        sys.exit(1)

    # Find all CDM files
    cdm_files = list(cdm_path.glob("*.cdm.json"))
    if not cdm_files:
        print(f"Error: No *.cdm.json files found in {cdm_path}")
        sys.exit(1)

    print(f"Found {len(cdm_files)} CDM files:")
    for f in cdm_files:
        print(f"  - {f.name}")

    # Convert CDM files to individual JSON Schema files
    temp_dir = Path("/tmp/bc_schemas")
    temp_dir.mkdir(exist_ok=True)

    schema_files = []
    for cdm_file in cdm_files:
        try:
            with open(cdm_file) as f:
                cdm_data = json.load(f)

            schema = cdm_to_jsonschema(cdm_data)

            # Save as individual JSON Schema file
            schema_file = temp_dir / f"{cdm_file.stem}.schema.json"
            with open(schema_file, "w") as f:
                json.dump(schema, f, indent=2)

            schema_files.append(schema_file)
            print(f"  ✓ Converted {cdm_file.name} → {schema_file.name}")

        except Exception as e:
            print(f"  ✗ Failed to convert {cdm_file.name}: {e}")
            continue

    if not schema_files:
        print("❌ No CDM files could be converted to JSON Schema")
        sys.exit(1)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Generate models individually and combine them
    all_model_code = []

    for schema_file in schema_files:
        print(f"Generating model from {schema_file.name}...")

        # Create temporary output for this model
        temp_model_file = temp_dir / f"{schema_file.stem}_model.py"

        cmd = [
            "datamodel-codegen",
            "--input-file-type",
            "jsonschema",
            "--base-class",
            "sparkdantic.SparkModel",
            "--target-python-version",
            "3.11",
            "--remove-special-field-name-prefix",
            "--use-standard-collections",
            "--use-schema-description",
            "--use-field-description",
            "--reuse-model",
            "--field-constraints",
            "--strict-nullable",
            "--use-union-operator",
            "--input",
            str(schema_file.absolute()),
            "--output",
            str(temp_model_file),
        ]

        result = subprocess.run(cmd, cwd=package_dir, check=False)

        if result.returncode == 0:
            # Read the generated model and extract just the class definition
            with open(temp_model_file) as f:
                content = f.read()

            # Extract everything after the imports (skip header comments and imports)
            lines = content.split("\n")
            class_lines = []
            found_class = False

            for line in lines:
                if line.strip().startswith("class ") and "SparkModel" in line:
                    found_class = True
                if found_class:
                    class_lines.append(line)

            if class_lines:
                all_model_code.append("\n".join(class_lines))
                print(f"  ✓ Generated model from {schema_file.name}")

            # Clean up temp model file
            temp_model_file.unlink()
        else:
            print(f"  ✗ Failed to generate model from {schema_file.name}")

    if not all_model_code:
        print("❌ No BC models could be generated")
        sys.exit(1)

    # Write combined models file
    with open(output_path, "w") as f:
        # Write header
        f.write('"""\n')
        f.write("CDM models generated from CDM JSON schema files.\n")
        f.write("\n")
        f.write("Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n")
        f.write('"""\n\n')

        # Write imports
        f.write("from __future__ import annotations\n\n")
        f.write("from pydantic import ConfigDict, Field\n")
        f.write("from sparkdantic import SparkModel\n\n\n")

        # Write all model classes
        for model_code in all_model_code:
            f.write(model_code)
            f.write("\n\n")

    # Clean up temp files
    for schema_file in schema_files:
        schema_file.unlink()
    temp_dir.rmdir()

    print(f"✅ Generated {len(all_model_code)} BC models in {output_path}")


def generate_regular_models(schema_file: str, output_file: str, package_dir: str):
    """Generate models for PSA/Jira using pyproject.toml config."""
    schema_path = Path(schema_file).absolute()
    output_path = Path(output_file).absolute()

    if not schema_path.exists():
        print(f"Error: Schema file {schema_path} not found")
        sys.exit(1)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Run datamodel-codegen with the package's pyproject.toml
    cmd = [
        "datamodel-codegen",
        "--input",
        str(schema_path),
        "--output",
        str(output_path),
    ]

    print(f"Generating models from {schema_path} → {output_path}")
    print(f"Using config from {package_dir}/pyproject.toml")

    result = subprocess.run(cmd, cwd=package_dir, check=False)

    if result.returncode == 0:
        print("✅ Model generation completed!")
    else:
        print("❌ Model generation failed!")
        sys.exit(1)


def main():
    if len(sys.argv) < 4:
        print("Usage: python regenerate_models.py <integration> <schema_file_or_dir> <output_file>")
        print("Examples:")
        print(
            "  PSA: python regenerate_models.py psa PSA_OpenAPI_schema.json packages/unified-etl-connectwise/src/unified_etl_connectwise/models/models.py"
        )
        print(
            "  BC:  python regenerate_models.py bc cdm_manifests packages/unified-etl-businesscentral/src/unified_etl_businesscentral/models/models.py"
        )
        sys.exit(1)

    integration = sys.argv[1]  # psa, bc, jira
    schema_input = sys.argv[2]  # file for PSA/Jira, directory for BC
    output_file = sys.argv[3]

    # Find the package directory for the integration
    package_map = {
        "psa": "packages/unified-etl-connectwise",
        "bc": "packages/unified-etl-businesscentral",
        "jira": "packages/unified-etl-jira",
    }

    if integration not in package_map:
        print(f"Error: Unknown integration '{integration}'. Supported: {list(package_map.keys())}")
        sys.exit(1)

    package_dir = package_map[integration]
    package_pyproject = Path(package_dir) / "pyproject.toml"

    if not package_pyproject.exists():
        print(f"Error: Package pyproject.toml not found at {package_pyproject}")
        sys.exit(1)

    # BC needs special CDM handling
    if integration == "bc":
        generate_bc_models(schema_input, output_file, package_dir)
    else:
        generate_regular_models(schema_input, output_file, package_dir)


if __name__ == "__main__":
    main()
