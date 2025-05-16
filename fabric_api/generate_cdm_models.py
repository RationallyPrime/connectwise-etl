#!/usr/bin/env python
"""
Generate all CDM models from a directory of CDM schema files.

This script reads all CDM JSON schema files in a directory and generates
a single models.py file containing all the Pydantic models.

Usage:
    python generate_cdm_models.py [--input-dir <dir>] [--output <file>]

Example:
    python generate_cdm_models.py --input-dir cdm_manifests --output cdm_models.py
"""

import argparse
import json
import re
from pathlib import Path

# Import the CDM model generator functions
from cdm_model_generator import (
    generate_model_class,
)


def process_cdm_directory(input_dir: Path, output_path: Path) -> None:
    """Process all CDM schema files in a directory and generate a single models file."""

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
            with open(cdm_file) as f:
                schema = json.load(f)

            definitions = schema.get("definitions", [])
            for entity in definitions:
                model_code = generate_model_class(entity)
                all_models.append(model_code)

                # Extract class name
                entity_name = entity.get("entityName", "")
                class_name_match = re.match(r"^([A-Za-z]+).*", entity_name)
                if class_name_match:
                    class_name = class_name_match.group(1)
                else:
                    class_name = entity_name.replace("-", "")
                model_names.append(class_name)

        except Exception as e:
            print(f"Error processing {cdm_file.name}: {e}")

    # Generate the combined models file
    with open(output_path, "w") as f:
        # Write header
        f.write('"""\n')
        f.write("CDM models generated from CDM JSON schema files.\n\n")
        f.write("Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n")
        f.write('"""\n\n')

        # Write imports
        f.write("from __future__ import annotations\n\n")
        f.write("import datetime\n")
        f.write("from typing import Any\n\n")
        f.write("from pydantic import Field\n")
        f.write("from sparkdantic import SparkModel\n\n\n")

        # Add all models
        for model_code in all_models:
            f.write(model_code)
            f.write("\n")

    print(f"✅ Generated {len(all_models)} models in {output_path}")

    # Create __init__.py file
    init_path = output_path.parent / "__init__.py"
    with open(init_path, "w") as f:
        f.write('"""CDM models for Business Central data.\n\n')
        f.write("Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n")
        f.write('"""\n\n')

        # Import all models
        f.write("from .models import (\n")
        for model_name in sorted(model_names):
            f.write(f"    {model_name},\n")
        f.write(")\n\n")

        # __all__ list
        f.write("__all__ = [\n")
        for model_name in sorted(model_names):
            f.write(f'    "{model_name}",\n')
        f.write("]\n")

    print(f"✅ Created __init__.py at {init_path}")


def main():
    """Main entry point for the CDM models generator."""
    parser = argparse.ArgumentParser(
        description="Generate all CDM models from a directory of CDM schema files"
    )
    parser.add_argument(
        "--input-dir",
        "-i",
        type=Path,
        default=Path("BC_ETL-main/BC_ETL-main/cdm_manifests"),
        help="Directory containing CDM JSON schema files",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("fabric_api/cdm_models/models.py"),
        help="Output file path for generated models",
    )

    args = parser.parse_args()

    # Validate input directory exists
    if not args.input_dir.exists():
        print(f"Error: Input directory not found: {args.input_dir}")
        return 1

    # Create output directory if needed
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Generate the models
    try:
        process_cdm_directory(args.input_dir, args.output)
        return 0
    except Exception as e:
        print(f"Error generating models: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
