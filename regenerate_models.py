#!/usr/bin/env python
"""Simple model regeneration using integration-specific configs."""
import subprocess
import sys
from pathlib import Path


def main():
    if len(sys.argv) < 4:
        print("Usage: python regenerate_models.py <integration> <schema_file> <output_file>")
        print("Example: python regenerate_models.py psa PSA_OpenAPI_schema.json packages/unified-etl-connectwise/src/unified_etl_connectwise/models/models.py")
        sys.exit(1)
    
    integration = sys.argv[1]  # psa, bc, jira
    schema_file = sys.argv[2]
    output_file = sys.argv[3]
    
    config_file = f"configs/{integration}-codegen.toml"
    
    if not Path(config_file).exists():
        print(f"Error: Config file {config_file} not found")
        sys.exit(1)
    
    if not Path(schema_file).exists():
        print(f"Error: Schema file {schema_file} not found")
        sys.exit(1)
    
    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    cmd = [
        "datamodel-codegen",
        "--input", schema_file,
        "--output", output_file,
        "--config", config_file
    ]
    
    print(f"Generating {integration} models from {schema_file} → {output_file}")
    subprocess.run(cmd, check=True)
    print("✅ Model generation completed!")


if __name__ == "__main__":
    main()