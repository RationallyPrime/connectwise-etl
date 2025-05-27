#!/usr/bin/env python
"""Simple model regeneration using pyproject.toml configs in each package."""
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
    
    if not Path(schema_file).exists():
        print(f"Error: Schema file {schema_file} not found")
        sys.exit(1)
    
    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Find the package directory for the integration
    package_map = {
        "psa": "packages/unified-etl-connectwise",
        "bc": "packages/unified-etl-businesscentral", 
        "jira": "packages/unified-etl-jira"
    }
    
    if integration not in package_map:
        print(f"Error: Unknown integration '{integration}'. Supported: {list(package_map.keys())}")
        sys.exit(1)
    
    package_dir = package_map[integration]
    package_pyproject = Path(package_dir) / "pyproject.toml"
    
    if not package_pyproject.exists():
        print(f"Error: Package pyproject.toml not found at {package_pyproject}")
        sys.exit(1)
    
    # Convert to absolute paths since we're changing working directory
    schema_path = Path(schema_file).absolute()
    output_path = Path(output_file).absolute()
    
    # Run datamodel-codegen with the package's pyproject.toml
    cmd = [
        "datamodel-codegen",
        "--input", str(schema_path),
        "--output", str(output_path),
    ]
    
    print(f"Generating {integration} models from {schema_path} → {output_path}")
    print(f"Using config from {package_pyproject}")
    
    # Change to package directory so it picks up the pyproject.toml config
    result = subprocess.run(cmd, cwd=package_dir, check=False)
    
    if result.returncode == 0:
        print("✅ Model generation completed!")
    else:
        print("❌ Model generation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()