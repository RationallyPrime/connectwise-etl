#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Build script for creating a wheel package for deployment to Microsoft Fabric.
"""

import os
import subprocess
import shutil
from datetime import datetime

# Configuration
PACKAGE_NAME = "fabric_api"
VERSION = "0.2.0"  # Increment as needed
OUTPUT_DIR = "dist"

def main():
    """Build the package and prepare it for deployment."""
    print(f"Building {PACKAGE_NAME} v{VERSION} for Microsoft Fabric...")
    
    # Ensure we have the necessary tools
    try:
        subprocess.check_call(["pip", "install", "--upgrade", "build", "wheel"])
        print("✅ Build tools installed")
    except subprocess.CalledProcessError:
        print("❌ Failed to install build tools")
        return False
    
    # Create necessary directories
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Build the wheel
    print("Building wheel...")
    try:
        subprocess.check_call(["python", "-m", "build", "--wheel"])
        print(f"✅ Built wheel in {OUTPUT_DIR}/")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to build wheel: {e}")
        return False
    
    # Create deployment instructions
    create_deployment_instructions()
    
    print(f"\n✅ Package build complete! Wheel available in {OUTPUT_DIR}/")
    print("See deploy_to_fabric.md for deployment instructions")
    return True

def create_deployment_instructions():
    """Create a markdown file with deployment instructions."""
    instructions = f"""# Deploying to Microsoft Fabric

## Package Information
- Package: {PACKAGE_NAME}
- Version: {VERSION}
- Built: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Deployment Steps

1. **Create a Lakehouse** in your Fabric workspace (if you don't already have one)

2. **Add Secrets to Key Vault**
   - Navigate to your workspace Key Vault
   - Add the following secrets:
     - `CW_COMPANY`
     - `CW_PUBLIC_KEY`
     - `CW_PRIVATE_KEY`
     - `CW_CLIENTID`

3. **Upload the Wheel File**
   - Upload the wheel file to your lakehouse:
     - `{OUTPUT_DIR}/{PACKAGE_NAME}-{VERSION}-py3-none-any.whl`

4. **Create a New Notebook**
   - Attach your Lakehouse to the notebook
   - In the first cell, install the package:
     ```python
     %pip install /lakehouse/Files/{PACKAGE_NAME}-{VERSION}-py3-none-any.whl
     ```

5. **Run the ETL Process**
   - In the next cell, run the OneLake ETL process:
     ```python
     from fabric_api.orchestration import run_onelake_etl
     
     # Run a full ETL process with default parameters
     table_paths = run_onelake_etl()
     
     # Print results
     print("ETL Results:")
     print(f"  [Entity]: [Path]")
     ```

6. **Schedule Refreshes** (Optional)
   - Create a Data Pipeline in Fabric
   - Add a Notebook activity that runs your ETL notebook
   - Schedule the pipeline to run on your desired frequency
"""
    
    with open("deploy_to_fabric.md", "w") as f:
        f.write(instructions)

if __name__ == "__main__":
    main()