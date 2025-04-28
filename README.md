# ConnectWise PSA to Microsoft Fabric Integration

This package extracts company data from ConnectWise PSA API and loads it directly into Microsoft Fabric OneLake.

## Key Features

- Optimized for Microsoft Fabric execution environment
- Secure credential handling via Fabric Key Vault
- Direct writing to OneLake using native Fabric paths

## Setup

### Building the Package

Build a wheel distribution for deployment to Fabric:

```bash
# Using the provided build script
python build_wheel.py

# Or manually with Python build
python -m pip install build
python -m build --wheel
```

This will create a `.whl` file in the `dist/` directory.

### Deploying to Microsoft Fabric

1. **Create a Lakehouse** in your workspace
2. **Add secrets** to your workspace Key Vault:
   - `CW_COMPANY`
   - `CW_PUBLIC_KEY`
   - `CW_PRIVATE_KEY`
   - `CW_CLIENTID`
3. **Upload the wheel file** to your lakehouse
4. **Create a Notebook** and attach your Lakehouse
5. Install the wheel in the first cell:
   ```python
   %pip install /lakehouse/Files/dist/fabric_api-0.1.0-py3-none-any.whl
   ```
6. Run the ETL code in the next cell:
   ```python
   from fabric_api.main import main
   main(
       workspace_name="YourWorkspaceName",  # Your actual workspace name
       lakehouse_name="YourLakehouseName"   # Your actual lakehouse name
   )
   ```

## Data Flow

1. **Extract**: Pull company data from ConnectWise PSA API
2. **Transform**: Convert to DataFrame and add extraction timestamp
3. **Load**: Write directly to OneLake via abfss:// URLs

## Architecture Notes

- `fabric_helpers.py`: Utilities for OneLake path resolution
- `transform.py`: Processes data and writes directly to OneLake
- `upload.py`: No-op function for compatibility (data is already in OneLake)
- Package-based deployment for clean integration with Fabric
