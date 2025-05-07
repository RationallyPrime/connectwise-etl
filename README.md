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

## ConnectWise API Workarounds

This package now includes workarounds for relationship endpoint permission issues in the ConnectWise API. Instead of using direct relationship endpoints (which may require additional permissions), it uses filtered queries to standard endpoints to retrieve related data.

### Key Improvements:

1. **Time Entries**: Uses `/time/entries` with `invoice/id` filter instead of `/finance/invoices/{id}/timeentries`
2. **Expenses**: Uses `/expense/entries` with `invoice/id` filter instead of `/finance/invoices/{id}/expenses`
3. **Products**: Uses `/procurement/products` with `invoice/id` filter instead of `/finance/invoices/{id}/products`

These workarounds maintain full functionality while requiring fewer API permissions.

## JSON Export Tool

A production-ready export script is included to extract invoice data to JSON files:

```bash
# Basic usage (exports last 30 days of invoices)
python production_export.py

# Specify date range
python production_export.py --start_date 2025-04-01 --end_date 2025-04-30

# Specify output directory
python production_export.py --output_dir invoice_data_april

# Limit number of invoices
python production_export.py --limit 50

# Use credentials file instead of environment variables
python production_export.py --credentials config.json
```

### Credentials

Set up credentials using either:

1. **Environment variables**:
   ```
   CW_AUTH_USERNAME=your_username
   CW_AUTH_PASSWORD=your_password
   CW_CLIENTID=your_client_id
   ```

2. **JSON config file** (see `config.sample.json`):
   ```json
   {
       "username": "your_cw_username",
       "password": "your_cw_password",
       "client_id": "your_cw_client_id"
   }
   ```

### Output Format

The export script creates a JSON file for each invoice with this structure:

```json
{
    "header": {
        "id": 1001,
        "invoice_number": "1001",
        ...
    },
    "lines": [
        {
            "invoice_number": "1001",
            "line_no": 1,
            "description": "Time entry",
            "time_entry_id": 1631,
            ...
        },
        ...
    ]
}
```

It also generates a summary file with export statistics.

## Architecture Notes

- `fabric_helpers.py`: Utilities for OneLake path resolution
- `transform.py`: Processes data and writes directly to OneLake
- `upload.py`: No-op function for compatibility (data is already in OneLake)
- Package-based deployment for clean integration with Fabric
