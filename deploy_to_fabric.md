# Deploying to Microsoft Fabric

## Package Information
- Package: fabric_api
- Version: 0.2.2
- Built: 2025-05-13 09:24:37

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
     - `dist/fabric_api-0.2.2-py3-none-any.whl`

4. **Create a New Notebook**
   - Attach your Lakehouse to the notebook
   - In the first cell, install the package:
     ```python
     %pip install /lakehouse/Files/fabric_api-0.2.2-py3-none-any.whl
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
