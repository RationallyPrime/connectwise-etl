from datetime import datetime

import pandas as pd


def process_fabric_data(data, entity_name="reports"):
    """
    Process data and write directly to Fabric Lakehouse.
    Optimized for running inside Microsoft Fabric notebooks.

    Args:
        data: List of dictionaries from the API
        entity_name: Name of the entity (e.g., "companies", "contacts")

    Returns:
        tuple: (lakehouse_path, dataframe) with the stored path and processed data
    """
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(data)

    # Add extraction timestamp
    df["extraction_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Generate timestamp and filenames
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"connectwise_{entity_name}_{ts}.parquet"

    # In Fabric, we can directly save to the Files folder in the lakehouse
    lakehouse_path = f"/lakehouse/default/Files/raw/connectwise/{file_name}"

    # Write directly to the lakehouse
    print(f"Saving data to lakehouse: {lakehouse_path}")
    df.to_parquet(lakehouse_path, index=False)

    return lakehouse_path, df
