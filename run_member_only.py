#!/usr/bin/env python3
"""Run ETL pipeline for Member entity only."""

import os
from pathlib import Path

# Load .env
env_file = Path(".env")
if env_file.exists():
    exec(env_file.read_text())

from src.connectwise_etl.client import ConnectWiseClient
from src.connectwise_etl.models import Member
from src.connectwise_etl.main import get_table_name

def run_member_extraction():
    """Extract only Member data to Bronze layer."""

    # Rebuild model
    Member.model_rebuild()

    # Initialize client
    client = ConnectWiseClient()

    # Extract Member data
    print("Extracting Member data...")
    df = client.extract(
        endpoint="/system/members",
        model_class=Member,
        page_size=100,
        max_pages=None  # Get all
    )

    # Get table name
    table_name = get_table_name("bronze", "member")

    # Write to table (this would need Spark context in Fabric)
    print(f"Would write {df.count()} records to {table_name}")

    return df

if __name__ == "__main__":
    df = run_member_extraction()
    print(f"Extraction complete: {df.count()} members")