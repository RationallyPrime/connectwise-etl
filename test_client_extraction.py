#!/usr/bin/env python3
"""Test ConnectWise client extraction locally without Spark."""

import os
from pathlib import Path

# Load .env file if it exists (Python format)
env_file = Path(".env")
if env_file.exists():
    exec(env_file.read_text())

from connectwise_etl.client import ConnectWiseClient

def test_client_extraction():
    """Test client extraction to catch validation issues early."""

    # Check credentials
    required_vars = ["CW_AUTH_USERNAME", "CW_AUTH_PASSWORD", "CW_CLIENTID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Missing credentials: {missing_vars}")
        print("Set these environment variables first:")
        for var in missing_vars:
            print(f"  export {var}='your-value'")
        return False

    print("✅ Credentials found")

    # Test endpoints one by one
    endpoints = {
        "Agreement": "/finance/agreements",
        "TimeEntry": "/time/entries",
        "Member": "/system/members",
        "Company": "/company/companies",
    }

    client = ConnectWiseClient()

    for entity_name, endpoint in endpoints.items():
        print(f"\n🔍 Testing {entity_name} extraction from {endpoint}")

        try:
            # Get more records to catch validation issues
            raw_data = client.paginate(
                endpoint=endpoint,
                entity_name=entity_name,
                page_size=100,  # Larger sample
                max_pages=3     # Multiple pages
            )

            print(f"  ✅ Raw extraction: {len(raw_data)} records")

            # Test validation
            model_class = client.get_model_class(entity_name)
            validated_count = 0
            error_count = 0

            for i, record in enumerate(raw_data):
                try:
                    validated = model_class(**record)
                    validated_count += 1
                except Exception as e:
                    error_count += 1
                    if error_count <= 3:  # Show first 3 errors
                        print(f"  ❌ Validation error in record {i}: {str(e)[:100]}...")

            print(f"  📊 Validation results: {validated_count} valid, {error_count} errors")

        except Exception as e:
            print(f"  ❌ Extraction failed: {e}")

    return True

if __name__ == "__main__":
    print("🧪 Testing ConnectWise Client Extraction")
    print("=" * 50)
    test_client_extraction()