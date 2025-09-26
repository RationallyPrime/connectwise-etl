#!/usr/bin/env python3
"""Fetch fresh OpenAPI schema from ConnectWise API."""

import os
import json
from pathlib import Path

# Load .env file if it exists (Python format)
env_file = Path(".env")
if env_file.exists():
    exec(env_file.read_text())

import requests

def fetch_openapi_schema():
    """Fetch the OpenAPI schema directly from ConnectWise."""

    base_url = os.getenv("CW_BASE_URL", "https://verk.thekking.is/v4_6_release/apis/3.0")

    # Try common OpenAPI endpoints
    openapi_endpoints = [
        "/openapi",
        "/openapi.json",
        "/swagger.json",
        "/api-docs",
        "/docs/openapi.json",
        "/v3/api-docs",
        "/swagger/v1/swagger.json"
    ]

    headers = {
        "clientId": os.environ["CW_CLIENTID"],
        "Accept": "application/json",
    }

    auth = (os.environ["CW_AUTH_USERNAME"], os.environ["CW_AUTH_PASSWORD"])

    for endpoint in openapi_endpoints:
        url = f"{base_url.rstrip('/')}{endpoint}"
        print(f"Trying: {url}")

        try:
            resp = requests.get(url, headers=headers, auth=auth, timeout=10)
            if resp.status_code == 200:
                print(f"✅ Found OpenAPI schema at {url}")

                # Save the schema
                schema = resp.json()

                # Check if it's actually an OpenAPI schema
                if 'openapi' in schema or 'swagger' in schema:
                    filename = "PSA_OpenAPI_schema_fresh.json"
                    with open(filename, 'w') as f:
                        json.dump(schema, f, indent=2)

                    print(f"💾 Saved fresh schema to {filename}")
                    print(f"Schema version: {schema.get('openapi', schema.get('swagger'))}")

                    # Check the CustomFieldValue definition
                    if 'components' in schema and 'schemas' in schema['components']:
                        if 'CustomFieldValue' in schema['components']['schemas']:
                            cfv = schema['components']['schemas']['CustomFieldValue']
                            print("\n📋 CustomFieldValue.value definition:")
                            if 'properties' in cfv and 'value' in cfv['properties']:
                                print(json.dumps(cfv['properties']['value'], indent=2))

                    return True
                else:
                    print(f"  Response doesn't look like OpenAPI schema")
            else:
                print(f"  {resp.status_code}: {resp.reason}")
        except Exception as e:
            print(f"  Failed: {e}")

    print("\n❌ Could not find OpenAPI schema endpoint")
    print("\nTrying to get API documentation info...")

    # Try root endpoint for API info
    try:
        resp = requests.get(base_url, headers=headers, auth=auth)
        if resp.status_code == 200:
            print(f"API root response: {resp.text[:500]}")
    except Exception as e:
        print(f"Failed to get API info: {e}")

    return False

if __name__ == "__main__":
    fetch_openapi_schema()