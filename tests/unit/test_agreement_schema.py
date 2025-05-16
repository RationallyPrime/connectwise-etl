"""Script to examine the agreement endpoint schema."""

import json
import os
import sys

from fabric_api.client import ConnectWiseClient


def get_agreement_schema():
    """Get and print the agreement schema from a real API call."""
    # Initialize client
    client = ConnectWiseClient(
        basic_username=os.getenv("CW_AUTH_USERNAME"),
        basic_password=os.getenv("CW_AUTH_PASSWORD"),
        client_id=os.getenv("CW_CLIENTID"),
    )

    # Try to get a specific agreement first
    try:
        agreement_id = int(sys.argv[1]) if len(sys.argv) > 1 else None

        if agreement_id:
            print(f"Fetching agreement {agreement_id}...")
            response = client.get(f"/finance/agreements/{agreement_id}")
            agreement = response.json()

            # Print the raw JSON for inspection
            print("\nAgreement JSON:")
            print(json.dumps(agreement, indent=2))

            # Also print the structure for model creation
            print("\nField structure for Pydantic model:")
            for key, value in agreement.items():
                if isinstance(value, dict):
                    print(f"{key}: dict = {value}")
                elif isinstance(value, list):
                    print(f"{key}: list = {value}")
                else:
                    value_type = type(value).__name__
                    print(f"{key}: {value_type} = {value}")

        # Get all agreements to see structure variation
        print("\nFetching first 5 agreements to check structure consistency...")
        agreements = client.paginate(
            endpoint="/finance/agreements", entity_name="agreements", max_pages=1
        )[:5]

        # Check if all agreements have the same fields
        all_fields = set()
        for agreement in agreements:
            all_fields.update(agreement.keys())

        print(f"\nAll possible fields across agreements: {sorted(all_fields)}")

        # Save a sample agreement to a file for reference
        if agreements:
            with open("sample_agreement.json", "w") as f:
                json.dump(agreements[0], indent=2, fp=f)
            print("\nSaved first agreement to sample_agreement.json")

    except Exception as e:
        print(f"Error: {e!s}")


if __name__ == "__main__":
    get_agreement_schema()
