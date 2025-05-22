"""Test script for validating time entry data against schemas.TimeEntry model.

This script demonstrates the use of the schema-based time entry fetching and validation
as part of the ConnectWise Delta load pipeline improvements.
"""

import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pydantic import ValidationError

# Load environment variables from .env file in the root directory
# This must be done before importing modules that use environment variables
root_dir = Path(__file__).parent.parent
env_path = root_dir / ".env"
load_dotenv()

# Import from parent directory
sys.path.append("..")

from unified_etl.client import ConnectWiseClient
from unified_etl.connectwise_models import TimeEntry
from unified_etl.extract.time import fetch_time_entries_raw

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_time_validation():
    """
    Test the fetch_time_entries_raw function and validate each time entry.

    This function:
    1. Creates a ConnectWiseClient
    2. Calls fetch_time_entries_raw to get raw time entry data
    3. Attempts to validate each time entry using schemas.TimeEntry
    4. Logs success or validation errors
    """
    # Create a client using environment variables loaded from .env
    logger.info("Creating ConnectWiseClient using environment variables from .env")

    # Set up authentication details
    # Default test credentials (these should be replaced with actual credentials)
    default_auth_username = "thekking+yemGyHDPdJ1hpuqx"  # Replace with actual test username
    default_auth_password = "yMqpe26Jcu55FbQk"  # Replace with actual test password
    default_client_id = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"  # Replace with actual client ID

    # Override with environment variables if available
    auth_username = os.getenv("CW_AUTH_USERNAME", default_auth_username)
    auth_password = os.getenv("CW_AUTH_PASSWORD", default_auth_password)
    client_id = os.getenv("CW_CLIENTID", default_client_id)

    logger.info(f"Using auth username: {auth_username}")

    # Create the client with explicit authentication parameters
    client = ConnectWiseClient(
        basic_username=auth_username, basic_password=auth_password, client_id=client_id
    )

    # Optional: Use a small page_size for testing to speed up the process
    page_size = 5
    max_pages = 1

    # Optional: Add conditions for testing specific time entries
    # conditions = "member/identifier='username'"  # Example: only fetch time entries for a specific member

    # Call the function to fetch raw time entries
    logger.info(f"Fetching time entries (page_size={page_size}, max_pages={max_pages})")
    raw_time_entries = fetch_time_entries_raw(
        client=client,
        page_size=page_size,
        max_pages=max_pages,
        # conditions=conditions  # Uncomment to use conditions
    )

    logger.info(f"Retrieved {len(raw_time_entries)} time entries")

    # Dump raw time entries to JSON file for inspection
    output_dir = Path("debug_output")
    output_dir.mkdir(exist_ok=True)
    time_dump_path = output_dir / "raw_time_entries.json"

    with open(time_dump_path, "w") as f:
        json.dump(raw_time_entries, f, indent=2, default=str)

    logger.info(f"Dumped raw time entries to {time_dump_path} for inspection")

    # Validate each time entry
    valid_count = 0
    invalid_count = 0

    for i, raw_time_entry in enumerate(raw_time_entries):
        time_entry_id = raw_time_entry.get("id", f"Unknown-{i}")
        logger.info(f"Validating time entry {time_entry_id} ({i + 1}/{len(raw_time_entries)})")

        # Debug: Print raw keys to see what we're getting from the API

        logger.info(f"Raw time entry keys: {list(raw_time_entry.keys())}")
        if "timeStart" in raw_time_entry:
            logger.info(f"timeStart value: {raw_time_entry['timeStart']}")
        else:
            logger.info("No 'timeStart' key found in raw data")

        try:
            # Attempt to validate using the Pydantic model
            TimeEntry.model_validate(raw_time_entry)
            valid_count += 1
            logger.info(f"✅ SUCCESS: Time entry {time_entry_id} validated successfully")

        except ValidationError as e:
            # Log validation errors with detailed info
            logger.error(
                f"❌ VALIDATION ERROR: Time entry ID {time_entry_id} (index {i + 1}) validation failed"
            )

            # Print the actual data that failed validation to help diagnose the issue
            logger.error(f"Raw data fields: {', '.join(raw_time_entry.keys())}")

            # Print detailed error information
            for error in e.errors():
                field_path = ".".join(str(loc) for loc in error["loc"])
                logger.error(f"  - Field: {field_path}")
                logger.error(f"    Error: {error['msg']}")
                logger.error(f"    Type: {error['type']}")

            # If the field is in the raw data, show its value
            for error in e.errors():
                if len(error["loc"]) == 1 and error["loc"][0] in raw_time_entry:
                    field_name = error["loc"][0]
                    logger.error(f"    Value in raw data: {raw_time_entry[field_name]!r}")
                elif len(error["loc"]) == 1:
                    logger.error("    Field not present in raw data")
            invalid_count += 1
            logger.error(f"❌ ERROR: Time entry {time_entry_id} validation failed")

    # Print summary
    logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid time entries")

    # Return a non-zero exit code if any time entries failed validation
    if invalid_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    # Ensure we're running the script with .env file access
    if not env_path.exists():
        logger.error(f".env file not found at {env_path}")
        logger.error("Please create a .env file with ConnectWise credentials in the root directory")
        sys.exit(1)

    sys.exit(test_time_validation())
