"""Test script for validating agreement data against schemas.Agreement model.

This script demonstrates the use of the schema-based agreement fetching and validation
as part of the ConnectWise Delta load pipeline improvements.
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file in the root directory
# This must be done before importing modules that use environment variables
root_dir = Path(__file__).parent.parent
env_path = root_dir / ".env"
load_dotenv()

# Import from parent directory
sys.path.append("..")

from fabric_api.client import ConnectWiseClient
from fabric_api.extract.agreements import fetch_agreements_raw
from fabric_api.validation import logger as validation_logger
from fabric_api.validation import validate_agreements

# Set up logging - more concise for testing
logging.basicConfig(
    level=logging.WARNING,  # Only show warnings and errors
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configure the validation module to be less verbose
validation_logger.setLevel(logging.WARNING)  # Reduce verbosity of validation


def test_agreement_validation():
    """
    Test the fetch_agreements_raw function and validate each agreement.

    This function:
    1. Creates a ConnectWiseClient
    2. Calls fetch_agreements_raw to get raw agreement data
    3. Attempts to validate each agreement using schemas.Agreement
    4. Logs success or validation errors
    """
    # Create a client using environment variables loaded from .env
    logger.info("Creating ConnectWiseClient using environment variables from .env")

    # Set up authentication details, similar to the approach in test_api.py
    # Default test credentials (from test_api.py example - these should be replaced with actual credentials)
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

    # Optional: Use a small page_size for testing
    page_size = 10
    max_pages = 1

    # Optional: Add conditions for testing specific agreements
    # conditions = "id=12345"  # Example: only fetch a specific agreement

    # Call the new function to fetch raw agreements
    logger.info(f"Fetching agreements (page_size={page_size}, max_pages={max_pages})")
    raw_agreements = fetch_agreements_raw(
        client=client,
        page_size=page_size,
        max_pages=max_pages,
        # conditions=conditions  # Uncomment to use conditions
    )

    logger.info(f"Retrieved {len(raw_agreements)} agreements")

    # Use the validation module to validate the agreements

    # Track missing applicationCycle field specifically
    missing_application_cycle = []

    # First check for applicationCycle issue
    for agreement in raw_agreements:
        if "applicationCycle" not in agreement and "id" in agreement:
            missing_application_cycle.append(agreement["id"])

    if missing_application_cycle:
        logger.warning(
            f"Found {len(missing_application_cycle)} agreements missing 'applicationCycle' field: {missing_application_cycle[:5]}"
            + (" ...and more" if len(missing_application_cycle) > 5 else "")
        )

    # Use the validation module to validate
    validation_result = validate_agreements(raw_agreements)
    valid_count = len(validation_result.valid_objects)
    invalid_count = len(validation_result.errors)

    # Analyze error patterns
    if validation_result.errors:
        error_fields = {}
        for error in validation_result.errors:
            for err in error["errors"]:
                field = ".".join(str(loc) for loc in err["loc"]) if "loc" in err else "unknown"
                error_type = err.get("type", "unknown")
                if field not in error_fields:
                    error_fields[field] = {}
                if error_type not in error_fields[field]:
                    error_fields[field][error_type] = 0
                error_fields[field][error_type] += 1

        # Print a concise summary of error patterns
        logger.warning("Validation error patterns:")
        for field, types in error_fields.items():
            error_counts = ", ".join([f"{t}: {c}" for t, c in types.items()])
            logger.warning(f"  - {field}: {error_counts}")

        # Show a sample error
        if validation_result.errors:
            sample_error = validation_result.errors[0]
            logger.warning(f"Sample error for Agreement ID {sample_error['raw_data_id']}:")
            for err in sample_error["errors"][:3]:  # Show at most 3 errors
                field = ".".join(str(loc) for loc in err["loc"]) if "loc" in err else "unknown"
                logger.warning(f"  - {field}: {err.get('msg', 'unknown error')}")

        if "applicationCycle" in error_fields:
            logger.warning(
                "RECOMMENDATION: The 'applicationCycle' field is required but missing in several records."
            )
            logger.warning("Consider updating the Agreement model or preprocessing the data.")
    else:
        logger.info("All agreements validated successfully. No errors found!")

    # Print summary
    logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid agreements")

    # Return a non-zero exit code if any agreements failed validation
    if invalid_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    # Ensure we're running the script with .env file access
    if not env_path.exists():
        logger.error(f".env file not found at {env_path}")
        logger.error("Please create a .env file with ConnectWise credentials in the root directory")
        sys.exit(1)

    sys.exit(test_agreement_validation())
