"""Test script for validating invoice data against schemas models.

This script demonstrates the use of the schema-based invoice fetching and validation
as part of the ConnectWise Delta load pipeline improvements.
"""

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
from unified_etl.connectwise_models import PostedInvoice, UnpostedInvoice
from unified_etl.extract.invoices import fetch_posted_invoices_raw, fetch_unposted_invoices_raw

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_posted_invoice_validation():
    """
    Test the fetch_posted_invoices_raw function and validate each invoice.

    This function:
    1. Creates a ConnectWiseClient
    2. Calls fetch_posted_invoices_raw to get raw invoice data
    3. Attempts to validate each invoice using schemas.Invoice
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
    page_size = 5
    max_pages = 1

    # Call the function to fetch raw posted invoices
    logger.info(f"Fetching posted invoices (page_size={page_size}, max_pages={max_pages})")
    raw_invoices = fetch_posted_invoices_raw(
        client=client, page_size=page_size, max_pages=max_pages
    )

    logger.info(f"Retrieved {len(raw_invoices)} posted invoices")

    # Validate each invoice
    valid_count = 0
    invalid_count = 0

    for i, raw_invoice in enumerate(raw_invoices):
        raw_invoice.get("id", f"Unknown-{i}")
        invoice_number = raw_invoice.get("identifier", f"Unknown-{i}")
        logger.info(f"Validating posted invoice {invoice_number} ({i + 1}/{len(raw_invoices)})")

        try:
            # Attempt to validate using the Pydantic model
            PostedInvoice.model_validate(raw_invoice)
            valid_count += 1
            logger.info(f"✅ SUCCESS: Posted invoice {invoice_number} validated successfully")

        except ValidationError as e:
            invalid_count += 1
            logger.error(f"❌ ERROR: Posted invoice {invoice_number} validation failed")

            # Print detailed validation errors
            for error in e.errors():
                location = ".".join(str(loc) for loc in error["loc"])
                logger.error(f"  - Field: {location}")
                logger.error(f"    Error: {error['msg']}")
                logger.error(f"    Type: {error['type']}")

    # Print summary
    logger.info(
        f"Validation complete: {valid_count} valid, {invalid_count} invalid posted invoices"
    )

    # Return a non-zero exit code if any invoices failed validation
    if invalid_count > 0:
        return 1
    return 0


def test_unposted_invoice_validation():
    """
    Test the fetch_unposted_invoices_raw function and validate each unposted invoice.

    This function:
    1. Creates a ConnectWiseClient
    2. Calls fetch_unposted_invoices_raw to get raw unposted invoice data
    3. Attempts to validate each unposted invoice using schemas.UnpostedInvoice
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
    page_size = 5
    max_pages = 1

    # Call the function to fetch raw unposted invoices
    logger.info(f"Fetching unposted invoices (page_size={page_size}, max_pages={max_pages})")
    raw_unposted_invoices = fetch_unposted_invoices_raw(
        client=client, page_size=page_size, max_pages=max_pages
    )

    logger.info(f"Retrieved {len(raw_unposted_invoices)} unposted invoices")

    # Validate each unposted invoice
    valid_count = 0
    invalid_count = 0

    for i, raw_invoice in enumerate(raw_unposted_invoices):
        raw_invoice.get("id", f"Unknown-{i}")
        invoice_number = raw_invoice.get("identifier", f"Unknown-{i}")
        logger.info(
            f"Validating unposted invoice {invoice_number} ({i + 1}/{len(raw_unposted_invoices)})"
        )

        try:
            # Attempt to validate using the Pydantic model
            UnpostedInvoice.model_validate(raw_invoice)
            valid_count += 1
            logger.info(f"✅ SUCCESS: Unposted invoice {invoice_number} validated successfully")

        except ValidationError as e:
            invalid_count += 1
            logger.error(f"❌ ERROR: Unposted invoice {invoice_number} validation failed")

            # Print detailed validation errors
            for error in e.errors():
                location = ".".join(str(loc) for loc in error["loc"])
                logger.error(f"  - Field: {location}")
                logger.error(f"    Error: {error['msg']}")
                logger.error(f"    Type: {error['type']}")

    # Print summary
    logger.info(
        f"Validation complete: {valid_count} valid, {invalid_count} invalid unposted invoices"
    )

    # Return a non-zero exit code if any invoices failed validation
    if invalid_count > 0:
        return 1
    return 0


if __name__ == "__main__":
    # Ensure we're running the script with .env file access
    if not env_path.exists():
        logger.error(f".env file not found at {env_path}")
        logger.error("Please create a .env file with ConnectWise credentials in the root directory")
        sys.exit(1)

    # Run both test functions
    posted_result = test_posted_invoice_validation()
    unposted_result = test_unposted_invoice_validation()

    # Exit with error if either test failed
    sys.exit(posted_result or unposted_result)
