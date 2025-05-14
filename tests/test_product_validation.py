"""Test script for validating product item data against schemas.ProductItem model.

This script demonstrates the use of the schema-based product item fetching and validation
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
env_path = root_dir / '.env'
load_dotenv()

# Import from parent directory
sys.path.append('..')
from fabric_api.connectwise_models import ProductItem
from fabric_api.client import ConnectWiseClient
from fabric_api.extract.products import fetch_product_items_raw

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_product_validation():
    """
    Test the fetch_product_items_raw function and validate each product item.

    This function:
    1. Creates a ConnectWiseClient
    2. Calls fetch_product_items_raw to get raw product item data
    3. Attempts to validate each product item using schemas.ProductItem
    4. Logs success or validation errors
    """
    # Create a client using environment variables loaded from .env
    logger.info("Creating ConnectWiseClient using environment variables from .env")

    # Set up authentication details
    # Default test credentials (these should be replaced with actual credentials)
    default_auth_username = "thekking+yemGyHDPdJ1hpuqx"  # Replace with actual test username
    default_auth_password = "yMqpe26Jcu55FbQk"          # Replace with actual test password
    default_client_id = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"  # Replace with actual client ID

    # Override with environment variables if available
    auth_username = os.getenv("CW_AUTH_USERNAME", default_auth_username)
    auth_password = os.getenv("CW_AUTH_PASSWORD", default_auth_password)
    client_id = os.getenv("CW_CLIENTID", default_client_id)

    logger.info(f"Using auth username: {auth_username}")

    # Create the client with explicit authentication parameters
    client = ConnectWiseClient(
        basic_username=auth_username,
        basic_password=auth_password,
        client_id=client_id
    )

    # Optional: Use a small page_size for testing to speed up the process
    page_size = 5
    max_pages = 1

    # Optional: Add conditions for testing specific products
    # conditions = "catalogItem/identifier='ProductCode'"  # Example: only fetch products with a specific code

    # Call the function to fetch raw product items
    logger.info(f"Fetching product items (page_size={page_size}, max_pages={max_pages})")
    raw_products = fetch_product_items_raw(
        client=client,
        page_size=page_size,
        max_pages=max_pages,
        # conditions=conditions  # Uncomment to use conditions
    )

    logger.info(f"Retrieved {len(raw_products)} product items")

    # Dump raw products to JSON file for inspection
    output_dir = Path("debug_output")
    output_dir.mkdir(exist_ok=True)
    product_dump_path = output_dir / "raw_products.json"

    with open(product_dump_path, "w") as f:
        json.dump(raw_products, f, indent=2, default=str)

    logger.info(f"Dumped raw products to {product_dump_path} for inspection")

    # Track validation success/failure
    valid_count = 0
    invalid_count = 0

    # Skip field logging as it's not needed
    logger.info("Validating product items...")

    # Validate each product item
    for i, item in enumerate(raw_products):
        product_id = item.get("id", f"Unknown-{i}")
        product_name = item.get("description", f"Unknown-{i}")
        logger.info(f"Validating product item {product_id} ({i+1}/{len(raw_products)})")

        try:
            # Validate against the schema
            ProductItem.model_validate(item)
            valid_count += 1
            logger.info(f"✅ SUCCESS: Product item {product_id} ({product_name}) validated successfully")

        except ValidationError as e:
            invalid_count += 1
            logger.error(f"❌ ERROR: Product item {product_id} ({product_name}) validation failed")

            # Print detailed validation errors
            for error in e.errors():
                location = ".".join(str(loc) for loc in error["loc"])
                logger.error(f"  - Field: {location}")
                logger.error(f"    Error: {error['msg']}")
                logger.error(f"    Type: {error['type']}")

            # Print more specific error details for debugging
            try:
                if 'id' in item:
                    logger.error(f"  Detailed error for Product ID {item['id']}")
                logger.error(f"  Raw error: {e!s}")
            except Exception as debug_error:
                logger.error(f"  Error during debug: {debug_error}")

    # Print summary
    logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid product items")

    # Return a non-zero exit code if any product items failed validation
    if invalid_count > 0:
        return 1
    return 0

if __name__ == "__main__":
    # Ensure we're running the script with .env file access
    if not env_path.exists():
        logger.error(f".env file not found at {env_path}")
        logger.error("Please create a .env file with ConnectWise credentials in the root directory")
        sys.exit(1)

    sys.exit(test_product_validation())
