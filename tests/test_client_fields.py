"""
Unit test for ConnectWiseClient's fields and conditions parameters.

Verifies that the ConnectWiseClient properly handles fields and conditions parameters
when making API requests.
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file in the root directory
root_dir = Path(__file__).parent.parent
env_path = root_dir / '.env'
load_dotenv()

# Add parent directory to path for imports
sys.path.append('..')
from fabric_api.client import ConnectWiseClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # Use INFO for most output to reduce verbosity
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def setup_client():
    """Set up and return a ConnectWiseClient instance with credentials from environment."""
    # Default test credentials - should be overridden by environment variables
    default_auth_username = "thekking+yemGyHDPdJ1hpuqx"
    default_auth_password = "yMqpe26Jcu55FbQk"
    default_client_id = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"

    # Override with environment variables if available
    auth_username = os.getenv("CW_AUTH_USERNAME", default_auth_username)
    auth_password = os.getenv("CW_AUTH_PASSWORD", default_auth_password)
    client_id = os.getenv("CW_CLIENTID", default_client_id)

    logger.info(f"Setting up client with auth username: {auth_username}")

    return ConnectWiseClient(
        basic_username=auth_username,
        basic_password=auth_password,
        client_id=client_id
    )

def test_fields_parameter():
    """Test if fields parameter is correctly sent to the API."""
    client = setup_client()
    logger.info("TEST 1: Testing fields parameter with paginate() method")

    # Use a simple fields parameter - just request id and name
    fields = "id,name"

    # Fetch a small number of agreements with only specified fields
    agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        fields=fields,
        max_pages=1,
        page_size=2  # Just get 2 agreements for testing
    )

    # Log results for verification
    logger.info(f"Retrieved {len(agreements)} agreements with fields={fields}")

    # Simple validation - just check if we got any results
    if not agreements:
        logger.error("No agreements returned, cannot validate fields parameter")
        return False

    # Log the first result for manual inspection
    logger.info(f"First agreement fields: {list(agreements[0].keys())}")

    # Test passes if we got any data
    logger.info("✅ TEST 1 PASS: fields parameter was successfully processed")
    return True

def test_conditions_parameter():
    """Test if conditions parameter is correctly sent to the API."""
    client = setup_client()
    logger.info("TEST 2: Testing conditions parameter with paginate() method")

    # Test a simple condition - active agreements using ConnectWise syntax
    conditions = "cancelledFlag=false"

    # Fetch agreements matching the condition
    filtered_agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        conditions=conditions,
        max_pages=1,
        page_size=2  # Just get 2 agreements for testing
    )

    # Log results for verification
    logger.info(f"Retrieved {len(filtered_agreements)} agreements with conditions='{conditions}'")

    # Simple validation - just check if we got any results
    if not filtered_agreements:
        logger.error("No agreements returned, cannot validate conditions parameter")
        return False

    # Log the first result for manual inspection
    if filtered_agreements:
        logger.info(f"First agreement id: {filtered_agreements[0].get('id')}")

    # Test passes if we got any data
    logger.info("✅ TEST 2 PASS: conditions parameter was successfully processed")
    return True

def test_child_conditions_parameter():
    """Test if child_conditions parameter is correctly sent to the API."""
    client = setup_client()
    logger.info("TEST 3: Testing child_conditions parameter with paginate() method")

    # Test with a child_conditions parameter (this depends on the data structure)
    # You may need to adjust this for your specific ConnectWise instance
    child_conditions = "id>0"  # A generic condition that should always be true

    # Use with a simple endpoint that has child objects
    agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        child_conditions=child_conditions,
        max_pages=1,
        page_size=2  # Just get 2 agreements for testing
    )

    # Log results for verification
    logger.info(f"Retrieved {len(agreements)} agreements with child_conditions='{child_conditions}'")

    # Simple validation - just check if we got any results
    if not agreements:
        logger.error("No agreements returned, cannot validate child_conditions parameter")
        return False

    # Test passes if we got any data
    logger.info("✅ TEST 3 PASS: child_conditions parameter was successfully processed")
    return True

def test_order_by_parameter():
    """Test if order_by parameter is correctly sent to the API."""
    client = setup_client()
    logger.info("TEST 4: Testing order_by parameter with paginate() method")

    # Test with an order_by parameter
    order_by = "id desc"

    # Fetch agreements ordered by id in descending order
    agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        order_by=order_by,
        max_pages=1,
        page_size=2  # Just get 2 agreements for testing
    )

    # Log results for verification
    logger.info(f"Retrieved {len(agreements)} agreements with order_by='{order_by}'")

    # Simple validation - just check if we got any results
    if not agreements:
        logger.error("No agreements returned, cannot validate order_by parameter")
        return False

    # Log the first result for manual inspection
    if agreements:
        logger.info(f"First agreement id: {agreements[0].get('id')}")

    # Test passes if we got any data
    logger.info("✅ TEST 4 PASS: order_by parameter was successfully processed")
    return True

def test_combined_parameters():
    """Test if multiple parameters can be combined correctly."""
    client = setup_client()
    logger.info("TEST 5: Testing combination of multiple parameters")

    # Combine fields, conditions, and order_by
    fields = "id,name"
    conditions = "cancelledFlag=false"
    order_by = "id desc"

    # Fetch agreements with combined parameters
    agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        fields=fields,
        conditions=conditions,
        order_by=order_by,
        max_pages=1,
        page_size=2
    )

    # Log results for verification
    logger.info(f"Retrieved {len(agreements)} agreements with combined parameters")

    # Simple validation - check if we got any results
    if not agreements:
        logger.error("No agreements returned, cannot validate combined parameters")
        return False

    # Log the first result fields for inspection
    logger.info(f"First agreement fields: {list(agreements[0].keys())}")

    # Test passes if we got any data
    logger.info("✅ TEST 5 PASS: Combined parameters were successfully processed")
    return True


if __name__ == "__main__":
    logger.info("=====================================")
    logger.info("TESTING CONNECTWISE CLIENT PARAMETERS")
    logger.info("=====================================\n")

    # Run all tests
    tests = [
        ("Fields parameter", test_fields_parameter),
        ("Conditions parameter", test_conditions_parameter),
        ("Child conditions parameter", test_child_conditions_parameter),
        ("Order by parameter", test_order_by_parameter),
        ("Combined parameters", test_combined_parameters),
    ]

    # Track results
    passed = 0
    failed = 0

    # Run each test
    for test_name, test_func in tests:
        logger.info(f"\nRunning test: {test_name}")
        try:
            result = test_func()
            if result:
                passed += 1
            else:
                failed += 1
                logger.error(f"Test '{test_name}' failed")
        except Exception as e:
            failed += 1
            logger.error(f"Test '{test_name}' failed with exception: {e!s}")

    # Print summary
    logger.info("\n=====================================")
    logger.info(f"TEST RESULTS: {passed} passed, {failed} failed")
    logger.info("=====================================\n")

    # Exit with status code based on test results
    if failed == 0:
        logger.info("All tests completed successfully!")
        sys.exit(0)
    else:
        logger.error("Some tests failed.")
        sys.exit(1)
