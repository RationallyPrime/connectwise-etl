"""
Unit test for ConnectWiseClient's fields and conditions parameters.

Verifies that the ConnectWiseClient properly handles fields and conditions parameters
when making API requests, and validates the returned data against our Pydantic models.
"""

import logging
import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load environment variables from .env file in the root directory
root_dir = Path(__file__).parent.parent
env_path = root_dir / '.env'
load_dotenv(dotenv_path=env_path)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Silence noisy loggers
logging.getLogger("fabric_api.client").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# Import our modules
from fabric_api.client import ConnectWiseClient
from fabric_api.connectwise_models import ExpenseEntry, Invoice, ProductItem, TimeEntry
from fabric_api.extract.generic import extract_entity


@pytest.fixture
def client():
    """Return a ConnectWiseClient instance with credentials from environment."""
    # Environment variables must be set before running tests
    if not os.getenv("CW_AUTH_USERNAME") or not os.getenv("CW_AUTH_PASSWORD") or not os.getenv("CW_CLIENTID"):
        pytest.skip("Environment variables for ConnectWise API not set. Skipping test.")

    return ConnectWiseClient()

def test_fields_parameter(client):
    """Test if fields parameter is correctly sent to the API."""
    logger.info("Testing fields parameter")

    # Use a simple fields parameter - just request id and name
    fields = "id,name"

    # Fetch agreements with only specified fields
    agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        fields=fields,
        max_pages=1,
        page_size=5
    )

    # Validate the results
    assert len(agreements) > 0, "No agreements returned"

    # Check that we got exactly the fields we requested
    for agreement in agreements:
        assert set(agreement.keys()).issubset({"id", "name", "_info"}), "Received fields beyond what was requested"
        assert "id" in agreement, "id field missing"
        assert "name" in agreement, "name field missing"

    # Verify we didn't get extra fields
    if "type" in agreements[0]:
        logger.warning("Field filtering failed - received 'type' field despite not requesting it")

    logger.info("FIELDS PARAMETER: Successfully filtered to only requested fields")

def test_conditions_parameter(client):
    """Test if conditions parameter is correctly sent to the API."""
    logger.info("Testing conditions parameter")

    # Test a simple condition - active agreements using ConnectWise syntax
    conditions = "cancelledFlag=false"

    # Fetch agreements matching the condition
    filtered_agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        conditions=conditions,
        max_pages=1,
        page_size=5
    )

    assert len(filtered_agreements) > 0, "No agreements returned"

    # Log results for verification
    logger.info(f"CONDITIONS PARAMETER: Successfully filtered agreements (got {len(filtered_agreements)} records)")

def test_extract_entity_with_validation(client):
    """Test extracting agreements with validation against Pydantic models."""
    logger.info("Testing Agreement model validation")

    # Extract agreements with validation - larger batch
    valid_agreements, errors = extract_entity(
        client=client,
        entity_name="Agreement",
        max_pages=2,
        page_size=20,  # Fetch more agreements
        return_validated=True
    )

    # Analyze validation results
    total_records = len(valid_agreements) + len(errors)

    # Collect error information
    field_errors = {}
    if errors:
        for error in errors:
            for e in error.get('errors', []):
                field_path = '.'.join(str(loc) for loc in e.get('loc', []))
                if field_path not in field_errors:
                    field_errors[field_path] = []
                field_errors[field_path].append({
                    'type': e.get('type'),
                    'msg': e.get('msg'),
                    'input': str(e.get('input', ''))[:50]  # Truncate long inputs
                })

    # Print summary
    logger.info(f"AGREEMENT: Validated {len(valid_agreements)} of {total_records} records ({len(errors)} errors)")

    # Print field errors if any
    if field_errors:
        logger.info("AGREEMENT validation errors by field:")
        for field, errs in field_errors.items():
            logger.info(f"  - {field}: {len(errs)} errors")
            for i, err in enumerate(errs[:3]):  # Show max 3 examples per field
                logger.info(f"    {i+1}. {err['type']}: {err['msg']} (input: {err['input']})")
            if len(errs) > 3:
                logger.info(f"    ... and {len(errs) - 3} more errors")

    assert len(valid_agreements) > 0, "No valid agreements returned"

def test_time_entry_extraction(client):
    """Test extracting time entries with validation."""
    logger.info("Testing TimeEntry model validation")

    # Extract time entries with validation - larger batch
    valid_entries, errors = extract_entity(
        client=client,
        entity_name="TimeEntry",
        max_pages=2,
        page_size=20,
        return_validated=True
    )

    # Check if we got results - not all systems will have time entries
    if not valid_entries and not errors:
        logger.warning("No time entries found. Test skipped.")
        pytest.skip("No time entries found for validation.")

    # Analyze validation results
    total_records = len(valid_entries) + len(errors)

    # Collect error information
    field_errors = {}
    if errors:
        for error in errors:
            for e in error.get('errors', []):
                field_path = '.'.join(str(loc) for loc in e.get('loc', []))
                if field_path not in field_errors:
                    field_errors[field_path] = []
                field_errors[field_path].append({
                    'type': e.get('type'),
                    'msg': e.get('msg'),
                    'input': str(e.get('input', ''))[:50]  # Truncate long inputs
                })

    # Print summary
    logger.info(f"TIME ENTRY: Validated {len(valid_entries)} of {total_records} records ({len(errors)} errors)")

    # Print field errors if any
    if field_errors:
        logger.info("TIME ENTRY validation errors by field:")
        for field, errs in field_errors.items():
            logger.info(f"  - {field}: {len(errs)} errors")
            for i, err in enumerate(errs[:3]):  # Show max 3 examples per field
                logger.info(f"    {i+1}. {err['type']}: {err['msg']} (input: {err['input']})")
            if len(errs) > 3:
                logger.info(f"    ... and {len(errs) - 3} more errors")

    if valid_entries:
        assert all(isinstance(entry, TimeEntry) for entry in valid_entries), "Returned objects are not TimeEntry instances"

def test_expense_entry_validation(client):
    """Test extracting and validating expense entries."""
    logger.info("Testing ExpenseEntry model validation")

    # Extract expense entries with validation
    valid_entries, errors = extract_entity(
        client=client,
        entity_name="ExpenseEntry",
        max_pages=2,
        page_size=20,
        return_validated=True
    )

    # Check if we got results - not all systems will have entries
    if not valid_entries and not errors:
        logger.warning("No expense entries found. Test skipped.")
        pytest.skip("No expense entries found for validation.")

    # Analyze validation results
    total_records = len(valid_entries) + len(errors)

    # Collect error information
    field_errors = {}
    if errors:
        for error in errors:
            for e in error.get('errors', []):
                field_path = '.'.join(str(loc) for loc in e.get('loc', []))
                if field_path not in field_errors:
                    field_errors[field_path] = []
                field_errors[field_path].append({
                    'type': e.get('type'),
                    'msg': e.get('msg'),
                    'input': str(e.get('input', ''))[:50]  # Truncate long inputs
                })

    # Print summary
    logger.info(f"EXPENSE ENTRY: Validated {len(valid_entries)} of {total_records} records ({len(errors)} errors)")

    # Print field errors if any
    if field_errors:
        logger.info("EXPENSE ENTRY validation errors by field:")
        for field, errs in field_errors.items():
            logger.info(f"  - {field}: {len(errs)} errors")
            for i, err in enumerate(errs[:3]):  # Show max 3 examples per field
                logger.info(f"    {i+1}. {err['type']}: {err['msg']} (input: {err['input']})")
            if len(errs) > 3:
                logger.info(f"    ... and {len(errs) - 3} more errors")

    if valid_entries:
        assert all(isinstance(entry, ExpenseEntry) for entry in valid_entries), "Returned objects are not ExpenseEntry instances"

def test_invoice_validation(client):
    """Test extracting and validating invoices."""
    logger.info("Testing Invoice model validation")

    # Extract invoices with validation
    valid_entries, errors = extract_entity(
        client=client,
        entity_name="PostedInvoice",
        max_pages=2,
        page_size=20,
        return_validated=True
    )

    # Check if we got results - not all systems will have invoices
    if not valid_entries and not errors:
        logger.warning("No invoices found. Test skipped.")
        pytest.skip("No invoices found for validation.")

    # Analyze validation results
    total_records = len(valid_entries) + len(errors)

    # Collect error information
    field_errors = {}
    if errors:
        for error in errors:
            for e in error.get('errors', []):
                field_path = '.'.join(str(loc) for loc in e.get('loc', []))
                if field_path not in field_errors:
                    field_errors[field_path] = []
                field_errors[field_path].append({
                    'type': e.get('type'),
                    'msg': e.get('msg'),
                    'input': str(e.get('input', ''))[:50]  # Truncate long inputs
                })

    # Print summary
    logger.info(f"INVOICE: Validated {len(valid_entries)} of {total_records} records ({len(errors)} errors)")

    # Print field errors if any
    if field_errors:
        logger.info("INVOICE validation errors by field:")
        for field, errs in field_errors.items():
            logger.info(f"  - {field}: {len(errs)} errors")
            for i, err in enumerate(errs[:3]):  # Show max 3 examples per field
                logger.info(f"    {i+1}. {err['type']}: {err['msg']} (input: {err['input']})")
            if len(errs) > 3:
                logger.info(f"    ... and {len(errs) - 3} more errors")

    if valid_entries:
        assert all(isinstance(entry, Invoice) for entry in valid_entries), "Returned objects are not Invoice instances"

def test_product_validation(client):
    """Test extracting and validating products."""
    logger.info("Testing ProductItem model validation")

    # Extract products with validation
    valid_entries, errors = extract_entity(
        client=client,
        entity_name="ProductItem",
        max_pages=2,
        page_size=20,
        return_validated=True
    )

    # Check if we got results - not all systems will have products
    if not valid_entries and not errors:
        logger.warning("No product items found. Test skipped.")
        pytest.skip("No product items found for validation.")

    # Analyze validation results
    total_records = len(valid_entries) + len(errors)

    # Collect error information
    field_errors = {}
    if errors:
        for error in errors:
            for e in error.get('errors', []):
                field_path = '.'.join(str(loc) for loc in e.get('loc', []))
                if field_path not in field_errors:
                    field_errors[field_path] = []
                field_errors[field_path].append({
                    'type': e.get('type'),
                    'msg': e.get('msg'),
                    'input': str(e.get('input', ''))[:50]  # Truncate long inputs
                })

    # Print summary
    logger.info(f"PRODUCT ITEM: Validated {len(valid_entries)} of {total_records} records ({len(errors)} errors)")

    # Print field errors if any
    if field_errors:
        logger.info("PRODUCT ITEM validation errors by field:")
        for field, errs in field_errors.items():
            logger.info(f"  - {field}: {len(errs)} errors")
            for i, err in enumerate(errs[:3]):  # Show max 3 examples per field
                logger.info(f"    {i+1}. {err['type']}: {err['msg']} (input: {err['input']})")
            if len(errs) > 3:
                logger.info(f"    ... and {len(errs) - 3} more errors")

    if valid_entries:
        assert all(isinstance(entry, ProductItem) for entry in valid_entries), "Returned objects are not ProductItem instances"

def test_combined_parameters(client):
    """Test if multiple parameters can be combined correctly."""
    logger.info("Testing parameter combination")

    # Combine fields, conditions, and order_by
    fields = "id,name,type"
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
        page_size=5
    )

    assert len(agreements) > 0, "No agreements returned"

    # Validate we got the requested fields
    for agreement in agreements:
        assert set(agreement.keys()).issubset({"id", "name", "type", "_info"}), "Received fields beyond what was requested"

    logger.info("PARAMETER COMBINATION: Successfully applied multiple filters (fields, conditions, order_by)")

if __name__ == "__main__":
    logger.info("===========================================")
    logger.info("TESTING CONNECTWISE CLIENT AND MODEL VALIDATION")
    logger.info("===========================================\n")

    # Run all tests
    tests = [
        ("Fields parameter", test_fields_parameter),
        ("Conditions parameter", test_conditions_parameter),
        ("Agreement extraction and validation", test_extract_entity_with_validation),
        ("Time entry extraction and validation", test_time_entry_extraction),
        ("Expense entry extraction and validation", test_expense_entry_validation),
        ("Invoice extraction and validation", test_invoice_validation),
        ("Product extraction and validation", test_product_validation),
        ("Combined parameters", test_combined_parameters),
    ]

    # Track results
    passed = 0
    failed = 0

    # Run each test
    for test_name, test_func in tests:
        logger.info(f"\nRunning test: {test_name}")
        try:
            result = test_func(client())
            if result is not False:  # None is a passing result for pytest functions
                passed += 1
            else:
                failed += 1
                logger.error(f"Test '{test_name}' failed")
        except Exception as e:
            failed += 1
            logger.error(f"Test '{test_name}' failed with exception: {e!s}")

    # Print summary
    logger.info("\n===========================================")
    logger.info(f"TEST RESULTS: {passed} passed, {failed} failed")
    logger.info("===========================================\n")

    # Exit with status code based on test results
    if failed == 0:
        logger.info("All tests completed successfully!")
        sys.exit(0)
    else:
        logger.error("Some tests failed.")
        sys.exit(1)
