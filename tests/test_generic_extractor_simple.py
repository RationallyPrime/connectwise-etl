"""
Simple integration tests for the generic entity extractor.

This validates that generic.py works correctly for all entity types.
"""

import logging
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load environment variables from .env file in the root directory
root_dir = Path(__file__).parent.parent
env_path = root_dir / ".env"
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import our modules
from unified_etl.client import ConnectWiseClient
from unified_etl.extract import extract_entity


@pytest.fixture
def cw_client():
    """Create a ConnectWiseClient using environment variables."""
    return ConnectWiseClient()


def test_extract_agreement(cw_client):
    """Test Agreement extraction."""
    # Extract agreements
    agreements = extract_entity(client=cw_client, entity_name="Agreement", max_pages=1, page_size=5)

    # Verify we got data back
    assert isinstance(agreements, list)
    if agreements:
        # Check first record has expected fields
        first = agreements[0]
        assert "id" in first
        # Note: _info field might not be included in generic extractor by default

    logger.info(f"Successfully extracted {len(agreements)} agreements")


def test_extract_time_entry(cw_client):
    """Test TimeEntry extraction."""
    # Extract time entries
    time_entries = extract_entity(
        client=cw_client, entity_name="TimeEntry", max_pages=1, page_size=5
    )

    # Verify we got data back
    assert isinstance(time_entries, list)
    if time_entries:
        # Check first record has expected fields
        first = time_entries[0]
        assert "id" in first
        # Note: _info field might not be included in generic extractor by default

    logger.info(f"Successfully extracted {len(time_entries)} time entries")


def test_extract_expense_entry(cw_client):
    """Test ExpenseEntry extraction."""
    # Extract expense entries
    expense_entries = extract_entity(
        client=cw_client, entity_name="ExpenseEntry", max_pages=1, page_size=5
    )

    # Verify we got data back
    assert isinstance(expense_entries, list)
    if expense_entries:
        # Check first record has expected fields
        first = expense_entries[0]
        assert "id" in first
        # Note: _info field might not be included in generic extractor by default

    logger.info(f"Successfully extracted {len(expense_entries)} expense entries")


def test_extract_posted_invoice(cw_client):
    """Test PostedInvoice extraction."""
    # Extract posted invoices
    invoices = extract_entity(
        client=cw_client, entity_name="PostedInvoice", max_pages=1, page_size=5
    )

    # Verify we got data back
    assert isinstance(invoices, list)
    if invoices:
        # Check first record has expected fields
        first = invoices[0]
        assert "id" in first
        # Note: _info field might not be included in generic extractor by default

    logger.info(f"Successfully extracted {len(invoices)} posted invoices")


def test_extract_unposted_invoice(cw_client):
    """Test UnpostedInvoice extraction."""
    # Extract unposted invoices
    invoices = extract_entity(
        client=cw_client, entity_name="UnpostedInvoice", max_pages=1, page_size=5
    )

    # Verify we got data back
    assert isinstance(invoices, list)
    if invoices:
        # Check first record has expected fields
        first = invoices[0]
        assert "id" in first
        # Note: _info field might not be included in generic extractor by default

    logger.info(f"Successfully extracted {len(invoices)} unposted invoices")


def test_extract_product_item(cw_client):
    """Test ProductItem extraction."""
    # Extract product items
    products = extract_entity(client=cw_client, entity_name="ProductItem", max_pages=1, page_size=5)

    # Verify we got data back
    assert isinstance(products, list)
    if products:
        # Check first record has expected fields
        first = products[0]
        assert "id" in first
        # Note: _info field might not be included in generic extractor by default

    logger.info(f"Successfully extracted {len(products)} product items")


def test_validation_functionality(cw_client):
    """Test the validation functionality of the generic extractor."""
    # Extract TimeEntries with validation
    valid_entries, errors = extract_entity(
        client=cw_client, entity_name="TimeEntry", return_validated=True, max_pages=1, page_size=10
    )

    # Log results for debugging
    logger.info(f"Valid entries: {len(valid_entries)}, Errors: {len(errors)}")

    # Check that validation is working
    if errors:
        # Examine the validation errors
        first_error = errors[0]
        logger.info(f"Sample validation error: {first_error}")
        assert "errors" in first_error
        assert "entity" in first_error
        assert first_error["entity"] == "TimeEntry"

    # At least some valid entries should exist
    assert isinstance(valid_entries, list)
    assert all(hasattr(entry, "id") for entry in valid_entries), (
        "Valid entries should have id field"
    )


if __name__ == "__main__":
    pytest.main(["-v", __file__])
