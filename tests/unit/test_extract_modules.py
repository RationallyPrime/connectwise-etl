#!/usr/bin/env python
"""
Test script to verify field selection in extract modules.
"""

import logging

from fabric_api.client import ConnectWiseClient
from fabric_api.extract import agreements, expenses, invoices, products, time

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_schema_driven_field_selection():
    """
    Test the enhanced schema-driven field selection in extract modules.
    Note: This test does not actually call the API (max_pages=0), it just prints the fields.
    """
    logger.info("Testing schema-driven field selection in extract modules")

    # Create a client (no API calls will be made with max_pages=0)
    client = ConnectWiseClient()

    # Test agreements
    logger.info("Testing agreements field selection")
    agreements.fetch_agreements_raw(client, max_pages=0)

    # Test time entries
    logger.info("\nTesting time entries field selection")
    time.fetch_time_entries_raw(client, max_pages=0)

    # Test unposted invoices
    logger.info("\nTesting unposted invoices field selection")
    invoices.fetch_unposted_invoices_raw(client, max_pages=0)

    # Test posted invoices
    logger.info("\nTesting posted invoices field selection")
    invoices.fetch_posted_invoices_raw(client, max_pages=0)

    # Test expense entries
    logger.info("\nTesting expense entries field selection")
    expenses.fetch_expense_entries_raw(client, max_pages=0)

    # Test product items
    logger.info("\nTesting product items field selection")
    products.fetch_product_items_raw(client, max_pages=0)

    logger.info("\nAll extract modules tested for field selection")


if __name__ == "__main__":
    test_schema_driven_field_selection()
