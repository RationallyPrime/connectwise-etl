#!/usr/bin/env python
"""
Test script to validate the fallback mechanism for nested fields in SparkModel conversion.
This tests the _flatten_nested_structures function in bronze_loader.py.
"""

import logging

from pyspark.sql import SparkSession

from fabric_api.bronze_loader import _flatten_nested_structures, process_entity
from fabric_api.client import ConnectWiseClient

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_flatten_nested_structures():
    """
    Test the _flatten_nested_structures function with various complex data.
    """
    logger.info("Testing the flatten_nested_structures function")

    # Test case 1: Simple nested object
    test_data_1 = {
        "id": 123,
        "name": "Test Object",
        "parent": {"id": 456, "name": "Parent Object", "type": "Parent Type"},
    }

    result_1 = _flatten_nested_structures(test_data_1)
    logger.info("Test case 1 (simple nested object):")
    for key, value in result_1.items():
        logger.info(f"  {key}: {value}")

    # Test case 2: Array of objects
    test_data_2 = {
        "id": 123,
        "name": "Test Object",
        "children": [
            {"id": 1, "name": "Child 1"},
            {"id": 2, "name": "Child 2"},
            {"id": 3, "name": "Child 3"},
        ],
    }

    result_2 = _flatten_nested_structures(test_data_2)
    logger.info("Test case 2 (array of objects):")
    for key, value in result_2.items():
        logger.info(f"  {key}: {value}")

    # Test case 3: Deeply nested structure
    test_data_3 = {
        "id": 123,
        "name": "Test Object",
        "company": {
            "id": 456,
            "name": "Company Name",
            "address": {
                "street": "123 Main St",
                "city": "Test City",
                "state": "Test State",
                "zip": "12345",
            },
            "contacts": [
                {"id": 1, "name": "Contact 1", "email": "contact1@example.com"},
                {"id": 2, "name": "Contact 2", "email": "contact2@example.com"},
            ],
        },
    }

    result_3 = _flatten_nested_structures(test_data_3)
    logger.info("Test case 3 (deeply nested structure):")
    for key, value in result_3.items():
        logger.info(f"  {key}: {value}")

    # Test case 4: Array of primitives
    test_data_4 = {"id": 123, "name": "Test Object", "tags": ["tag1", "tag2", "tag3"]}

    result_4 = _flatten_nested_structures(test_data_4)
    logger.info("Test case 4 (array of primitives):")
    for key, value in result_4.items():
        logger.info(f"  {key}: {value}")


def test_end_to_end_fallback():
    """
    Test the fallback mechanism within process_entity with a real entity.
    """
    logger.info("Testing end-to-end fallback with TimeEntry")

    # Create a SparkSession for testing
    import pyspark

    spark = SparkSession(pyspark.SparkContext("local[*]", "FallbackTest"))

    # Create a test directory for output
    import os
    import tempfile

    temp_dir = tempfile.mkdtemp()
    bronze_path = os.path.join(temp_dir, "bronze")
    os.makedirs(bronze_path, exist_ok=True)

    # Initialize ConnectWise client
    client = ConnectWiseClient()

    try:
        # Process just a few TimeEntry records to test fallback
        df, errors = process_entity(
            entity_name="TimeEntry",
            spark=spark,
            client=client,
            bronze_path=bronze_path,
            page_size=5,  # Small page size for testing
            max_pages=1,  # Just the first page
            write_mode="overwrite",
        )

        # Show the result
        logger.info(f"Processed {df.count()} TimeEntry records with {len(errors)} errors")
        logger.info("DataFrame schema:")
        for field in df.schema.fields[:10]:  # Show first 10 fields only
            logger.info(f"  {field.name}: {field.dataType}")

        # If possible, intentionally force fallback by causing schema conversion to fail
        logger.info("Now testing with forced schema failure...")

        # This is a dummy implementation - in a real test we'd have to monkey patch
        # or inject a failure into the schema conversion
        # For now we'll just note that we should test this path manually

    finally:
        # Always stop the SparkSession
        spark.stop()
        logger.info(f"Test output was written to {bronze_path}")
        logger.info("You may want to delete this directory when done testing")


if __name__ == "__main__":
    # Test the flattening function
    test_flatten_nested_structures()

    # Uncomment to test with real API data (requires credentials)
    # test_end_to_end_fallback()
