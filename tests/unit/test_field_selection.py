#!/usr/bin/env python
"""
Test script for the enhanced field selection utility.
"""

import logging

from fabric_api.api_utils import get_fields_for_api_call
from fabric_api.connectwise_models import (
    Agreement,
    ExpenseEntry,
    PostedInvoice,
    ProductItem,
    TimeEntry,
    UnpostedInvoice,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def test_field_selection_basic():
    """
    Test basic field selection (default depth=1) for various entity types.
    """
    logger.info("Testing basic field selection for entity types")

    # Test each entity type
    models = [
        Agreement,
        TimeEntry,
        PostedInvoice,
        UnpostedInvoice,
        ExpenseEntry,
        ProductItem
    ]

    for model_class in models:
        model_name = model_class.__name__
        logger.info(f"Testing field selection for {model_name}")

        # Get fields with default depth (1)
        fields = get_fields_for_api_call(model_class)
        logger.info(f"Fields for {model_name} (depth=1):")
        logger.info(f"  {fields}")
        logger.info(f"  Field count: {len(fields.split(','))}")

def test_field_selection_nested():
    """
    Test nested field selection with increased depth.
    """
    logger.info("Testing nested field selection")

    # Test Agreement with different depths
    for depth in [1, 2, 3]:
        fields = get_fields_for_api_call(Agreement, max_depth=depth)
        logger.info(f"Fields for Agreement (depth={depth}):")
        logger.info(f"  {fields}")
        logger.info(f"  Field count: {len(fields.split(','))}")

    # Test with include_all_nested=True
    fields = get_fields_for_api_call(Agreement, max_depth=2, include_all_nested=True)
    logger.info("Fields for Agreement (depth=2, include_all_nested=True):")
    logger.info(f"  {fields}")
    logger.info(f"  Field count: {len(fields.split(','))}")

def test_nested_path_formatting():
    """
    Test that nested paths are formatted correctly for the API.
    """
    logger.info("Testing nested path formatting")

    # Test TimeEntry which has more complex nesting
    fields = get_fields_for_api_call(TimeEntry, max_depth=2)

    # Check for properly formatted paths like "company/id", "company/name", etc.
    nested_paths = [f for f in fields.split(',') if '/' in f]
    logger.info("Nested paths in TimeEntry (depth=2):")
    for path in nested_paths[:10]:  # Show first 10 paths only
        logger.info(f"  {path}")

if __name__ == "__main__":
    test_field_selection_basic()
    logger.info("\n")
    test_field_selection_nested()
    logger.info("\n")
    test_nested_path_formatting()
