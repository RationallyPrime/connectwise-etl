#!/usr/bin/env python
"""
Generic entity extractor for ConnectWise API.
"""

import logging
from typing import Any

from pydantic import BaseModel

from ..client import ConnectWiseClient
from ..core.api_utils import get_fields_for_api_call
from ..core.config import get_entity_config
from ._common import validate_batch

logger = logging.getLogger(__name__)

def get_model_class(entity_name: str) -> type[BaseModel]:
    """
    Dynamically import and return the model class for an entity.

    Args:
        entity_name: Name of the entity

    Returns:
        Pydantic model class
    """
    # Import all model classes
    from ..connectwise_models import (
        Agreement,
        ExpenseEntry,
        PostedInvoice,
        ProductItem,
        TimeEntry,
        UnpostedInvoice,
    )

    # Create a mapping of entity names to model classes
    model_mapping = {
        "Agreement": Agreement,
        "TimeEntry": TimeEntry,
        "ExpenseEntry": ExpenseEntry,
        "ProductItem": ProductItem,
        "PostedInvoice": PostedInvoice,
        "UnpostedInvoice": UnpostedInvoice
    }

    if entity_name not in model_mapping:
        raise ValueError(f"Unknown entity: {entity_name}. Must be one of {list(model_mapping.keys())}")

    return model_mapping[entity_name]

def extract_entity(
    client: ConnectWiseClient,
    entity_name: str,
    page_size: int = 100,
    max_pages: int | None = None,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
    return_validated: bool = False,
) -> list[dict[str, Any]] | tuple[list[BaseModel], list[dict[str, Any]]]:
    """
    Generic function to extract any entity type from ConnectWise API.

    Args:
        client: ConnectWiseClient instance
        entity_name: Name of the entity to extract
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch (None for all)
        conditions: API query conditions for filtering
        child_conditions: API query conditions for child objects
        order_by: Field to order results by
        fields_override: Override the fields to request
        return_validated: If True, validate raw data and return (valid, errors)

    Returns:
        If return_validated=False: List of raw data dictionaries
        If return_validated=True: Tuple of (valid models, validation errors)
    """
    # Get entity configuration
    config = get_entity_config(entity_name)
    endpoint = config["endpoint"]

    # Get model class
    model_class = get_model_class(entity_name)

    # Generate fields string based on model
    fields_str = fields_override if fields_override else get_fields_for_api_call(model_class, max_depth=2)
    logger.debug(f"Using fields for {entity_name}: {fields_str}")

    # Paginate through API results
    raw_data = client.paginate(
        endpoint=endpoint,
        entity_name=entity_name + "s",  # Pluralize for API
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_data)} raw {entity_name} records")

    # Return raw data or validate
    if return_validated:
        return validate_batch(raw_data, model_class)

    return raw_data
