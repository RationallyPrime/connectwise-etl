from __future__ import annotations

"""fabric_api.extract.products - Product item extraction routines."""

from typing import Any, Dict, List, Optional
import logging

from ..client import ConnectWiseClient
from ..connectwise_models import ProductItem
from ..api_utils import get_fields_for_api_call, build_condition_string

logger = logging.getLogger(__name__)

def fetch_product_items_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Fetch raw product item data from ConnectWise API using schema-based field selection.
    Targets: /procurement/products
    Validates against: ProductItem model
    """
    logger.info("Fetching raw product items using schema-based field selection")

    fields_str = fields_override if fields_override else get_fields_for_api_call(ProductItem, max_depth=2)
    logger.debug(f"Using fields for product items: {fields_str}")

    raw_product_items = client.paginate(
        endpoint="/procurement/products",
        entity_name="product items",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_product_items)} raw product items")
    return raw_product_items


def fetch_products_by_catalog_id(
    client: ConnectWiseClient,
    catalog_id: str,
    page_size: int = 100,
    max_pages: int | None = 50,
) -> List[Dict[str, Any]]:
    """
    Fetch product items by catalog ID.
    
    Args:
        client: ConnectWiseClient instance
        catalog_id: Catalog ID to filter by
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        
    Returns:
        List of product items
    """
    condition = f'catalogItem/identifier="{catalog_id}"'
    
    return fetch_product_items_raw(
        client=client,
        conditions=condition,
        page_size=page_size,
        max_pages=max_pages
    )


def fetch_products_by_type(
    client: ConnectWiseClient,
    product_type: str,
    page_size: int = 100,
    max_pages: int | None = 50,
) -> List[Dict[str, Any]]:
    """
    Fetch product items by type.
    
    Args:
        client: ConnectWiseClient instance
        product_type: Product type to filter by
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        
    Returns:
        List of product items
    """
    condition = f'type/name="{product_type}"'
    
    return fetch_product_items_raw(
        client=client,
        conditions=condition,
        page_size=page_size,
        max_pages=max_pages
    )