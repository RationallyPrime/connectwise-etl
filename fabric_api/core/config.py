#!/usr/bin/env python
"""
Centralized configuration for ConnectWise ETL.
"""

from typing import Any

# Entity configuration with endpoints and other settings
ENTITY_CONFIG = {
    "Agreement": {
        "endpoint": "/finance/agreements",
        "output_table": "cw_agreement",
        "description": "ConnectWise agreements with all related metadata"
    },
    "PostedInvoice": {
        "endpoint": "/finance/invoices",
        "output_table": "cw_posted_invoice",
        "description": "ConnectWise posted (finalized) invoices"
    },
    "UnpostedInvoice": {
        "endpoint": "/finance/invoices/search",  # Special endpoint for unposted
        "output_table": "cw_unposted_invoice",
        "description": "ConnectWise unposted (draft) invoices"
    },
    "TimeEntry": {
        "endpoint": "/time/entries",
        "output_table": "cw_time_entry",
        "description": "ConnectWise time entries for billing and tracking"
    },
    "ExpenseEntry": {
        "endpoint": "/expense/entries",
        "output_table": "cw_expense_entry",
        "description": "ConnectWise expense entries for billing and tracking"
    },
    "ProductItem": {
        "endpoint": "/procurement/products",
        "output_table": "cw_product_item",
        "description": "ConnectWise product items for billing"
    }
}

# Delta write options optimized for Fabric
DELTA_WRITE_OPTIONS = {
    "mergeSchema": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}

def get_entity_config(entity_name: str) -> dict[str, Any]:
    """
    Get configuration for a specific entity.

    Args:
        entity_name: Name of the entity

    Returns:
        Entity configuration dictionary
    """
    if entity_name not in ENTITY_CONFIG:
        raise ValueError(f"Unknown entity: {entity_name}. Must be one of {list(ENTITY_CONFIG.keys())}")

    return ENTITY_CONFIG[entity_name]
