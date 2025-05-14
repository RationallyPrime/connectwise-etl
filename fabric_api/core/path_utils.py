#!/usr/bin/env python
"""
Path handling for Microsoft Fabric.
"""

import os


def get_table_path(
    entity_name: str,
    base_path: str | None = None,
    prefix: str = "cw_"
) -> str:
    """
    Get path for an entity table in Fabric.

    Args:
        entity_name: Name of the entity (e.g., "Agreement", "TimeEntry")
        base_path: Base path from notebook (defaults to lakehouse Tables path)
        prefix: Prefix to add to table name

    Returns:
        Full path to the table
    """
    # Normalize entity name
    normalized_name = entity_name.lower().replace(" ", "_")
    table_name = f"{prefix}{normalized_name}"

    # Use provided base path or default to Tables in lakehouse
    path = os.path.join(base_path or "/lakehouse/default/Tables", table_name)

    return path
