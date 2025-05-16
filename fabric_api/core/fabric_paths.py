#!/usr/bin/env python
"""
Consolidated path handling utilities optimized for Microsoft Fabric.
"""

import os

# Default Fabric paths
DEFAULT_LAKEHOUSE_ROOT = "/lakehouse/default"
DEFAULT_TABLES_PATH = f"{DEFAULT_LAKEHOUSE_ROOT}/Tables"
DEFAULT_FILES_PATH = f"{DEFAULT_LAKEHOUSE_ROOT}/Files"
DEFAULT_ERROR_TABLE = "validation_errors"


def get_table_path(entity_name: str, base_path: str | None = None) -> str:
    """
    Get the standard path for an entity table in Fabric.

    Args:
        entity_name: Name of the entity (e.g., "Agreement", "TimeEntry")
        base_path: Base path for tables (defaults to DEFAULT_TABLES_PATH)

    Returns:
        Full path to the table in OneLake
    """
    # Use entity name as-is for table name
    table_name = entity_name

    # Use provided base path or default to Tables in lakehouse
    path = os.path.join(base_path or DEFAULT_TABLES_PATH, table_name)

    return path


def get_error_table_path(base_path: str | None = None) -> str:
    """
    Get the path for the validation errors table.

    Args:
        base_path: Base path for tables (defaults to DEFAULT_TABLES_PATH)

    Returns:
        Path to the validation errors table
    """
    return os.path.join(base_path or DEFAULT_TABLES_PATH, DEFAULT_ERROR_TABLE)


def get_entity_table_name(entity_name: str) -> str:
    """
    Get the standardized table name for an entity.

    Args:
        entity_name: Name of the entity (e.g., "Agreement", "TimeEntry")

    Returns:
        Standardized table name for the entity (same as entity name)
    """
    return entity_name


def normalize_lakehouse_path(path: str) -> str:
    """
    Normalize a path to ensure it uses the proper OneLake format.

    Args:
        path: Path to normalize

    Returns:
        Normalized path in OneLake format
    """
    # Strip leading/trailing slashes and spaces
    clean_path = path.strip().strip("/")

    # Ensure path starts with /lakehouse prefix if it doesn't already
    if not clean_path.startswith("lakehouse/"):
        if not clean_path.startswith("/lakehouse/"):
            clean_path = f"{DEFAULT_LAKEHOUSE_ROOT}/{clean_path}"
    else:
        clean_path = f"/{clean_path}"

    return clean_path
