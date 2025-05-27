"""Naming utilities for table and column standardization."""
import re
from typing import Optional


def standardize_table_reference(table_name: str, include_suffix: bool = True) -> str:
    """
    Standardize table reference names according to naming conventions.
    
    Args:
        table_name: Original table name
        include_suffix: Whether to include numeric suffix
        
    Returns:
        Standardized table name
    """
    # Remove common prefixes
    clean_name = table_name
    for prefix in ['bc_', 'cw_', 'bronze_', 'silver_', 'gold_']:
        if clean_name.lower().startswith(prefix):
            clean_name = clean_name[len(prefix):]
    
    # Handle hyphenated format (e.g., "GLAccount-15")
    if '-' in clean_name and include_suffix:
        parts = clean_name.split('-')
        if len(parts) == 2 and parts[1].isdigit():
            return f"{parts[0]}{parts[1]}"
    elif '-' in clean_name and not include_suffix:
        parts = clean_name.split('-')
        if len(parts) == 2 and parts[1].isdigit():
            return parts[0]
    
    return clean_name


def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def snake_to_camel(name: str) -> str:
    """Convert snake_case to camelCase."""
    components = name.split('_')
    return components[0] + ''.join(word.capitalize() for word in components[1:])


def standardize_column_name(column_name: str, preserve_camel_case: bool = True) -> str:
    """
    Standardize column names according to project conventions.
    
    Args:
        column_name: Original column name
        preserve_camel_case: Whether to preserve camelCase (per CLAUDE.md)
        
    Returns:
        Standardized column name
    """
    if preserve_camel_case:
        # Preserve camelCase as specified in CLAUDE.md
        return column_name
    else:
        # Convert to snake_case if needed
        return camel_to_snake(column_name)