"""Configuration utilities for the unified ETL framework."""
from typing import Dict, Any, Optional


def get_table_config(table_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Configuration dictionary for the table
    """
    # For now, return basic default config
    # This will be enhanced to load from actual config files
    return {
        "bronze_table_name": f"bronze_{table_name}",
        "silver_table_name": f"silver_{table_name}",
        "gold_table_name": f"gold_{table_name}",
        "scd_type": 1,
        "business_keys": ["id"],
        "incremental_column": "lastModified"
    }


def extract_table_config(entity_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract table-specific configuration from entity config.
    
    Args:
        entity_name: Name of the entity
        config: Full configuration dictionary
        
    Returns:
        Table-specific configuration
    """
    if 'entities' in config and entity_name in config['entities']:
        return config['entities'][entity_name]
    
    # Return default config if not found
    return get_table_config(entity_name)