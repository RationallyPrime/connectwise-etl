"""Minimal configuration utilities for ETL framework."""

from typing import Any


def get_table_config(entity_name: str, configs: dict[str, Any]) -> dict[str, Any]:
    """Get table configuration for a specific entity.
    
    Args:
        entity_name: Name of the entity
        configs: Dictionary of entity configurations
        
    Returns:
        Configuration dictionary for the entity
        
    Raises:
        KeyError: If entity configuration not found
    """
    if entity_name not in configs:
        raise KeyError(f"Configuration not found for entity: {entity_name}")
    
    return configs[entity_name]