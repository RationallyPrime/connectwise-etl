"""ConnectWise configuration loader."""

import importlib.resources
from pathlib import Path
from typing import Any

import yaml


def load_silver_config() -> dict[str, Any]:
    """Load the ConnectWise silver configuration from package resources.

    Returns:
        Dictionary containing silver configuration for all ConnectWise entities
    """
    try:
        # Try to load from package resources (for installed package)
        if hasattr(importlib.resources, "files"):
            # Python 3.9+
            config_files = importlib.resources.files("unified_etl_connectwise.config")
            config_text = (config_files / "connectwise_silver_config.yaml").read_text()
        else:
            # Python 3.8 fallback
            import pkg_resources

            config_text = pkg_resources.resource_string(
                "unified_etl_connectwise.config", "connectwise_silver_config.yaml"
            ).decode("utf-8")

        return yaml.safe_load(config_text)

    except (ImportError, FileNotFoundError):
        # Fallback to file system for development
        config_path = Path(__file__).parent.parent.parent / "connectwise_silver_config.yaml"
        if config_path.exists():
            with open(config_path) as f:
                return yaml.safe_load(f)
        else:
            raise FileNotFoundError(
                "Could not find connectwise_silver_config.yaml in package or filesystem"
            )


def get_entity_config(entity_name: str) -> dict[str, Any]:
    """Get silver configuration for a specific entity.

    Args:
        entity_name: Name of the entity (e.g., 'agreement', 'time_entry')

    Returns:
        Configuration dictionary for the entity

    Raises:
        KeyError: If entity not found in configuration
    """
    config = load_silver_config()

    if entity_name not in config["entities"]:
        raise KeyError(f"Entity '{entity_name}' not found in silver configuration")

    return config["entities"][entity_name]
