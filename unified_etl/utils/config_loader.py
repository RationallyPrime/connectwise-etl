# unified_etl/utils/config_loader.py
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from unified_etl.utils import logging
from unified_etl.utils.exceptions import ConfigurationError

# Default configuration path
_CONFIG_PATH = "/lakehouse/default/Files/config/base_config.yaml"


# Function to set custom config path
def set_config_path(path: str) -> None:
    """
    Sets a custom configuration path and clears the cache.

    Args:
        path: New path to the YAML configuration file.
    """
    global _CONFIG_PATH
    _CONFIG_PATH = path
    # Clear the cache to ensure next load uses the new path
    load_etl_config.cache_clear()
    logging.info(f"Configuration path updated to: {path}")


# Cache the loaded configuration to avoid repeated file reads
@lru_cache(maxsize=1)
def load_etl_config(
    config_path: str | None = None,
) -> dict[str, Any]:
    """
    Loads the enriched ETL configuration YAML file.

    Args:
        config_path: Optional path to the YAML configuration file. If None, uses the default path.

    Returns:
        Dictionary containing the loaded ETL configuration.

    Raises:
        ConfigurationError: If the config file cannot be loaded or parsed.
    """
    # Use provided path or fall back to default
    path_to_use = config_path or _CONFIG_PATH

    try:
        config_file = Path(path_to_use)
        if not config_file.is_file():
            raise ConfigurationError(f"Configuration file not found at {path_to_use}")

        with open(config_file) as f:
            config_data = yaml.safe_load(f)

        if not config_data or "tables" not in config_data:
            raise ConfigurationError("Invalid configuration format: 'tables' key missing.")

        logging.info(f"Successfully loaded ETL configuration from {path_to_use}")
        return config_data["tables"]  # Return the dictionary of tables

    except yaml.YAMLError as e:
        error_msg = f"Error parsing YAML configuration file {path_to_use}: {e}"
        logging.error(error_msg)
        raise ConfigurationError(error_msg) from e
    except Exception as e:
        error_msg = f"Failed to load ETL configuration from {path_to_use}: {e!s}"
        logging.error(error_msg)
        # Don't raise the base ConfigurationError if it's already that type
        if not isinstance(e, ConfigurationError):
            raise ConfigurationError(error_msg) from e
        else:
            raise ConfigurationError(
                str(e)
            ) from e  # Fix type error by specifying return type more precisely


def get_table_config(table_base_name: str) -> dict[str, Any] | None:
    """
    Retrieves the configuration for a specific table base name.

    Args:
        table_base_name: The base name of the table (e.g., 'GLAccount', 'GLEntry').

    Returns:
        The configuration dictionary for the table, or None if not found.
    """
    try:
        config = load_etl_config()  # Uses cached version after first call
        return config.get(table_base_name)
    except ConfigurationError:
        # Log the error but allow processes to potentially continue if config isn't critical
        logging.error(f"Configuration not loaded, cannot get config for {table_base_name}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error getting config for {table_base_name}: {e!s}")
        return None


def get_all_table_configs() -> dict[str, Any]:
    """Returns all loaded table configurations."""
    try:
        return load_etl_config()
    except ConfigurationError:
        logging.error("Configuration not loaded, cannot get all configs.")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error getting all configs: {e!s}")
        return {}


def get_column_schema(table_base_name: str) -> dict[str, Any]:
    """
    Returns the column schema part of a table's config.

    Args:
        table_base_name: The base name of the table (e.g., 'GLAccount', 'Customer')

    Returns:
        Dictionary containing column schema information where keys are standardized column names
        and values are column configuration dictionaries with data_type, bronze_name, etc.
    """
    table_cfg = get_table_config(table_base_name)

    if not table_cfg:
        logging.warning(f"No table configuration found for '{table_base_name}'")
        return {}

    columns = table_cfg.get("columns", {})

    if not columns:
        logging.warning(f"No column schema defined for table '{table_base_name}'")

    return columns
