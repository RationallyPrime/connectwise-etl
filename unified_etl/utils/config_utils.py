# unified_etl/utils/config_utils.py
from typing import Any

from unified_etl.utils import config_loader, logging
from unified_etl.utils.exceptions import ConfigurationError
from unified_etl.utils.naming import standardize_table_reference


def extract_table_config(
    table_base_name: str,
    required_keys: list[str] | None = None,
    default_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Extract and validate standard configuration parameters for a table.

    Args:
        table_base_name: Base name of the table
        required_keys: List of keys that must be present in config
        default_values: Default values for missing keys

    Returns:
        Dictionary of extracted configuration parameters

    Raises:
        ConfigurationError: If a required key is missing
    """
    # Get table config
    std_table_name = standardize_table_reference(table_base_name)
    table_config = config_loader.get_table_config(std_table_name)

    if not table_config:
        if required_keys:
            raise ConfigurationError(f"No configuration found for {std_table_name}")
        return default_values or {}

    # Set up result with defaults
    result = default_values.copy() if default_values else {}

    # Extract common parameters
    # Table type and names
    result["table_type"] = table_config.get("type")
    result["silver_name"] = table_config.get("silver_name")
    result["silver_target"] = table_config.get("silver_target")
    result["gold_name"] = table_config.get("gold_name")
    result["gold_target"] = table_config.get("gold_target")

    # Keys and SCD handling
    result["business_keys"] = table_config.get("business_keys", [])
    result["scd_type"] = table_config.get("scd_type", 1)
    result["incremental_column"] = table_config.get("incremental_column")

    # Partitioning
    partition_config = table_config.get("partition_by", {})
    if isinstance(partition_config, dict):
        result["silver_partition_columns"] = partition_config.get("silver", ["$Company"])
        result["gold_partition_columns"] = partition_config.get("gold")
    else:
        # Legacy format - assumed to be silver partitioning
        result["silver_partition_columns"] = partition_config or ["$Company"]
        result["gold_partition_columns"] = None

    # Check for required keys
    if required_keys:
        missing_keys = [key for key in required_keys if key not in result or result[key] is None]
        if missing_keys:
            raise ConfigurationError(
                f"Missing required configuration keys for {std_table_name}: {missing_keys}"
            )

    return result


def get_global_settings() -> dict[str, Any]:
    """
    Get global settings from configuration.

    Returns:
        Dictionary of global settings
    """
    all_configs = config_loader.get_all_table_configs()
    return all_configs.get("global_settings", {})


def get_dimension_types() -> dict[str, str]:
    """
    Get dimension type mappings from global settings.

    Returns:
        Dictionary mapping dimension types to codes
    """
    global_settings = get_global_settings()
    dimension_types = global_settings.get("dimension_types", {})

    if not dimension_types:
        # Default values if not in config
        dimension_types = {"TEAM": "TEAM", "PRODUCT": "PRODUCT"}
        logging.warning(
            "No dimension types found in config, using defaults",
            defaults=dimension_types,
        )

    return dimension_types


def get_fiscal_settings() -> dict[str, Any]:
    """
    Get fiscal year settings from global configuration.

    Returns:
        Dictionary with fiscal year settings
    """
    global_settings = get_global_settings()

    # Extract fiscal settings with defaults
    return {
        "fiscal_year_start": global_settings.get("fiscal_year_start", 1),  # Default to January
        "min_year": global_settings.get("min_year"),  # No default, will be None if not set
        "max_lookback_years": global_settings.get("max_lookback_years", 5),  # Default to 5 years
    }


def find_business_keys(table_base_name: str) -> list[str]:
    """
    Find business keys for a table from configuration.

    Args:
        table_base_name: Base name of the table

    Returns:
        List of business key column names
    """
    std_table_name = standardize_table_reference(table_base_name)
    table_config = config_loader.get_table_config(std_table_name)

    if not table_config:
        logging.warning(
            f"No configuration found for {std_table_name}, returning empty business keys"
        )
        return []

    # Get business keys from config
    business_keys = table_config.get("business_keys", [])

    # If no explicit business keys, try to infer from columns
    if not business_keys:
        columns = config_loader.get_column_schema(std_table_name)
        if columns:
            # Look for columns marked as business keys or primary keys
            business_keys = [
                col_name
                for col_name, attrs in columns.items()
                if attrs.get("is_business_key") or attrs.get("is_primary_key")
            ]

    # If still no business keys and it's a master data table, look for common patterns
    if not business_keys and table_config.get("type") == "master_data":
        # Common business key patterns for master data tables
        common_keys = ["No", "Code", "ID", "Key"]

        # If we have columns from schema, check for common patterns
        if columns:
            for key in common_keys:
                if key in columns:
                    business_keys.append(key)
                    break
        else:
            # If no columns available, guess based on table name
            logging.warning(
                f"Guessing business key for {std_table_name} without column information"
            )
            if any(term in std_table_name for term in ["Account", "Customer", "Vendor", "Item"]):
                business_keys.append("No")
            elif any(term in std_table_name for term in ["Dimension", "Location"]):
                business_keys.append("Code")

    # Always include $Company if not already present
    if business_keys and "$Company" not in business_keys:
        business_keys.append("$Company")

    return business_keys


def extract_join_keys(
    source_config: dict[str, Any], target_base_name: str
) -> list[tuple[str, str]]:
    """
    Extract join keys between source and target tables from configuration.

    Args:
        source_config: Source table configuration
        target_base_name: Target table base name

    Returns:
        List of (source_column, target_column) tuples
    """
    join_keys = []

    # Look for foreign key relationships in source column definitions
    columns = source_config.get("columns", {})
    for col_name, attrs in columns.items():
        references = attrs.get("references")
        if (
            attrs.get("is_foreign_key")
            and isinstance(references, dict)
            and references.get("table") == target_base_name
            and references.get("column")
        ):
            target_col = references.get("column")
            join_keys.append((col_name, target_col))

    return join_keys
