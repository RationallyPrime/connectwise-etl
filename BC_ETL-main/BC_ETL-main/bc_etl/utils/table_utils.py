# bc_etl/utils/table_utils.py
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from bc_etl.utils import config_loader, logging
from bc_etl.utils.exceptions import TableNotFoundExceptionError
from bc_etl.utils.naming import construct_table_path, standardize_table_reference


def read_table_from_config(
    spark: SparkSession,
    table_base_name: str,
    db_path: str,
    config_key: str | None = None,
    required: bool = True,
) -> tuple[DataFrame | None, str]:
    """
    Read a table based on configuration, with proper error handling.

    Args:
        spark: SparkSession to use
        table_base_name: Base name of the table
        db_path: Database/schema path
        config_key: Optional config key if different from table_base_name
        required: Whether the table is required (raises exception if not found)

    Returns:
        Tuple of (DataFrame, actual_table_name)

    Raises:
        TableNotFoundExceptionError: If table cannot be found and is required
    """
    # Get table config using provided key or base name
    config_lookup_key = config_key or table_base_name

    # Standardize the lookup key to handle variations in naming
    std_lookup_key = standardize_table_reference(config_lookup_key)
    table_config = config_loader.get_table_config(std_lookup_key)

    if not table_config:
        logging.warning(f"No configuration found for {std_lookup_key}")

    # Determine actual table name from config or use base name
    if table_config and "silver_name" in table_config:
        table_name = table_config["silver_name"]
    elif table_config and "gold_name" in table_config:
        table_name = table_config["gold_name"]
    else:
        table_name = table_base_name

    # Construct full table path
    full_table_path = construct_table_path(db_path, table_name)

    try:
        logging.info(f"Reading table {full_table_path}")
        df = spark.table(full_table_path)
        row_count = df.count()
        col_count = len(df.columns)
        logging.info(
            f"Successfully read {full_table_path} with {row_count} rows, {col_count} columns"
        )
        return df, table_name
    except Exception as e:
        error_msg = f"Failed to read table {full_table_path}: {str(e)}"
        logging.error(error_msg)

        if required:
            raise TableNotFoundExceptionError(error_msg) from e

        # Return empty DataFrame with same schema if possible
        try:
            empty_df = spark.createDataFrame([], spark.table(full_table_path).schema)
            logging.warning(f"Returning empty DataFrame for {full_table_path}")
            return empty_df, table_name
        except Exception:
            # If we can't get the schema, raise or return None based on required flag
            if required:
                raise TableNotFoundExceptionError(error_msg) from e

            logging.warning(f"Returning None for {full_table_path}")
            return None, table_name


def extract_fact_name_from_config(
    table_base_name: str, default_fact_type: str | None = None
) -> str:
    """
    Extract the fact table name from configuration.

    Args:
        table_base_name: Base name of the table (e.g., 'GLEntry', 'SalesLine')
        default_fact_type: Default fact type if not found in config

    Returns:
        Fact table name (e.g., 'fact_Finance', 'fact_Sales')
    """
    std_table_name = standardize_table_reference(table_base_name)
    table_config = config_loader.get_table_config(std_table_name)

    if not table_config:
        if default_fact_type:
            return f"fact_{default_fact_type}"
        return f"fact_{std_table_name}"

    # Try to find the fact table name in the config
    if "gold_name" in table_config:
        return table_config["gold_name"]

    if "gold_target" in table_config:
        return table_config["gold_target"]

    # Fallback to default fact type or table name
    if default_fact_type:
        return f"fact_{default_fact_type}"

    # Guess the fact type based on table name
    if any(term in std_table_name for term in ["GLEntry", "Ledger"]):
        return "fact_Finance"
    elif any(term in std_table_name for term in ["Sales", "Customer"]):
        return "fact_Sales"
    elif any(term in std_table_name for term in ["Purchase", "Vendor"]):
        return "fact_Purchase"
    elif any(term in std_table_name for term in ["Item", "Inventory"]):
        return "fact_Item"

    # Last resort
    return f"fact_{std_table_name}"


def get_table_stats(
    spark: SparkSession,
    full_table_path: str,
    include_column_stats: bool = False,
    sample_size: int = 1000,
) -> dict[str, Any]:
    """
    Get statistics about a table.

    Args:
        spark: SparkSession to use
        full_table_path: Full path to the table
        include_column_stats: Whether to include column-level statistics
        sample_size: Sample size for calculating column statistics

    Returns:
        Dictionary of table statistics
    """
    try:
        # Try to get table stats
        stats = {}

        # Get basic table info
        df = spark.table(full_table_path)
        stats["table_name"] = full_table_path
        stats["row_count"] = df.count()
        stats["column_count"] = len(df.columns)
        stats["columns"] = df.columns

        # Get additional metadata if available
        try:
            metadata = spark.sql(f"DESCRIBE DETAIL {full_table_path}").collect()[0]
            stats["location"] = metadata.location
            stats["format"] = metadata.format
            if hasattr(metadata, "partitionColumns"):
                stats["partition_columns"] = metadata.partitionColumns
        except Exception as e:
            logging.debug(f"Could not get detailed metadata: {str(e)}")

        # Get column statistics if requested
        if include_column_stats:
            # Take a sample to avoid expensive processing
            sample_df = df.limit(sample_size)

            column_stats = {}
            for column in df.columns:
                col_stat = {}

                # Get data type
                col_stat["data_type"] = next(
                    (f.dataType.simpleString() for f in df.schema.fields if f.name == column),
                    "unknown",
                )

                # Get null count
                null_count = sample_df.filter(df[column].isNull()).count()
                col_stat["null_percentage"] = round(null_count / sample_size * 100, 2)

                # Add to column stats
                column_stats[column] = col_stat

            stats["column_stats"] = column_stats

        return stats
    except Exception as e:
        logging.error(f"Error getting table stats for {full_table_path}: {str(e)}")
        return {"table_name": full_table_path, "error": str(e), "exists": False}
