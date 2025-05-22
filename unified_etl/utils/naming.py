import re

from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.session import SparkSession

from unified_etl.utils import logging


def escape_column_name(col_name: str) -> str:
    """
    Escape a Spark DataFrame column name with backticks if it contains special characters
    or is a reserved keyword.

    Args:
        col_name: The original column name.

    Returns:
        The escaped column name.
    """
    # Remove existing backticks first to avoid double-escaping
    clean_name = col_name.strip("`")
    # Escape if it contains special chars, spaces, or might be a keyword (basic check)
    if (
        (
            not clean_name.isalnum() and "_" not in clean_name
        )  # Simple check if not alphanumeric or underscore
        or "$" in clean_name
        or "." in clean_name
        or "-" in clean_name
        or " " in clean_name
        or any(c in clean_name for c in "[]().,;:")
        # Add common reserved words if needed, though backticks handle most cases
        # or clean_name.lower() in ['date', 'timestamp', 'table', 'select']
    ):
        return f"`{clean_name}`"
    return clean_name


def get_escaped_col(df: "DataFrame", col_name: str) -> Column:
    """
    Returns a Spark Column object for the given column name, escaping it if necessary.

    Args:
        df: The DataFrame containing the column.
        col_name: The name of the column.

    Returns:
        A Spark Column object.

    Raises:
        ValueError: If the column name is not found in the DataFrame.
    """
    if col_name not in df.columns:
        raise ValueError(f"Column '{col_name}' not found in DataFrame columns: {df.columns}")
    # Escape the name and then get the column object
    return df[escape_column_name(col_name)]


def construct_table_path(base_path: str, table_name: str) -> str:
    """
    Constructs a fully qualified table path (e.g., database.table).

    Args:
        base_path: The database or schema name (e.g., 'LH.silver', 'gold_db').
        table_name: The name of the table.

    Returns:
        The fully qualified table path.
    """
    if not base_path:
        # If base_path is empty, just return the table name (might be temp view)
        return table_name
    # Simple concatenation, assuming base_path is correctly formatted
    return f"{base_path}.{table_name}"


def standardize_table_reference(table_name: str, include_suffix: bool = False) -> str:
    """
    Standardize table references for consistent lookups.

    Args:
        table_name: Original table name (e.g., 'GLAccount15', 'dim_GLAccount')
        include_suffix: Whether to include numeric suffix in result

    Returns:
        Standardized table reference name (e.g., 'GLAccount')
    """
    if not table_name:
        return ""

    # Handle special prefixes like 'dim_', 'fact_'
    for prefix in ["dim_", "fact_"]:
        if table_name.lower().startswith(prefix):
            table_name = table_name[len(prefix) :]

    # Extract base name and optional suffix
    pattern = r"([A-Za-z]+)(\d*)"
    match = re.match(pattern, table_name)

    if not match:
        # If not matching expected pattern, return as-is
        return table_name

    base_name = match.group(1)
    suffix = match.group(2) if match.group(2) else ""

    # Capitalize first letter for consistency
    base_name = base_name[0].upper() + base_name[1:] if base_name else ""

    # Return with or without suffix based on flag
    if include_suffix and suffix:
        return f"{base_name}{suffix}"
    return base_name


def get_gold_dimension_table(
    spark: SparkSession,
    gold_path: str,
    dimension_name: str,
) -> tuple[str | None, DataFrame | None]:
    """
    Find the correct dimension table name in the gold layer.

    Args:
        spark: Active SparkSession
        gold_path: Path to gold layer
        dimension_name: Base dimension name (e.g., 'GLAccount', 'Customer')

    Returns:
        Tuple of (table_name, DataFrame) or (None, None) if not found
    """
    # Standardize dimension name for consistent lookup
    dim_base_name = standardize_table_reference(dimension_name)

    # Fix gold path construction to prevent schema/database problems
    # Extract just the last part of the gold_path if it contains multiple parts
    gold_db_parts = gold_path.split(".")
    if len(gold_db_parts) > 1:
        clean_gold_path = gold_db_parts[-1]  # Use just the last part (schema name)
        logging.debug(f"Using simplified gold_path: {clean_gold_path} from {gold_path}")
    else:
        clean_gold_path = gold_path

    # Check for exact match first
    try:
        dim_df = spark.table(construct_table_path(clean_gold_path, dim_base_name))
        return dim_base_name, dim_df
    except Exception:
        pass

    # Try with Dim prefix
    try:
        dim_table_name = f"Dim{dim_base_name}"
        dim_df = spark.table(construct_table_path(clean_gold_path, dim_table_name))
        return dim_table_name, dim_df
    except Exception:
        pass

    # Try with dimension_ prefix
    try:
        dim_table_name = f"dimension_{dim_base_name.lower()}"
        dim_df = spark.table(construct_table_path(clean_gold_path, dim_table_name))
        return dim_table_name, dim_df
    except Exception:
        pass

    # Try with dim_ prefix (most common in our refactored code)
    try:
        dim_table_name = f"dim_{dim_base_name}"
        dim_df = spark.table(construct_table_path(clean_gold_path, dim_table_name))
        return dim_table_name, dim_df
    except Exception:
        pass

    logging.warning(f"Could not find dimension table for {dimension_name} in {gold_path}")
    return None, None
