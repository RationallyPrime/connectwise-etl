# unified_etl/bronze/reader.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from unified_etl.utils import logging
from unified_etl.utils.config_loader import get_table_config
from unified_etl.utils.exceptions import TableNotFoundExceptionError
from unified_etl.utils.naming import standardize_table_reference


def read_bc_table(spark: SparkSession, bronze_path: str, table_name: str) -> DataFrame:
    """
    Read BC table from bronze layer with proper naming convention handling.

    Args:
        spark: Active SparkSession
        bronze_path: Fully qualified path to bronze layer
        table_name: Original BC table name (e.g. 'GLAccount' or 'GLAccount-15')

    Returns:
        DataFrame containing the table data

    Raises:
        TableNotFoundException: If table cannot be found in any of the expected formats
    """
    # Get table config if available
    table_base_name = standardize_table_reference(table_name, include_suffix=False)
    table_config = get_table_config(table_base_name)

    if table_config and "bronze_table" in table_config:
        # Use configuration-defined bronze table name as primary
        primary_candidate = table_config["bronze_table"]
        table_candidates = [primary_candidate]
        # Add traditional candidates as fallbacks
        table_candidates.extend(_generate_table_name_candidates(table_name))
    else:
        # Fall back to traditional candidate generation if no config
        table_candidates = _generate_table_name_candidates(table_name)
        logging.debug(f"No configuration found for {table_base_name}, using generated candidates")

    # Try each candidate in sequence
    errors = []
    for candidate in table_candidates:
        full_table_path = f"{bronze_path}.{candidate}"
        try:
            logging.debug("Attempting to read table", table_path=full_table_path)
            return spark.table(full_table_path)
        except AnalysisException as e:
            # Only catch "Table or view not found" errors
            if "Table or view not found" in str(e):
                errors.append(f"Table not found: {full_table_path}")
                continue
            # Re-raise any other AnalysisException
            raise

    # If we get here, all candidates failed
    error_msg = f"Could not find table '{table_name}' in any expected format"
    logging.error(
        "Table not found in any expected format",
        table_name=table_name,
        tried_candidates=table_candidates,
    )
    raise TableNotFoundExceptionError(error_msg)


def _generate_table_name_candidates(table_name: str) -> list[str]:
    """
    Generate possible table name variations based on BC naming conventions.

    Args:
        table_name: Original BC table name

    Returns:
        List of possible table name variations in priority order
    """
    candidates = []

    # 1. Original name as-is
    candidates.append(table_name)

    # 2. Standardized name with proper case and with numeric suffix
    standardized_with_suffix = standardize_table_reference(table_name, include_suffix=True)
    candidates.append(standardized_with_suffix)

    # 3. Standardized name with proper case and without numeric suffix
    standardized_without_suffix = standardize_table_reference(table_name, include_suffix=False)
    candidates.append(standardized_without_suffix)

    # 4. Lowercase versions
    candidates.append(table_name.lower())
    candidates.append(standardized_with_suffix.lower())
    candidates.append(standardized_without_suffix.lower())

    # 5. Handle hyphenated format (BC export format often uses hyphens)
    if "-" in table_name:
        parts = table_name.split("-")
        if len(parts) == 2 and parts[1].isdigit():
            # Add format like "glaccount15" (lowercase with suffix)
            candidates.append(f"{parts[0]}{parts[1]}".lower())
            # Add format like "GLAccount15" (proper case with suffix)
            candidates.append(
                f"{standardize_table_reference(parts[0], include_suffix=False)}{parts[1]}"
            )

    # Return unique candidates in priority order
    return list(dict.fromkeys(candidates))
