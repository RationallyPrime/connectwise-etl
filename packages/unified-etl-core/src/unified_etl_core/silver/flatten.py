# unified_etl/silver/flatten.py
"""
Column flattening utilities for silver layer transformation.
Handles nested JSON structures and complex data types.
"""

import json

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, MapType, StringType, StructType
from unified_etl_core.utils import logging
from unified_etl_core.utils.exceptions import ColumnStandardizationError


def flatten_nested_columns(df: DataFrame, max_depth: int = 3) -> DataFrame:
    """
    Flatten nested columns (structs, maps, arrays) into separate columns.

    Args:
        df: DataFrame with potentially nested columns
        max_depth: Maximum nesting depth to flatten

    Returns:
        DataFrame with flattened columns
    """

    def _flatten_struct_columns(df: DataFrame, prefix: str = "", depth: int = 0) -> DataFrame:
        """Recursively flatten struct columns."""
        if depth >= max_depth:
            return df

        result_df = df
        struct_columns = []

        # Find struct columns
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                struct_columns.append(field.name)

        # Flatten each struct column
        for struct_col in struct_columns:
            struct_fields = df.schema[struct_col].dataType.fields

            # Extract each field from the struct
            for field in struct_fields:
                new_col_name = (
                    f"{prefix}{struct_col}_{field.name}" if prefix else f"{struct_col}_{field.name}"
                )
                result_df = result_df.withColumn(new_col_name, F.col(f"{struct_col}.{field.name}"))

            # Drop the original struct column
            result_df = result_df.drop(struct_col)

            # Recursively flatten if there are still nested structs
            if any(isinstance(field.dataType, StructType) for field in struct_fields):
                result_df = _flatten_struct_columns(result_df, prefix, depth + 1)

        return result_df

    try:
        with logging.span("flatten_nested_columns", max_depth=max_depth):
            # First, handle JSON string columns that should be parsed
            json_columns = _identify_json_columns(df)

            if json_columns:
                df = _parse_json_columns(df, json_columns)

            # Then flatten struct columns
            flattened_df = _flatten_struct_columns(df)

            # Handle array columns by converting to delimited strings
            flattened_df = _flatten_array_columns(flattened_df)

            # Handle map columns by extracting common keys
            flattened_df = _flatten_map_columns(flattened_df)

            logging.info(
                "Column flattening complete",
                original_columns=len(df.columns),
                flattened_columns=len(flattened_df.columns),
                json_columns_parsed=len(json_columns) if json_columns else 0,
            )

            return flattened_df

    except Exception as e:
        error_msg = f"Failed to flatten nested columns: {e!s}"
        logging.error(error_msg)
        raise ColumnStandardizationError(error_msg) from e


def _identify_json_columns(df: DataFrame) -> list[str]:
    """
    Identify columns that contain JSON strings based on content analysis.

    Args:
        df: DataFrame to analyze

    Returns:
        List of column names that contain JSON data
    """
    json_columns = []

    # Sample some rows to check for JSON content
    sample_df = df.sample(0.1).limit(100)

    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            # Check if column contains JSON-like strings
            sample_values = [
                row[col_name]
                for row in sample_df.select(col_name).collect()
                if row[col_name] is not None
            ]

            json_count = 0
            for value in sample_values[:10]:  # Check first 10 non-null values
                if isinstance(value, str) and value.strip().startswith(("{", "[")):
                    try:
                        json.loads(value)
                        json_count += 1
                    except (json.JSONDecodeError, ValueError):
                        continue

            # If more than half the sampled values are valid JSON, consider it a JSON column
            if json_count > len(sample_values) * 0.5:
                json_columns.append(col_name)

    return json_columns


def _parse_json_columns(df: DataFrame, json_columns: list[str]) -> DataFrame:
    """
    Parse JSON string columns into struct columns.

    Args:
        df: DataFrame with JSON string columns
        json_columns: List of column names containing JSON

    Returns:
        DataFrame with parsed JSON columns
    """
    result_df = df

    for col_name in json_columns:
        try:
            # Parse JSON string to struct
            parsed_col = F.from_json(F.col(col_name), "map<string,string>")
            result_df = result_df.withColumn(f"{col_name}_parsed", parsed_col)

            logging.debug(f"Parsed JSON column: {col_name}")

        except Exception as e:
            logging.warning(f"Failed to parse JSON column {col_name}: {e}")
            continue

    return result_df


def _flatten_array_columns(df: DataFrame) -> DataFrame:
    """
    Flatten array columns by converting to delimited strings.

    Args:
        df: DataFrame with array columns

    Returns:
        DataFrame with flattened array columns
    """
    result_df = df

    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            col_name = field.name
            # Convert array to pipe-delimited string
            result_df = result_df.withColumn(col_name, F.concat_ws("|", F.col(col_name)))

            logging.debug(f"Flattened array column: {col_name}")

    return result_df


def _flatten_map_columns(df: DataFrame) -> DataFrame:
    """
    Flatten map columns by extracting common keys.

    Args:
        df: DataFrame with map columns

    Returns:
        DataFrame with flattened map columns
    """
    result_df = df

    for field in df.schema.fields:
        if isinstance(field.dataType, MapType):
            col_name = field.name

            # For now, convert map to JSON string for simplicity
            # In a production system, you might want to extract specific known keys
            result_df = result_df.withColumn(f"{col_name}_json", F.to_json(F.col(col_name))).drop(
                col_name
            )

            logging.debug(f"Flattened map column: {col_name}")

    return result_df


def strip_column_prefixes_suffixes(
    df: DataFrame,
    prefixes_to_strip: list[str] | None = None,
    suffixes_to_strip: list[str] | None = None,
) -> DataFrame:
    """
    Strip specified prefixes and suffixes from column names.

    Args:
        df: DataFrame to process
        prefixes_to_strip: List of prefixes to remove (e.g., ['api_', 'sys_'])
        suffixes_to_strip: List of suffixes to remove (e.g., ['_id', '_ref'])

    Returns:
        DataFrame with cleaned column names
    """
    prefixes_to_strip = prefixes_to_strip or []
    suffixes_to_strip = suffixes_to_strip or []

    if not prefixes_to_strip and not suffixes_to_strip:
        return df

    try:
        result_df = df
        renamed_columns = {}

        for col_name in df.columns:
            new_name = col_name

            # Strip prefixes
            for prefix in prefixes_to_strip:
                if new_name.startswith(prefix):
                    new_name = new_name[len(prefix) :]
                    break  # Only strip one prefix

            # Strip suffixes
            for suffix in suffixes_to_strip:
                if new_name.endswith(suffix):
                    new_name = new_name[: -len(suffix)]
                    break  # Only strip one suffix

            # Rename if changed
            if new_name != col_name and new_name:  # Ensure new name is not empty
                result_df = result_df.withColumnRenamed(col_name, new_name)
                renamed_columns[col_name] = new_name

        if renamed_columns:
            logging.info(
                "Stripped column prefixes/suffixes",
                renamed_count=len(renamed_columns),
                mapping=renamed_columns,
            )

        return result_df

    except Exception as e:
        error_msg = f"Failed to strip column prefixes/suffixes: {e!s}"
        logging.error(error_msg)
        raise ColumnStandardizationError(error_msg) from e
