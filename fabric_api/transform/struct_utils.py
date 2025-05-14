#!/usr/bin/env python
"""
Utilities for handling nested structures in DataFrames.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode_outer, to_json
from pyspark.sql.types import ArrayType, MapType, StructType

logger = logging.getLogger(__name__)

def flatten_dataframe(df: DataFrame, max_depth: int = 3) -> DataFrame:
    """
    Recursively flatten all nested structures in a DataFrame including
    nested structs, arrays of structs, and maps.

    Args:
        df: The DataFrame to flatten
        max_depth: Maximum recursion depth

    Returns:
        DataFrame with fully flattened columns
    """
    # Helper function to check if a column needs flattening
    def needs_flattening(field_dtype):
        return isinstance(field_dtype, StructType) or \
               (isinstance(field_dtype, ArrayType) and isinstance(field_dtype.elementType, StructType)) or \
               isinstance(field_dtype, MapType)

    # Initial schema check
    fields = df.schema.fields

    # Check if there are nested structures that need flattening
    nested_cols = [field.name for field in fields if needs_flattening(field.dataType)]

    # If no nested columns or max depth reached, return the DataFrame as is
    if len(nested_cols) == 0 or max_depth <= 0:
        return df

    # Process struct columns
    struct_cols = [field.name for field in fields
                  if isinstance(field.dataType, StructType)]

    expanded_cols = []

    # Handling regular columns (non-struct)
    for field in fields:
        if field.name not in struct_cols:
            expanded_cols.append(col(field.name))
        else:
            # For struct columns, flatten each field
            struct_fields = field.dataType.fields
            for struct_field in struct_fields:
                expanded_cols.append(
                    col(f"{field.name}.{struct_field.name}").alias(f"{field.name}_{struct_field.name}")
                )

    # Create DataFrame with expanded columns
    expanded_df = df.select(expanded_cols)

    # Recursively apply flattening until no more nested structures exist
    return flatten_dataframe(expanded_df, max_depth - 1)

def convert_arrays_to_json(df: DataFrame, array_columns: list | None = None) -> DataFrame:
    """
    Convert array columns to JSON strings for easier handling.

    Args:
        df: Input DataFrame
        array_columns: List of array columns to convert (if None, detect automatically)

    Returns:
        DataFrame with array columns converted to JSON strings
    """
    # If no array columns specified, detect automatically
    if array_columns is None:
        array_columns = [
            field.name for field in df.schema.fields
            if isinstance(field.dataType, ArrayType)
        ]

    # Convert each array column to JSON
    result_df = df
    for array_col in array_columns:
        result_df = result_df.withColumn(array_col, to_json(col(array_col)))

    return result_df

def explode_array_columns(df: DataFrame, array_columns: list | None = None) -> DataFrame:
    """
    Explode array columns into multiple rows.

    Args:
        df: Input DataFrame
        array_columns: List of array columns to explode (if None, detect automatically)

    Returns:
        DataFrame with array columns exploded into multiple rows
    """
    # If no array columns specified, detect automatically
    if array_columns is None:
        array_columns = [
            field.name for field in df.schema.fields
            if isinstance(field.dataType, ArrayType)
        ]

    # Explode each array column
    result_df = df
    for array_col in array_columns:
        result_df = result_df.withColumn(array_col, explode_outer(col(array_col)))

    return result_df
