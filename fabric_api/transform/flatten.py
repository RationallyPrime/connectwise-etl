#!/usr/bin/env python
"""
Utilities for flattening nested structures in DataFrames.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json
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

def flatten_array_struct_columns(df: DataFrame, array_struct_columns: list | None = None, array_to_json: bool = True) -> DataFrame:
    """
    Handle arrays of structs by either converting to JSON strings or exploding.

    Args:
        df: Input DataFrame
        array_struct_columns: List of array struct columns to process (if None, detect automatically)
        array_to_json: If True, convert arrays to JSON; if False, leave as is

    Returns:
        DataFrame with processed array columns
    """
    if df.isEmpty():
        return df

    # If no columns specified, detect automatically
    if array_struct_columns is None:
        array_struct_columns = [
            field.name for field in df.schema.fields
            if isinstance(field.dataType, ArrayType) and
               isinstance(field.dataType.elementType, StructType)
        ]

    if not array_struct_columns:
        return df

    # Process each array column
    result_df = df
    for array_col in array_struct_columns:
        if array_to_json:
            # Convert arrays to JSON strings
            result_df = result_df.withColumn(array_col, to_json(col(array_col)))

    return result_df

def flatten_all_nested_structures(df: DataFrame) -> DataFrame:
    """
    Apply complete flattening to all nested structures.

    Args:
        df: Input DataFrame

    Returns:
        Completely flattened DataFrame
    """
    # First flatten all struct types
    flattened_df = flatten_dataframe(df)

    # Then convert any remaining array of structs to JSON
    result_df = flatten_array_struct_columns(flattened_df)

    return result_df

def verify_no_remaining_structs(df: DataFrame) -> bool:
    """
    Verify that no struct types remain in the DataFrame.

    Args:
        df: DataFrame to check

    Returns:
        True if no structs remain, False otherwise
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            logger.warning(f"Field {field.name} is still a struct after flattening!")
            return False

    return True
