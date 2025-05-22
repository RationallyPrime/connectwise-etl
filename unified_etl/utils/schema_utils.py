# unified_etl/utils/schema_utils.py
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructType,
    TimestampType,
)

from unified_etl.utils import logging


def enforce_schema(
    df: DataFrame,
    target_schema: StructType,
    cast_columns: bool = True,
    add_missing: bool = True,
) -> DataFrame:
    """
    Enforce a target schema on a DataFrame by:
    1. Adding missing columns with default values
    2. Casting columns to the correct type
    3. Selecting columns in the specified order

    Args:
        df: Source DataFrame
        target_schema: Target schema to enforce
        cast_columns: Whether to cast columns to target types
        add_missing: Whether to add missing columns

    Returns:
        DataFrame with enforced schema
    """
    result_df = df

    # Check for missing columns and add them with default values
    if add_missing:
        for field in target_schema.fields:
            if field.name not in result_df.columns:
                logging.warning(f"Adding missing column {field.name} with default value")

                # Add column with appropriate default value based on data type
                if isinstance(field.dataType, StringType):
                    result_df = result_df.withColumn(field.name, F.lit(None).cast("string"))
                elif isinstance(field.dataType, IntegerType):
                    result_df = result_df.withColumn(field.name, F.lit(None).cast("integer"))
                elif isinstance(field.dataType, BooleanType):
                    result_df = result_df.withColumn(field.name, F.lit(False))
                elif isinstance(field.dataType, DateType):
                    result_df = result_df.withColumn(field.name, F.lit(None).cast("date"))
                elif isinstance(field.dataType, DecimalType):
                    result_df = result_df.withColumn(field.name, F.lit(None).cast(field.dataType))
                elif isinstance(field.dataType, TimestampType):
                    result_df = result_df.withColumn(field.name, F.lit(None).cast("timestamp"))
                else:
                    # Default fallback
                    result_df = result_df.withColumn(field.name, F.lit(None))

    # Cast columns to the correct types if requested
    if cast_columns:
        for field in target_schema.fields:
            if field.name in result_df.columns:
                result_field = next(
                    (f for f in result_df.schema.fields if f.name == field.name), None
                )

                if result_field and str(result_field.dataType) != str(field.dataType):
                    logging.warning(
                        f"Casting column {field.name} from {result_field.dataType} to {field.dataType}"
                    )
                    result_df = result_df.withColumn(
                        field.name, F.col(field.name).cast(field.dataType)
                    )

    # Select columns in the specified order
    required_columns = [field.name for field in target_schema.fields]
    result_df = result_df.select(*required_columns)

    return result_df


def validate_required_columns(
    df: DataFrame,
    required_columns: list[str],
    raise_error: bool = True,
    error_class: Exception | None = None,
) -> bool:
    """
    Validate that a DataFrame contains all required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present
        raise_error: Whether to raise an exception if columns are missing
        error_class: Exception class to raise (defaults to ValueError)

    Returns:
        True if all required columns are present, False otherwise

    Raises:
        Exception: If columns are missing and raise_error is True
    """
    missing_columns = [col for col in required_columns if col not in df.columns]

    if not missing_columns:
        return True

    error_msg = (
        f"Required columns missing: {', '.join(missing_columns)}. "
        f"Available columns: {', '.join(df.columns)}"
    )

    if raise_error:
        if error_class:
            raise error_class(error_msg)
        else:
            raise ValueError(error_msg)

    logging.warning(error_msg)
    return False


def get_schema_diff(df: DataFrame, target_schema: StructType) -> dict[str, Any]:
    """
    Get differences between a DataFrame's schema and a target schema.

    Args:
        df: DataFrame to check
        target_schema: Target schema to compare against

    Returns:
        Dictionary with schema difference information
    """
    # Get column sets
    df_columns = set(df.columns)
    target_columns = {field.name for field in target_schema.fields}

    # Find column differences
    missing_columns = target_columns - df_columns
    extra_columns = df_columns - target_columns

    # Check data type differences
    type_diffs = {}
    for field in target_schema.fields:
        if field.name in df_columns:
            df_field = next((f for f in df.schema.fields if f.name == field.name), None)
            if df_field and str(df_field.dataType) != str(field.dataType):
                type_diffs[field.name] = {
                    "current": str(df_field.dataType),
                    "target": str(field.dataType),
                }

    return {
        "missing_columns": list(missing_columns),
        "extra_columns": list(extra_columns),
        "type_differences": type_diffs,
        "is_compatible": len(missing_columns) == 0 and len(type_diffs) == 0,
    }


def create_dimension_flags(
    df: DataFrame, dimension_prefix_pairs: list[tuple] | None = None
) -> DataFrame:
    """
    Create standard dimension flag columns for a DataFrame.

    Args:
        df: DataFrame to process
        dimension_prefix_pairs: List of (dimension_name, code_column_prefix) tuples
                              Default: [("Team", "Team"), ("Product", "Product")]

    Returns:
        DataFrame with dimension flag columns added
    """
    result_df = df

    if dimension_prefix_pairs is None:
        dimension_prefix_pairs = [("Team", "Team"), ("Product", "Product")]

    for dim_name, prefix in dimension_prefix_pairs:
        code_col = f"{prefix}Code"
        flag_col = f"Has{dim_name}Dimension"

        if code_col in result_df.columns and flag_col not in result_df.columns:
            result_df = result_df.withColumn(
                flag_col, F.when(F.col(code_col).isNotNull(), F.lit(True)).otherwise(F.lit(False))
            )

    return result_df


def add_document_type_text(
    df: DataFrame, document_type_col: str = "DocumentType", text_col: str = "DocumentTypeText"
) -> DataFrame:
    """
    Add standardized document type text column based on document type codes.

    Args:
        df: DataFrame to process
        document_type_col: Source document type code column
        text_col: Target document type text column

    Returns:
        DataFrame with document type text column added
    """
    if document_type_col not in df.columns:
        return df

    if text_col in df.columns:
        return df

    return df.withColumn(
        text_col,
        F.when(F.col(document_type_col) == 0, "Payment")
        .when(F.col(document_type_col) == 1, "Invoice")
        .when(F.col(document_type_col) == 2, "Credit Memo")
        .when(F.col(document_type_col) == 3, "Finance Charge Memo")
        .when(F.col(document_type_col) == 4, "Reminder")
        .when(F.col(document_type_col) == 5, "Refund")
        .otherwise("Unknown"),
    )
