# unified_etl/utils/dataframe_utils.py
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame

from unified_etl.utils import logging


def add_audit_columns(
    df: DataFrame,
    layer: str = "silver",
    include_created: bool = True,
    include_modified: bool = True,
) -> DataFrame:
    """
    Add standard audit columns to a DataFrame.

    Args:
        df: DataFrame to add audit columns to
        layer: Layer name (e.g., 'silver', 'gold')
        include_created: Whether to include created timestamp
        include_modified: Whether to include modified timestamp

    Returns:
        DataFrame with audit columns added
    """
    result_df = df
    layer_capitalized = layer.capitalize()

    # Add created timestamp if not already present
    created_col = f"{layer_capitalized}CreatedAt"
    if include_created and created_col not in result_df.columns:
        result_df = result_df.withColumn(created_col, F.current_timestamp())

    # Add modified timestamp if not already present
    modified_col = f"{layer_capitalized}ModifiedAt"
    if include_modified and modified_col not in result_df.columns:
        result_df = result_df.withColumn(modified_col, F.current_timestamp())

    return result_df


def dataframe_stats(df: DataFrame, name: str = "DataFrame") -> dict[str, Any]:
    """
    Collect basic statistics about a DataFrame for logging.

    Args:
        df: DataFrame to analyze
        name: Optional name for the DataFrame

    Returns:
        Dictionary of statistics
    """
    row_count = df.count()
    column_count = len(df.columns)
    null_counts = {
        col: df.filter(F.col(col).isNull()).count()
        for col in df.columns[:10]  # Limit to first 10 columns to avoid excessive processing
    }

    # Calculate null percentages
    null_percentages = {
        col: round(count / row_count * 100, 2) if row_count > 0 else 0
        for col, count in null_counts.items()
    }

    # Find columns with high null percentages
    high_null_cols = {col: pct for col, pct in null_percentages.items() if pct > 80}

    return {
        "name": name,
        "row_count": row_count,
        "column_count": column_count,
        "high_null_columns": high_null_cols,
    }


def add_sign_columns(
    df: DataFrame,
    value_columns: list[str] | None = None,
) -> DataFrame:
    """
    Add sign columns for specified numeric value columns.

    Args:
        df: DataFrame
        value_columns: List of value columns to add sign columns for
                      Defaults to ["Amount", "Quantity"]

    Returns:
        DataFrame with sign columns added
    """
    result_df = df

    if value_columns is None:
        value_columns = ["Amount", "Quantity"]

    for col in value_columns:
        sign_col = f"{col}Sign"

        if col in result_df.columns and sign_col not in result_df.columns:
            result_df = result_df.withColumn(
                sign_col,
                F.when(F.col(col) >= 0, F.lit(1)).otherwise(F.lit(-1)),
            )

    return result_df


def year_filter(
    df: DataFrame,
    min_year: int,
    date_column: str = "PostingDate",
    year_column: str = "PostingYear",
    add_year_column: bool = True,
) -> DataFrame:
    """
    Filter DataFrame by year and optionally add year column.

    Args:
        df: DataFrame to filter
        min_year: Minimum year to include
        date_column: Column containing date values
        year_column: Name for the extracted year column
        add_year_column: Whether to add year column if not present

    Returns:
        Filtered DataFrame with year column added if requested
    """
    result_df = df

    # Add year column if requested and not present
    if (
        add_year_column
        and year_column not in result_df.columns
        and date_column in result_df.columns
    ):
        result_df = result_df.withColumn(year_column, F.year(F.col(date_column)))

    # Apply filter if year column exists
    if year_column in result_df.columns:
        original_count = result_df.count()
        result_df = result_df.filter(F.col(year_column) >= min_year)
        filtered_count = result_df.count()

        logging.info(
            f"Applied year filter (>= {min_year})",
            original_count=original_count,
            filtered_count=filtered_count,
            reduction_pct=round((original_count - filtered_count) / original_count * 100, 2)
            if original_count > 0
            else 0,
        )
    else:
        logging.warning(
            f"Cannot apply year filter: column {year_column} not found",
            available_columns=result_df.columns,
        )

    return result_df
