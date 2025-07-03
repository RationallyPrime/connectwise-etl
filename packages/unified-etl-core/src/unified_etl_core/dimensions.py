"""Generic dimension table generator for enum-like columns.

Creates dimension tables from columns with limited distinct values,
following proper dimensional modeling with surrogate keys.

This is a CORE utility - works for any source system (ConnectWise, BC, etc).
Following CLAUDE.md: Generic where possible, specialized where necessary.
"""

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

# Import ETL metadata function from facts module
from .facts import _add_etl_metadata

logger = logging.getLogger(__name__)


def create_dimension_from_column(
    spark: SparkSession,
    source_table: str,
    column_name: str,
    dimension_name: str,
    include_counts: bool = True,
) -> DataFrame:
    """Create a dimension table from a single column.

    Args:
        spark: SparkSession
        source_table: Source table name (e.g., "silver_cw_timeentry" or "Lakehouse.silver.silver_cw_timeentry")
        column_name: Column to extract values from
        dimension_name: Name for the dimension (without dim_ prefix)
        include_counts: Whether to include usage counts

    Returns:
        DataFrame with dimension data
    """
    # Read the source table dynamically
    source_df = spark.table(source_table)

    # Use DataFrame API instead of SQL query
    df = (
        source_df.where(F.col(column_name).isNotNull())
        .groupBy(column_name)
        .agg(F.count("*").alias("usage_count"))
        .withColumnRenamed(column_name, f"{dimension_name}_code")
        .orderBy(F.desc("usage_count"))
    )

    # Add surrogate key using row_number
    window = Window.orderBy(F.desc("usage_count"))
    dim_df = df.withColumn(f"{dimension_name}_key", F.row_number().over(window))

    # Reorder columns and add metadata
    result = dim_df.select(
        F.col(f"{dimension_name}_key"),
        F.col(f"{dimension_name}_code"),
        F.col("usage_count") if include_counts else F.lit(None).alias("usage_count"),
        F.lit(True).alias("is_active"),
        F.current_timestamp().alias("effective_date"),
        F.lit(None).cast("timestamp").alias("end_date"),
    )

    # Add standard ETL metadata using existing pattern
    result = _add_etl_metadata(result, layer="gold", source="dimension_generator")

    return result


def create_all_dimensions(
    spark: SparkSession,
    dimension_configs: list[tuple[str, str, str]],
    lakehouse_root: str = "/lakehouse/default/Tables/",
) -> dict:
    """Create all dimension tables from configuration.

    Args:
        spark: SparkSession
        dimension_configs: List of (source_table, column_name, dimension_name) tuples
        lakehouse_root: Root path for lakehouse tables

    Returns:
        Dictionary of dimension name -> DataFrame
    """
    dimensions = {}

    for source_table, column_name, dimension_name in dimension_configs:
        logger.info(
            f"Creating dimension: dim_{dimension_name} from {source_table}.{column_name}"
        )

        # Create dimension DataFrame - FAIL FAST on errors
        dim_df = create_dimension_from_column(
            spark=spark,
            source_table=source_table,
            column_name=column_name,
            dimension_name=dimension_name,
        )

        # Store in dictionary
        dimensions[f"dim_{dimension_name}"] = dim_df

        # Write to gold layer using saveAsTable for proper Fabric integration
        table_name = f"Lakehouse.gold.dim_{dimension_name}"
        
        dim_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

        logger.info(f"Created dim_{dimension_name} with {dim_df.count()} values")

    return dimensions


# Generic utilities only - source-specific configs belong in their packages


# Utility function to join fact tables with dimensions
def add_dimension_keys(
    fact_df: DataFrame, spark: SparkSession, dimension_mappings: list[tuple[str, str, str, str]]
) -> DataFrame:
    """Add dimension keys to fact table.

    Args:
        fact_df: Fact table DataFrame
        spark: SparkSession
        dimension_mappings: List of (fact_column, dim_table, dim_code_column, key_column)

    Returns:
        Fact DataFrame with dimension keys added
    """
    result_df = fact_df

    for fact_col, dim_table, dim_code_col, key_col in dimension_mappings:
        # Read dimension table
        dim_df = spark.table(f"gold.{dim_table}")

        # Join to get key - use broadcast for small dimension tables
        result_df = result_df.join(
            F.broadcast(dim_df.select(dim_code_col, key_col)),
            result_df[fact_col] == dim_df[dim_code_col],
            "left",
        ).drop(dim_code_col)

    return result_df
