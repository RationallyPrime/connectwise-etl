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
        .withColumnRenamed(column_name, f"{dimension_name}Code")
        .orderBy(F.desc("usage_count"))
    )

    # Add surrogate key using row_number
    window = Window.orderBy(F.desc("usage_count"))
    dim_df = df.withColumn(f"{dimension_name}Key", F.row_number().over(window))

    # Reorder columns and add metadata
    result = dim_df.select(
        F.col(f"{dimension_name}Key"),
        F.col(f"{dimension_name}Code"),
        F.col("usage_count") if include_counts else F.lit(None).alias("usage_count"),
        F.lit(True).alias("isActive"),
        F.current_timestamp().alias("effectiveDate"),
        F.lit(None).cast("timestamp").alias("endDate"),
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
            f"Creating dimension: dim{dimension_name} from {source_table}.{column_name}"
        )

        # Create dimension DataFrame - FAIL FAST on errors
        dim_df = create_dimension_from_column(
            spark=spark,
            source_table=source_table,
            column_name=column_name,
            dimension_name=dimension_name,
        )

        # Store in dictionary
        dimensions[f"dim{dimension_name}"] = dim_df

        # Write to gold layer using saveAsTable for proper Fabric integration
        table_name = f"Lakehouse.gold.dim{dimension_name}"

        dim_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

        logger.info(f"Created dim{dimension_name} with {dim_df.count()} values")

    return dimensions


def create_batch_dimensions(
    spark: SparkSession,
    dimension_configs: list[tuple[str, str, str]],
    lakehouse_root: str = "/lakehouse/default/Tables/",
) -> dict:
    """Create dimensions efficiently by batching operations per source table.

    Args:
        spark: SparkSession
        dimension_configs: List of (source_table, column_name, dimension_name) tuples
        lakehouse_root: Root path for lakehouse tables

    Returns:
        Dictionary of dimension name -> DataFrame
    """
    from collections import defaultdict

    dimensions = {}

    # Group configs by source table for efficient processing
    table_groups = defaultdict(list)
    for source_table, column_name, dimension_name in dimension_configs:
        table_groups[source_table].append((column_name, dimension_name))

    # Process each table once, creating multiple dimensions
    for source_table, columns in table_groups.items():
        logger.info(f"Processing {len(columns)} dimensions from {source_table}")

        # Read source table once
        source_df = spark.table(source_table)

        # Create all dimensions from this table
        for column_name, dimension_name in columns:
            logger.info(f"Creating dim{dimension_name} from {column_name}")

            # Create dimension using already-loaded source_df
            dim_df = _create_dimension_from_dataframe(
                source_df, column_name, dimension_name
            )

            # Store and write
            dimensions[f"dim{dimension_name}"] = dim_df
            table_name = f"Lakehouse.gold.dim{dimension_name}"

            dim_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

            logger.info(f"Created dim{dimension_name} with {dim_df.count()} values")

    return dimensions


def _create_dimension_from_dataframe(
    source_df: DataFrame, column_name: str, dimension_name: str
) -> DataFrame:
    """Create dimension from already-loaded DataFrame."""
    # Use DataFrame API instead of SQL query
    df = (
        source_df.where(F.col(column_name).isNotNull())
        .groupBy(column_name)
        .agg(F.count("*").alias("usage_count"))
        .withColumnRenamed(column_name, f"{dimension_name}Code")
        .orderBy(F.desc("usage_count"))
    )

    # Add surrogate key using row_number
    window = Window.orderBy(F.desc("usage_count"))
    dim_df = df.withColumn(f"{dimension_name}Key", F.row_number().over(window))

    # Reorder columns and add metadata
    result = dim_df.select(
        F.col(f"{dimension_name}Key"),
        F.col(f"{dimension_name}Code"),
        F.col("usage_count"),
        F.lit(True).alias("isActive"),
        F.current_timestamp().alias("effectiveDate"),
        F.lit(None).cast("timestamp").alias("endDate"),
    )

    # Add standard ETL metadata
    result = _add_etl_metadata(result, layer="gold", source="dimension_generator")

    return result


def create_rich_dimension(
    spark: SparkSession,
    dimension_config: dict,
    dimension_name: str,
) -> DataFrame:
    """Create a rich dimension table with additional business context columns.
    
    Args:
        spark: SparkSession
        dimension_config: Dictionary with source_table, primary_column, additional_columns
        dimension_name: Name for the dimension
        
    Returns:
        DataFrame with rich dimension data
    """
    source_table = dimension_config["source_table"]
    primary_column = dimension_config["primary_column"]
    additional_columns = dimension_config.get("additional_columns", {})

    # Get table schema to validate columns exist
    try:
        table_df = spark.table(source_table)
        available_columns = table_df.columns
    except Exception as e:
        logger.error(f"Could not access table {source_table}: {e}")
        raise

    # Extract referenced columns from CASE expressions
    import re
    def extract_column_refs(expr):
        """Extract column names referenced in expressions."""
        # Pattern to match column names (alphanumeric + underscore)
        pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        matches = re.findall(pattern, expr)
        # Filter out SQL keywords and function names
        sql_keywords = {'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'LIKE', 'RLIKE', 'IN', 'OR', 'AND', 'true', 'false'}
        return [m for m in matches if m not in sql_keywords and m in available_columns]

    # Build SQL query with proper GROUP BY handling
    additional_selects = []
    group_by_cols = [primary_column]

    for col_name, col_expr in additional_columns.items():
        if col_expr.startswith("CASE"):
            # Complex expression - extract referenced columns
            additional_selects.append(f"({col_expr}) as {col_name}")
            # Add referenced columns to GROUP BY
            refs = extract_column_refs(col_expr)
            for ref in refs:
                if ref not in group_by_cols:
                    group_by_cols.append(ref)
        else:
            # Simple column reference - check if it exists
            if col_expr in available_columns:
                additional_selects.append(f"{col_expr} as {col_name}")
                if col_expr not in group_by_cols:
                    group_by_cols.append(col_expr)
            else:
                logger.warning(f"Column {col_expr} not found in {source_table}, using NULL")
                additional_selects.append(f"NULL as {col_name}")

    # Build the complete SQL query
    sql_query = f"""
    SELECT 
        {', '.join(group_by_cols)},
        {', '.join(additional_selects) if additional_selects else "'N/A' as placeholder"},
        COUNT(*) as usage_count
    FROM {source_table}
    WHERE {primary_column} IS NOT NULL
    GROUP BY {', '.join(group_by_cols)}
    ORDER BY usage_count DESC
    """

    # Execute query
    df = spark.sql(sql_query)

    # Add surrogate key
    window = Window.orderBy(F.desc("usage_count"))
    dim_df = df.withColumn(f"{dimension_name}Key", F.row_number().over(window))

    # Rename primary column to standard naming
    dim_df = dim_df.withColumnRenamed(primary_column, f"{dimension_name}Code")

    # Add standard dimension metadata
    dim_df = dim_df.withColumn("isActive", F.lit(True))
    dim_df = dim_df.withColumn("effectiveDate", F.current_timestamp())
    dim_df = dim_df.withColumn("endDate", F.lit(None).cast("timestamp"))

    # Add ETL metadata
    result = _add_etl_metadata(dim_df, layer="gold", source="rich_dimension_generator")

    return result


def create_rich_dimensions(
    spark: SparkSession,
    rich_dimension_configs: dict,
    lakehouse_root: str = "/lakehouse/default/Tables/",
) -> dict:
    """Create rich dimension tables with business context.
    
    Args:
        spark: SparkSession
        rich_dimension_configs: Dictionary of dimension_name -> config
        lakehouse_root: Root path for lakehouse tables
        
    Returns:
        Dictionary of dimension name -> DataFrame
    """
    dimensions = {}

    for dimension_name, config in rich_dimension_configs.items():
        logger.info(f"Creating rich dimension: dim{dimension_name}")

        # Create rich dimension
        dim_df = create_rich_dimension(
            spark=spark,
            dimension_config=config,
            dimension_name=dimension_name
        )

        # Store in dictionary
        dimensions[f"dim{dimension_name}"] = dim_df

        # Write to gold layer
        table_name = f"Lakehouse.gold.dim{dimension_name}"
        dim_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

        logger.info(f"Created dim{dimension_name} with {dim_df.count()} values")

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
        # Read dimension table - check if it already has catalog/schema prefix
        if "." in dim_table:
            table_path = dim_table
        else:
            table_path = f"gold.{dim_table}"

        try:
            dim_df = spark.table(table_path)
        except Exception as e:
            # Try with Lakehouse prefix if simple path fails
            try:
                dim_df = spark.table(f"Lakehouse.gold.{dim_table}")
            except:
                logger.error(f"Could not find dimension table {dim_table}: {e}")
                continue

        # Join to get key - use broadcast for small dimension tables
        result_df = result_df.join(
            F.broadcast(dim_df.select(dim_code_col, key_col)),
            result_df[fact_col] == dim_df[dim_code_col],
            "left",
        ).drop(dim_code_col)

    return result_df
