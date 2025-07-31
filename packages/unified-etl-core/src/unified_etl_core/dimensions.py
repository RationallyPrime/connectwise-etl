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

from .config import DimensionConfig, DimensionMapping, ETLConfig
from .facts import _add_etl_metadata
from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import ETLConfigError, ETLProcessingError

logger = logging.getLogger(__name__)


@with_etl_error_handling(operation="create_dimension_from_column")
def create_dimension_from_column(
    config: ETLConfig,
    dimension_config: DimensionConfig,
    spark: SparkSession,
) -> DataFrame:
    """Create a dimension table from a single column.

    Args:
        config: ETL configuration
        dimension_config: Dimension configuration
        spark: SparkSession

    Returns:
        DataFrame with dimension data
    """
    # Validate config - FAIL FAST
    dimension_config.validate_config()

    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)

    # Read the source table
    source_df = spark.table(dimension_config.source_table)

    # Validate source column exists
    if dimension_config.source_column not in source_df.columns:
        raise ETLProcessingError(
            f"Source column '{dimension_config.source_column}' not found in table",
            code=ErrorCode.GOLD_DIMENSION_ERROR,
            details={
                "column": dimension_config.source_column,
                "table": dimension_config.source_table,
                "available_columns": source_df.columns
            }
        )

    # Extract distinct values with counts
    df = (
        source_df.where(F.col(dimension_config.source_column).isNotNull())
        .groupBy(dimension_config.source_column)
        .agg(F.count("*").alias("usage_count"))
        .withColumnRenamed(dimension_config.source_column, dimension_config.natural_key_column)
        .orderBy(F.desc("usage_count"))
    )

    # Add surrogate key
    window = Window.orderBy(F.desc("usage_count"))
    dim_df = df.withColumn(dimension_config.surrogate_key_column, F.row_number().over(window))

    # Add unknown member if configured
    if dimension_config.add_unknown_member:
        unknown_row = spark.createDataFrame(
            [(dimension_config.unknown_member_key,
              "Unknown",
              0,
              dimension_config.unknown_member_description)],
            [dimension_config.surrogate_key_column,
             dimension_config.natural_key_column,
             "usage_count",
             dimension_config.description_column]
        )
        dim_df = unknown_row.union(
            dim_df.withColumn(
                dimension_config.description_column,
                F.col(dimension_config.natural_key_column)
            )
        )
    else:
        dim_df = dim_df.withColumn(
            dimension_config.description_column,
            F.col(dimension_config.natural_key_column)
        )

    # Build final dimension with required columns
    result = dim_df.select(
        F.col(dimension_config.surrogate_key_column),
        F.col(dimension_config.natural_key_column),
        F.col(dimension_config.description_column),
        F.col("usage_count"),
        F.lit(True).alias("IsActive"),
        F.current_timestamp().alias("EffectiveDate"),
        F.lit(None).cast("timestamp").alias("EndDate"),
    )

    # Add additional columns if specified
    for col in dimension_config.additional_columns:
        if col in source_df.columns:
            # Get the most common value for this additional column
            result = result.join(
                source_df.groupBy(dimension_config.source_column, col)
                .count()
                .withColumnRenamed(dimension_config.source_column, dimension_config.natural_key_column),
                on=dimension_config.natural_key_column,
                how="left"
            ).drop("count")

    # Add ETL metadata if configured
    if dimension_config.add_audit_columns:
        result = _add_etl_metadata(result, layer="gold", source="dimension_generator")

    return result


@with_etl_error_handling(operation="create_all_dimensions")
def create_all_dimensions(
    config: ETLConfig,
    dimension_configs: list[DimensionConfig],
    spark: SparkSession,
) -> dict[str, DataFrame]:
    """Create all dimension tables from configuration.

    Args:
        config: ETL configuration
        dimension_configs: List of dimension configurations
        spark: SparkSession

    Returns:
        Dictionary of dimension name -> DataFrame
    """
    if not dimension_configs:
        raise ETLConfigError(
            "At least one dimension configuration is required",
            code=ErrorCode.CONFIG_MISSING
        )

    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)

    dimensions = {}

    for dim_config in dimension_configs:
        # Validate config
        dim_config.validate_config()

        logger.info(
            f"Creating dimension: dim_{dim_config.name} from {dim_config.source_table}.{dim_config.source_column}"
        )

        # Create dimension DataFrame
        dim_df = create_dimension_from_column(
            config=config,
            dimension_config=dim_config,
            spark=spark,
        )

        # Store in dictionary
        dim_key = f"dim_{dim_config.name}"
        dimensions[dim_key] = dim_df

        # Get table name from config
        table_name = config.get_table_name(
            "gold",
            "shared",  # Dimensions are shared across integrations
            dim_config.name,
            table_type="dim"
        )

        dim_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        logger.info(f"Created {table_name} with {dim_df.count()} values")

    return dimensions




@with_etl_error_handling(operation="create_date_dimension")
def create_date_dimension(
    config: ETLConfig,
    spark: SparkSession,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """Create a date dimension table.

    Args:
        config: ETL configuration
        spark: SparkSession
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with date dimension
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not start_date:
        raise ETLConfigError("start_date is required", code=ErrorCode.CONFIG_MISSING)
    if not end_date:
        raise ETLConfigError("end_date is required", code=ErrorCode.CONFIG_MISSING)

    # Create date range using Spark SQL

    # Generate date sequence using Spark
    date_df = spark.sql(f"""
        SELECT
            date_col as DateKey,
            date_col as Date,
            YEAR(date_col) as Year,
            MONTH(date_col) as Month,
            DAY(date_col) as Day,
            DAYOFWEEK(date_col) as DayOfWeek,
            WEEKDAY(date_col) as Weekday,
            WEEKOFYEAR(date_col) as WeekOfYear,
            QUARTER(date_col) as Quarter,
            DAYOFYEAR(date_col) as DayOfYear,
            DATE_FORMAT(date_col, 'MMMM') as MonthName,
            DATE_FORMAT(date_col, 'MMM') as MonthShortName,
            DATE_FORMAT(date_col, 'EEEE') as DayName,
            DATE_FORMAT(date_col, 'EEE') as DayShortName,
            CASE
                WHEN DAYOFWEEK(date_col) IN (1, 7) THEN TRUE
                ELSE FALSE
            END as IsWeekend,
            CASE
                WHEN MONTH(date_col) = 1 THEN 'Q4'
                WHEN MONTH(date_col) <= 3 THEN 'Q1'
                WHEN MONTH(date_col) <= 6 THEN 'Q2'
                WHEN MONTH(date_col) <= 9 THEN 'Q3'
                ELSE 'Q4'
            END as FiscalQuarter,
            CASE
                WHEN MONTH(date_col) >= 10 THEN YEAR(date_col) + 1
                ELSE YEAR(date_col)
            END as FiscalYear
        FROM (
            SELECT EXPLODE(SEQUENCE(
                TO_DATE('{start_date}', 'yyyy-MM-dd'),
                TO_DATE('{end_date}', 'yyyy-MM-dd'),
                INTERVAL 1 DAY
            )) as date_col
        )
    """)

    # Add ETL metadata
    result = _add_etl_metadata(date_df, layer="gold", source="date_dimension")

    return result


# Utility function to join fact tables with dimensions
@with_etl_error_handling(operation="add_dimension_keys")
def add_dimension_keys(
    config: ETLConfig,
    fact_df: DataFrame,
    dimension_mappings: list[DimensionMapping],
    spark: SparkSession,
) -> DataFrame:
    """Add dimension keys to fact table.

    Args:
        config: ETL configuration
        fact_df: Fact table DataFrame
        dimension_mappings: List of dimension mapping configurations
        spark: SparkSession

    Returns:
        Fact DataFrame with dimension keys added
    """
    if not dimension_mappings:
        return fact_df  # No dimensions to join

    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)

    result_df = fact_df

    for mapping in dimension_mappings:
        # Validate fact column exists
        if mapping.fact_column not in result_df.columns:
            raise ETLProcessingError(
                f"Fact column '{mapping.fact_column}' not found",
                code=ErrorCode.GOLD_DIMENSION_FAILED,
                details={
                    "column": mapping.fact_column,
                    "available_columns": result_df.columns
                }
            )

        # Read dimension table
        dim_df = spark.table(mapping.dimension_table)

        # Validate dimension columns exist
        if mapping.dimension_key_column not in dim_df.columns:
            raise ETLProcessingError(
                f"Dimension key column '{mapping.dimension_key_column}' not found in {mapping.dimension_table}",
                code=ErrorCode.GOLD_DIMENSION_FAILED,
                details={
                    "column": mapping.dimension_key_column,
                    "table": mapping.dimension_table,
                    "available_columns": dim_df.columns
                }
            )

        # Join to get key - use broadcast for small dimension tables
        result_df = result_df.join(
            F.broadcast(dim_df.select(
                F.col(mapping.dimension_key_column),
                F.col(mapping.surrogate_key_column)
            )),
            result_df[mapping.fact_column] == dim_df[mapping.dimension_key_column],
            "left",
        ).drop(mapping.dimension_key_column)

    return result_df
