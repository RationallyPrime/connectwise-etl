"""
Business Central Global Dimension (GD1-GD8) builders.

Creates dimension tables for BC's 8 global dimensions from DimensionValue table.
Following warehouse schema patterns for dim_GD1 through dim_GD8.
"""

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from unified_etl_core.gold import generate_surrogate_key
from unified_etl_core.utils.base import ErrorCode
from unified_etl_core.utils.decorators import with_etl_error_handling
from unified_etl_core.utils.exceptions import ETLConfigError, ETLProcessingError

from .gold_utils import construct_table_path


@with_etl_error_handling(operation="create_global_dimensions")
def create_global_dimensions(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    dimension_mapping: dict[str, str],
) -> dict[str, DataFrame]:
    """
    Create global dimension tables (GD1-GD8) from DimensionValue table.
    
    Args:
        spark: REQUIRED SparkSession
        silver_path: REQUIRED path to silver layer
        gold_path: REQUIRED path to gold layer  
        dimension_mapping: REQUIRED mapping of GD1-GD8 to dimension codes
                          e.g., {"GD1": "TEAM", "GD2": "PRODUCT", ...}
    
    Returns:
        Dictionary of dimension name to DataFrame
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not silver_path:
        raise ETLConfigError("silver_path is required", code=ErrorCode.CONFIG_MISSING)
    if not gold_path:
        raise ETLConfigError("gold_path is required", code=ErrorCode.CONFIG_MISSING)
    if not dimension_mapping:
        raise ETLConfigError("dimension_mapping is required", code=ErrorCode.CONFIG_MISSING)

    logging.info("Creating BC global dimensions")

    # Load DimensionValue table once
    try:
        dim_values = spark.table(construct_table_path(silver_path, "DimensionValue"))
        logging.info(f"Loaded DimensionValue table: {dim_values.count()} rows")
    except Exception as e:
        raise ETLProcessingError(
            "Failed to load DimensionValue table",
            code=ErrorCode.DATA_ACCESS_ERROR,
            details={"silver_path": silver_path}
        ) from e

    results = {}

    # Process each global dimension
    for gd_name, dim_code in dimension_mapping.items():
        logging.info(f"Processing {gd_name} with dimension code: {dim_code}")

        # Filter for specific dimension code
        gd_df = dim_values.filter(F.col("DimensionCode") == dim_code)

        if gd_df.count() == 0:
            logging.warning(f"No values found for {gd_name} (code: {dim_code})")
            continue

        # Map DimensionValueType to description
        gd_df = gd_df.withColumn(
            "Dimension Value Type Desc",
            F.when(F.col("DimensionValueType") == 0, "Standard")
            .when(F.col("DimensionValueType") == 1, "Heading")
            .when(F.col("DimensionValueType") == 2, "Total")
            .when(F.col("DimensionValueType") == 3, "Begin-Total")
            .when(F.col("DimensionValueType") == 4, "End-Total")
            .otherwise("Unknown")
        )

        # Rename columns to match warehouse schema pattern
        gd_df = gd_df.select(
            F.col("Code").alias(f"{gd_name} Code"),
            F.col("Name").alias(f"{gd_name} Name"),
            F.concat(F.col("Code"), F.lit(" - "), F.col("Name")).alias(gd_name),
            F.col("Consolidation Code").alias("Parent Code"),
            F.col("Totaling"),
            F.col("DimensionValueType").alias("Dimension Value Type"),
            F.col("Dimension Value Type Desc"),
            F.col("$Source"),
            F.col("$Environment"),
            F.col("$Company"),
            F.current_timestamp().alias("$LoadDate")
        )

        # Generate surrogate key
        gd_df = generate_surrogate_key(
            df=gd_df,
            business_keys=[f"{gd_name} Code", "$Company"],
            key_name=f"SK_{gd_name}",
            partition_columns=["$Company"]
        )

        logging.info(f"Created {gd_name} dimension: {gd_df.count()} rows")
        results[f"dim_{gd_name}"] = gd_df

    return results


@with_etl_error_handling(operation="create_date_dimension")
def create_date_dimension(
    spark: SparkSession,
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
) -> DataFrame:
    """
    Create date dimension table following warehouse schema.
    
    Args:
        spark: REQUIRED SparkSession
        start_date: Start date for dimension (default: 2020-01-01)
        end_date: End date for dimension (default: 2030-12-31)
    
    Returns:
        Date dimension DataFrame
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)

    logging.info(f"Creating date dimension from {start_date} to {end_date}")

    # Generate date range
    date_df = spark.sql(f"""
        SELECT 
            date_value as DateValue,
            CAST(DATE_FORMAT(date_value, 'yyyyMMdd') AS INT) as SK_Date,
            DAY(date_value) as Day,
            WEEKOFYEAR(date_value) as Week,
            MONTH(date_value) as Month,
            QUARTER(date_value) as Quarter,
            YEAR(date_value) as Year,
            DAYOFWEEK(date_value) as WeekDay,
            CAST(DATE_FORMAT(date_value, 'yyyyMM') AS INT) as YearMonthKey,
            CAST(CONCAT(YEAR(date_value), QUARTER(date_value)) AS INT) as YearQuarterKey,
            CAST(DATE_FORMAT(date_value, 'yyyyww') AS INT) as YearWeekKey,
            DATE_FORMAT(date_value, 'MMMM') as MonthDesc,
            CONCAT('Q', QUARTER(date_value), ' ', YEAR(date_value)) as QuarterDesc,
            CAST(YEAR(date_value) AS STRING) as YearDesc,
            DATE_FORMAT(date_value, 'EEEE') as WeekDayDesc,
            DATE_FORMAT(date_value, 'yyyy-MM') as YearMonthDesc,
            CONCAT(YEAR(date_value), ' Q', QUARTER(date_value)) as YearQuarterDesc,
            CONCAT('Week ', WEEKOFYEAR(date_value)) as WeekDesc,
            DATE_FORMAT(date_value, 'yyyy-ww') as YearWeekDesc,
            DATE_FORMAT(date_value, 'dd/MM/yyyy') as DateDDMMYYYYDesc,
            DATE_FORMAT(date_value, 'yyyy-MM-dd') as DateYYYYMMDDDesc,
            DATE_TRUNC('month', date_value) as StartOfMonth,
            LAST_DAY(date_value) as EndOfMonth,
            -- Current filters
            CASE WHEN DATE_FORMAT(date_value, 'yyyy-MM') = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM') 
                 THEN 1 ELSE 0 END as CurrentMonthFilter,
            CASE WHEN DATE_FORMAT(date_value, 'yyyy-MM') = DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM') 
                 THEN 'Current Month' ELSE 'Other' END as CurrentMonthFilterDesc,
            CASE WHEN YEAR(date_value) = YEAR(CURRENT_DATE()) 
                 THEN 1 ELSE 0 END as CurrentYearFilter,
            CASE WHEN YEAR(date_value) = YEAR(CURRENT_DATE()) 
                 THEN 'Current Year' ELSE 'Other' END as CurrentYearFilterDesc,
            -- Metadata
            'BC' as MD_Source,
            'BusinessCentral' as MD_Company,
            CURRENT_TIMESTAMP() as MD_Updated
        FROM (
            SELECT EXPLODE(SEQUENCE(
                TO_DATE('{start_date}'), 
                TO_DATE('{end_date}'), 
                INTERVAL 1 DAY
            )) as date_value
        )
    """)

    logging.info(f"Created date dimension: {date_df.count()} rows")
    return date_df


@with_etl_error_handling(operation="create_due_date_dimension")
def create_due_date_dimension(spark: SparkSession) -> DataFrame:
    """
    Create due date dimension for aging analysis.
    
    Args:
        spark: REQUIRED SparkSession
        
    Returns:
        Due date dimension DataFrame
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)

    logging.info("Creating due date dimension")

    # Create due date buckets
    due_date_df = spark.createDataFrame([
        # Future/Not Due
        (-365, 0, "Not Due", 0, "0 - Not Due", 0, "0 - Not Due", 1, "Not Due"),
        # Current
        (0, 0, "Due", 1, "1 - Current", 1, "1 - Current", 2, "Current"),
        # 1-30 days
        (1, 1, "Due", 1, "1 - 1-30 days", 1, "1 - 1-30 days", 3, "1-30 days"),
        (30, 1, "Due", 1, "1 - 1-30 days", 1, "1 - 1-30 days", 3, "1-30 days"),
        # 31-60 days
        (31, 1, "Due", 2, "2 - 31-60 days", 1, "1 - 31-60 days", 4, "31-60 days"),
        (60, 1, "Due", 2, "2 - 31-60 days", 1, "1 - 31-60 days", 4, "31-60 days"),
        # 61-90 days
        (61, 1, "Due", 3, "3 - 61-90 days", 1, "1 - 61-90 days", 5, "61-90 days"),
        (90, 1, "Due", 3, "3 - 61-90 days", 1, "1 - 61-90 days", 5, "61-90 days"),
        # Over 90 days
        (91, 1, "Due", 4, "4 - Over 90 days", 2, "2 - Over 90 days", 6, "Over 90 days"),
        (365, 1, "Due", 4, "4 - Over 90 days", 2, "2 - Over 90 days", 6, "Over 90 days"),
    ], ["Due Days", "Due Not Due", "Due Not Due Desc", "Bucket 30 days",
        "Bucket 30 days Desc", "Bucket 90 days", "Bucket 90 days Desc",
        "Bucket Order", "Bucket Desc"])

    # Add surrogate key
    due_date_df = due_date_df.withColumn("SK_DueDate", F.row_number().over(F.orderBy("Due Days")))

    # Add metadata
    due_date_df = due_date_df.withColumn("$Source", F.lit("BC")) \
        .withColumn("$Environment", F.lit("Production")) \
        .withColumn("$Company", F.lit("ALL")) \
        .withColumn("$LoadDate", F.current_timestamp())

    logging.info(f"Created due date dimension: {due_date_df.count()} rows")
    return due_date_df

