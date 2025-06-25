"""Date dimension and temporal utilities for ETL processing.

Generic date-related functions that can be reused across any data source.
"""

import logging
from datetime import datetime

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def generate_date_dimension(
    spark: SparkSession,
    start_date: str,
    end_date: str | None = None,
    fiscal_year_start_month: int = 1,
) -> DataFrame:
    """Generate a comprehensive date dimension with calendar and fiscal hierarchies.

    Args:
        spark: Spark session
        start_date: Start date for the dimension (YYYY-MM-DD format)
        end_date: End date for the dimension (defaults to current date)
        fiscal_year_start_month: Month when fiscal year starts (1=Jan, 2=Feb, etc.)

    Returns:
        DataFrame with date dimension
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Validate input parameters
    if fiscal_year_start_month < 1 or fiscal_year_start_month > 12:
        raise ValueError("fiscal_year_start_month must be between 1 and 12")

    # Generate date range using Spark SQL
    date_df = spark.sql(f"""
        SELECT EXPLODE(SEQUENCE(
            TO_DATE('{start_date}'),
            TO_DATE('{end_date}'),
            INTERVAL 1 DAY
        )) as Date
    """)

    # Add DateKey (yyyyMMdd format) for joins
    date_df = date_df.withColumn("DateKey", F.date_format("Date", "yyyyMMdd").cast("int"))

    # Standard calendar hierarchy
    date_df = date_df.withColumn("CalendarYear", F.year("Date"))
    date_df = date_df.withColumn("CalendarQuarterNo", F.quarter("Date"))
    date_df = date_df.withColumn(
        "CalendarQuarter", F.concat(F.lit("Q"), F.col("CalendarQuarterNo"))
    )
    date_df = date_df.withColumn("CalendarMonthNo", F.month("Date"))
    date_df = date_df.withColumn("CalendarMonth", F.date_format("Date", "MMMM"))
    date_df = date_df.withColumn("CalendarDay", F.dayofmonth("Date"))

    # Week hierarchy
    date_df = date_df.withColumn("WeekNumber", F.weekofyear("Date"))
    date_df = date_df.withColumn("WeekDay", F.date_format("Date", "EEEE"))
    date_df = date_df.withColumn("WeekDayNo", F.dayofweek("Date"))

    # YearMonth for sorting and filtering
    date_df = date_df.withColumn("YearMonth", F.date_format("Date", "yyyy-MM"))
    date_df = date_df.withColumn("YearMonthNo", F.expr("CalendarYear * 100 + CalendarMonthNo"))

    # Add fiscal year calculations based on fiscal_year_start_month
    date_df = date_df.withColumn(
        "FiscalYear",
        F.when(F.month("Date") >= fiscal_year_start_month, F.year("Date") + 1).otherwise(
            F.year("Date")
        ),
    )

    # Calculate fiscal quarter (1-4) based on fiscal year start
    date_df = date_df.withColumn(
        "FiscalQuarterNo",
        F.expr(f"mod(floor((month(Date) - {fiscal_year_start_month} + 12) % 12 / 3) + 1, 4) + 1"),
    )

    # Create FiscalQuarter as a formatted string (e.g., "FQ1")
    date_df = date_df.withColumn("FiscalQuarter", F.concat(F.lit("FQ"), F.col("FiscalQuarterNo")))

    # Calculate fiscal month (1-12) based on fiscal year start
    date_df = date_df.withColumn(
        "FiscalMonthNo",
        F.expr(f"mod((month(Date) - {fiscal_year_start_month} + 12), 12) + 1"),
    )

    # Add IsStartOfMonth, IsEndOfMonth, etc. indicators
    date_df = date_df.withColumn("IsStartOfMonth", F.dayofmonth("Date") == 1)
    date_df = date_df.withColumn(
        "IsEndOfMonth", F.dayofmonth("Date") == F.dayofmonth(F.last_day("Date"))
    )
    date_df = date_df.withColumn(
        "IsWeekend", (F.dayofweek("Date") == 1) | (F.dayofweek("Date") == 7)
    )

    # Add previous/next date keys for easy navigation
    date_df = date_df.withColumn(
        "PreviousDateKey", F.date_format(F.date_sub("Date", 1), "yyyyMMdd").cast("int")
    )
    date_df = date_df.withColumn(
        "NextDateKey", F.date_format(F.date_add("Date", 1), "yyyyMMdd").cast("int")
    )

    logger.info(f"Date dimension generated: {date_df.count()} records")
    return date_df


def create_date_spine(
    spark: SparkSession, start_date: str, end_date: str | None = None, frequency: str = "month"
) -> DataFrame:
    """Create a date spine for period-based facts.

    Args:
        spark: SparkSession
        start_date: Start date for spine generation
        end_date: End date (defaults to current date)
        frequency: 'month' or 'day'

    Returns:
        DataFrame with period columns (all DATE type)
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if frequency == "month":
        return spark.sql(f"""
            SELECT
                TO_DATE(DATE_FORMAT(date_col, 'yyyy-MM-01')) as period_start,
                LAST_DAY(date_col) as period_end,
                YEAR(date_col) as year,
                MONTH(date_col) as month,
                QUARTER(date_col) as quarter
            FROM (
                SELECT EXPLODE(SEQUENCE(
                    TO_DATE('{start_date}'),
                    TO_DATE('{end_date}'),
                    INTERVAL 1 MONTH
                )) as date_col
            )
        """).distinct()
    else:  # daily
        return spark.sql(f"""
            SELECT
                date_col as period_start,
                date_col as period_end,
                YEAR(date_col) as year,
                MONTH(date_col) as month,
                QUARTER(date_col) as quarter
            FROM (
                SELECT EXPLODE(SEQUENCE(
                    TO_DATE('{start_date}'),
                    TO_DATE('{end_date}'),
                    INTERVAL 1 DAY
                )) as date_col
            )
        """)


def add_date_key(df: DataFrame, date_column: str, key_name: str | None = None) -> DataFrame:
    """Add integer date key (YYYYMMDD) for a date column.

    Args:
        df: Source DataFrame
        date_column: Name of the date column
        key_name: Name for the key column (defaults to {date_column}SK)

    Returns:
        DataFrame with date key added
    """
    if not key_name:
        key_name = f"{date_column}SK"

    return df.withColumn(key_name, F.date_format(F.col(date_column), "yyyyMMdd").cast("int"))
