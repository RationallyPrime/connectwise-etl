"""
Core Gold layer utilities - universal dimensional modeling functions only.

Contains ONLY truly generic functions that every system needs:
- Date dimension creation (universal calendar)
- Surrogate key generation (universal pattern)

Business-specific gold logic delegated to individual packages:
- unified-etl-connectwise: ConnectWise gold transformations

Following CLAUDE.md: Generic where possible, specialized where necessary.
"""

import logging
from datetime import date, datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import ETLConfigError, ETLProcessingError


def create_date_dimension(
    spark: SparkSession,
    start_date: date,
    end_date: date,
    fiscal_year_start_month: int = 1,
) -> DataFrame:
    """
    Create universal date dimension. Works for any business domain.

    Args:
        spark: REQUIRED SparkSession
        start_date: REQUIRED start date for dimension
        end_date: REQUIRED end date for dimension
        fiscal_year_start_month: Month when fiscal year starts (1-12)
    """
    if not spark:
        raise ValueError("SparkSession is required")
    if not start_date:
        raise ValueError("start_date is required")
    if not end_date:
        raise ValueError("end_date is required")
    if start_date > end_date:
        raise ValueError("start_date must be before end_date")
    if fiscal_year_start_month < 1 or fiscal_year_start_month > 12:
        raise ValueError("fiscal_year_start_month must be between 1 and 12")

    # Create date dimension
    # Generate date range
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)

    logging.info(f"Generating date dimension: {len(dates)} dates from {start_date} to {end_date}")

    # Create date data with universal business attributes
    date_data = []
    for dt in dates:
        # Basic attributes
        date_key = int(dt.strftime("%Y%m%d"))
        year = dt.year
        month = dt.month
        day = dt.day
        quarter = (month - 1) // 3 + 1

        # Week attributes
        week_of_year = dt.isocalendar()[1]
        day_of_week = dt.weekday() + 1  # Monday = 1
        day_of_year = dt.timetuple().tm_yday

        # Fiscal year calculation
        fiscal_year = year if month >= fiscal_year_start_month else year - 1
        fiscal_quarter = ((month - fiscal_year_start_month) % 12) // 3 + 1

        # Formatted names
        month_name = dt.strftime("%B")
        month_short = dt.strftime("%b")
        day_name = dt.strftime("%A")
        day_short = dt.strftime("%a")
        quarter_name = f"Q{quarter}"
        fiscal_quarter_name = f"FY{fiscal_year} Q{fiscal_quarter}"

        # Universal business indicators
        is_weekend = day_of_week in [6, 7]
        is_weekday = not is_weekend
        is_holiday = _is_us_holiday(dt)  # Basic US holidays - can be extended
        is_business_day = is_weekday and not is_holiday

        # Date formatting
        date_string = dt.strftime("%Y-%m-%d")
        date_display = dt.strftime("%B %d, %Y")

        date_data.append(
            (
                date_key,
                dt,
                date_string,
                date_display,
                year,
                month,
                day,
                quarter,
                fiscal_year,
                fiscal_quarter,
                week_of_year,
                day_of_week,
                day_of_year,
                month_name,
                month_short,
                day_name,
                day_short,
                quarter_name,
                fiscal_quarter_name,
                is_weekend,
                is_weekday,
                is_holiday,
                is_business_day,
            )
        )

    # Define universal schema
    schema = StructType(
        [
            StructField("DateKey", IntegerType(), False),
            StructField("Date", DateType(), False),
            StructField("DateString", StringType(), False),
            StructField("DateDisplay", StringType(), False),
            StructField("Year", IntegerType(), False),
            StructField("Month", IntegerType(), False),
            StructField("Day", IntegerType(), False),
            StructField("Quarter", IntegerType(), False),
            StructField("FiscalYear", IntegerType(), False),
            StructField("FiscalQuarter", IntegerType(), False),
            StructField("WeekOfYear", IntegerType(), False),
            StructField("DayOfWeek", IntegerType(), False),
            StructField("DayOfYear", IntegerType(), False),
            StructField("MonthName", StringType(), False),
            StructField("MonthShort", StringType(), False),
            StructField("DayName", StringType(), False),
            StructField("DayShort", StringType(), False),
            StructField("QuarterName", StringType(), False),
            StructField("FiscalQuarterName", StringType(), False),
            StructField("IsWeekend", BooleanType(), False),
            StructField("IsWeekday", BooleanType(), False),
            StructField("IsHoliday", BooleanType(), False),
            StructField("IsBusinessDay", BooleanType(), False),
        ]
    )

    # Create DataFrame
    date_df = spark.createDataFrame(date_data, schema)
    logging.info(f"Date dimension created: {date_df.count()} records")
    return date_df


def _is_us_holiday(dt: date) -> bool:
    """Basic US holiday detection. Each package can override with country-specific logic."""
    # New Year's Day
    if dt.month == 1 and dt.day == 1:
        return True
    # Independence Day
    if dt.month == 7 and dt.day == 4:
        return True
    # Christmas Day
    return bool(dt.month == 12 and dt.day == 25)


@with_etl_error_handling(operation="generate_surrogate_key")
def generate_surrogate_key(
    df: DataFrame,
    business_keys: list[str],
    key_name: str,
    partition_columns: list[str] | None = None,
    start_value: int = 1,
) -> DataFrame:
    """
    Generate surrogate keys using universal pattern.

    Args:
        df: REQUIRED DataFrame to process
        business_keys: REQUIRED list of business key columns
        key_name: REQUIRED name for the surrogate key column
        partition_columns: Optional columns to partition by (e.g., ["company", "region"])
        start_value: Starting value for surrogate key
    """
    if not df:
        raise ETLConfigError("DataFrame is required", code=ErrorCode.CONFIG_MISSING)
    if not business_keys:
        raise ETLConfigError(
            "business_keys list is required and cannot be empty", code=ErrorCode.CONFIG_MISSING
        )
    if not key_name:
        raise ETLConfigError("key_name is required", code=ErrorCode.CONFIG_MISSING)

    # Validate business keys exist
    missing_keys = [k for k in business_keys if k not in df.columns]
    if missing_keys:
        raise ETLProcessingError(
            f"Business key columns not found in DataFrame: {missing_keys}",
            code=ErrorCode.GOLD_SURROGATE_KEY,
            details={"missing_keys": missing_keys, "available_columns": df.columns},
        )

    # Validate partition columns if provided
    if partition_columns:
        missing_partitions = [k for k in partition_columns if k not in df.columns]
        if missing_partitions:
            raise ETLProcessingError(
                f"Partition columns not found in DataFrame: {missing_partitions}",
                code=ErrorCode.GOLD_SURROGATE_KEY,
                details={"missing_partitions": missing_partitions, "available_columns": df.columns},
            )

    try:
        # Create window spec
        if partition_columns:
            window = Window.partitionBy(*[F.col(k) for k in partition_columns]).orderBy(
                *[F.col(k) for k in business_keys]
            )
        else:
            window = Window.orderBy(*[F.col(k) for k in business_keys])

        # Generate surrogate key
        result_df = df.withColumn(
            key_name, (F.dense_rank().over(window) + F.lit(start_value - 1)).cast("int")
        )

        # Verify key was generated
        if key_name not in result_df.columns:
            raise ETLProcessingError(
                f"Failed to generate surrogate key column: {key_name}",
                code=ErrorCode.GOLD_SURROGATE_KEY,
                details={"key_name": key_name},
            )

        # Log statistics
        key_count = result_df.select(key_name).distinct().count()
        row_count = result_df.count()
        density_pct = round((key_count / row_count) * 100, 2) if row_count > 0 else 0

        logging.info(
            f"Surrogate key '{key_name}': {key_count} unique keys, {row_count} rows, {density_pct}% density"
        )
        return result_df

    except Exception as e:
        raise ETLProcessingError(
            f"Surrogate key generation failed: {e}",
            code=ErrorCode.GOLD_SURROGATE_KEY,
            details={"key_name": key_name, "business_keys": business_keys, "error": str(e)},
        ) from e


@with_etl_error_handling(operation="create_date_dimension_table")
def create_or_update_date_dimension_table(
    spark: SparkSession,
    lakehouse_root: str,
    table_name: str = "DateDimension",
    start_date: date | None = None,
    end_date: date | None = None,
    force_recreate: bool = False,
) -> bool:
    """
    Create or update universal date dimension table.

    Args:
        spark: REQUIRED SparkSession
        lakehouse_root: REQUIRED lakehouse root path
        table_name: Table name for date dimension
        start_date: Start date (defaults to 10 years ago)
        end_date: End date (defaults to 10 years from now)
        force_recreate: Whether to force recreation
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not lakehouse_root:
        raise ETLConfigError("lakehouse_root is required", code=ErrorCode.CONFIG_MISSING)

    try:
        # Set default date range if not provided
        if start_date is None:
            start_date = date.today() - timedelta(days=365 * 10)
        if end_date is None:
            end_date = date.today() + timedelta(days=365 * 10)

        table_path = f"{lakehouse_root}gold/{table_name}"
        table_exists = spark.catalog.tableExists(table_path)

        if table_exists and not force_recreate:
            logging.info(f"Date dimension exists: {table_path}")
            return True

        # Create date dimension
        date_df = create_date_dimension(spark, start_date, end_date)

        # Write table
        write_mode = "overwrite" if table_exists or force_recreate else "append"
        date_df.write.mode(write_mode).option("mergeSchema", "true").saveAsTable(table_path)

        logging.info(f"Date dimension {'recreated' if force_recreate else 'created'}: {table_path}")
        return True

    except Exception as e:
        logging.error(f"Date dimension creation failed: {e}")
        # Since this returns bool, we'll just return False on error instead of re-raising
        return False


@with_etl_error_handling(operation="add_date_key_column")
def add_date_key_column(
    df: DataFrame, date_column: str, date_key_column: str | None = None
) -> DataFrame:
    """
    Add DateKey column from date column. Universal utility.

    Args:
        df: REQUIRED DataFrame
        date_column: REQUIRED source date column name
        date_key_column: Target DateKey column name (defaults to f"{date_column}Key")
    """
    if not df:
        raise ETLConfigError("DataFrame is required", code=ErrorCode.CONFIG_MISSING)
    if not date_column:
        raise ETLConfigError("date_column is required", code=ErrorCode.CONFIG_MISSING)
    if date_column not in df.columns:
        raise ETLProcessingError(
            f"Date column '{date_column}' not found in DataFrame",
            code=ErrorCode.GOLD_DIMENSION_FAILED,
            details={"date_column": date_column, "available_columns": df.columns},
        )

    if date_key_column is None:
        date_key_column = f"{date_column}Key"

    try:
        result_df = df.withColumn(
            date_key_column, F.date_format(F.col(date_column), "yyyyMMdd").cast("int")
        )
        logging.info(f"Generated {date_key_column} from {date_column}")
        return result_df

    except Exception as e:
        raise ETLProcessingError(
            f"DateKey generation failed: {e}",
            code=ErrorCode.GOLD_DIMENSION_FAILED,
            details={
                "date_column": date_column,
                "date_key_column": date_key_column,
                "error": str(e),
            },
        ) from e


def add_etl_metadata(df: DataFrame, layer: str = "gold", source: str | None = None) -> DataFrame:
    """
    Add universal ETL metadata columns.

    Args:
        df: REQUIRED DataFrame
        layer: ETL layer name (bronze/silver/gold)
        source: Optional source system name
    """
    if not df:
        raise ValueError("DataFrame is required")

    metadata_df = df.withColumn(f"_etl_{layer}_processed_at", F.current_timestamp())

    if source:
        metadata_df = metadata_df.withColumn("_etl_source", F.lit(source))

    metadata_df = metadata_df.withColumn(
        "_etl_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
    )

    return metadata_df
