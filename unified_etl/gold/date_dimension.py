# unified_etl/gold/date_dimension.py
"""
Date dimension table creation for gold layer.
Creates a comprehensive date dimension with business periods and calendar attributes.
"""

from datetime import date, datetime, timedelta
from typing import List

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from unified_etl.utils import logging
from unified_etl.utils.naming import construct_table_path


def create_date_dimension(
    spark: SparkSession,
    start_date: date | None = None,
    end_date: date | None = None,
    fiscal_year_start_month: int = 1
) -> DataFrame:
    """
    Create a comprehensive date dimension table.
    
    Args:
        spark: SparkSession
        start_date: Start date for dimension (default: 10 years ago)
        end_date: End date for dimension (default: 10 years from now)
        fiscal_year_start_month: Month when fiscal year starts (1-12)
    
    Returns:
        DataFrame with date dimension data
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=365 * 10)  # 10 years ago
    
    if end_date is None:
        end_date = date.today() + timedelta(days=365 * 10)  # 10 years from now
    
    with logging.span("create_date_dimension", start_date=str(start_date), end_date=str(end_date)):
        # Create date range
        date_list = []
        current_date = start_date
        
        while current_date <= end_date:
            date_list.append(current_date)
            current_date += timedelta(days=1)
        
        logging.info(f"Generating date dimension with {len(date_list)} dates")
        
        # Create DataFrame with date attributes
        date_data = []
        
        for dt in date_list:
            # Basic date attributes
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
            if month >= fiscal_year_start_month:
                fiscal_year = year
            else:
                fiscal_year = year - 1
            
            fiscal_quarter = ((month - fiscal_year_start_month) % 12) // 3 + 1
            
            # Month and day names
            month_name = dt.strftime("%B")
            month_short = dt.strftime("%b")
            day_name = dt.strftime("%A")
            day_short = dt.strftime("%a")
            
            # Business day indicators
            is_weekend = day_of_week in [6, 7]  # Saturday, Sunday
            is_weekday = not is_weekend
            
            # Holiday logic (basic - can be enhanced)
            is_holiday = _is_holiday(dt)
            is_business_day = is_weekday and not is_holiday
            
            # Quarter names
            quarter_name = f"Q{quarter}"
            fiscal_quarter_name = f"FY{fiscal_year} Q{fiscal_quarter}"
            
            # Date formatting
            date_string = dt.strftime("%Y-%m-%d")
            date_display = dt.strftime("%B %d, %Y")
            
            date_data.append((
                date_key,           # DateKey (YYYYMMDD)
                dt,                 # Date
                date_string,        # DateString (YYYY-MM-DD)
                date_display,       # DateDisplay (Month DD, YYYY)
                year,               # Year
                month,              # Month
                day,                # Day
                quarter,            # Quarter
                fiscal_year,        # FiscalYear
                fiscal_quarter,     # FiscalQuarter
                week_of_year,       # WeekOfYear
                day_of_week,        # DayOfWeek (1=Monday)
                day_of_year,        # DayOfYear
                month_name,         # MonthName
                month_short,        # MonthShort
                day_name,           # DayName
                day_short,          # DayShort
                quarter_name,       # QuarterName
                fiscal_quarter_name, # FiscalQuarterName
                is_weekend,         # IsWeekend
                is_weekday,         # IsWeekday
                is_holiday,         # IsHoliday
                is_business_day     # IsBusinessDay
            ))
        
        # Define schema
        schema = StructType([
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
        ])
        
        # Create DataFrame
        date_df = spark.createDataFrame(date_data, schema)
        
        logging.info(f"Date dimension created with {date_df.count()} records")
        
        return date_df


def _is_holiday(dt: date) -> bool:
    """
    Simple holiday detection (US holidays).
    Can be enhanced with more comprehensive holiday logic.
    
    Args:
        dt: Date to check
    
    Returns:
        True if the date is a holiday
    """
    # New Year's Day
    if dt.month == 1 and dt.day == 1:
        return True
    
    # Independence Day
    if dt.month == 7 and dt.day == 4:
        return True
    
    # Christmas Day
    if dt.month == 12 and dt.day == 25:
        return True
    
    # Add more holidays as needed
    return False


def create_or_update_date_dimension_table(
    spark: SparkSession,
    lakehouse_root: str,
    table_name: str = "DateDimension",
    force_recreate: bool = False
) -> bool:
    """
    Create or update the date dimension table in the gold layer.
    
    Args:
        spark: SparkSession
        lakehouse_root: Root path for lakehouse storage
        table_name: Name for the date dimension table
        force_recreate: Whether to force recreation of the table
    
    Returns:
        True if successful, False otherwise
    """
    try:
        table_path = construct_table_path(lakehouse_root, "gold", table_name)
        
        # Check if table exists
        table_exists = spark.catalog.tableExists(table_path)
        
        if table_exists and not force_recreate:
            logging.info(f"Date dimension table already exists: {table_path}")
            return True
        
        # Create date dimension
        date_df = create_date_dimension(spark)
        
        # Write to table
        write_mode = "overwrite" if table_exists or force_recreate else "append"
        
        date_df.write \
            .mode(write_mode) \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path)
        
        logging.info(f"Date dimension table {'recreated' if force_recreate else 'created'}: {table_path}")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to create date dimension table: {e}")
        return False


def get_date_key_for_date(dt: date | datetime | str) -> int:
    """
    Generate a date key (YYYYMMDD) for a given date.
    
    Args:
        dt: Date as date, datetime, or string (YYYY-MM-DD)
    
    Returns:
        Integer date key in YYYYMMDD format
    """
    if isinstance(dt, str):
        dt = datetime.strptime(dt, "%Y-%m-%d").date()
    elif isinstance(dt, datetime):
        dt = dt.date()
    
    return int(dt.strftime("%Y%m%d"))


def add_date_dimension_joins(
    df: DataFrame,
    date_columns: List[str],
    lakehouse_root: str,
    date_dimension_table: str = "DateDimension"
) -> DataFrame:
    """
    Add date dimension joins to a fact table.
    
    Args:
        df: Fact table DataFrame
        date_columns: List of date column names to join with date dimension
        lakehouse_root: Root path for lakehouse storage
        date_dimension_table: Name of the date dimension table
    
    Returns:
        DataFrame with date dimension joins added
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        date_dim_path = construct_table_path(lakehouse_root, "gold", date_dimension_table)
        
        if not spark.catalog.tableExists(date_dim_path):
            logging.warning(f"Date dimension table not found: {date_dim_path}")
            return df
        
        date_dim_df = spark.table(date_dim_path)
        result_df = df
        
        for date_col in date_columns:
            if date_col in df.columns:
                # Create date key column
                date_key_col = f"{date_col}Key"
                
                # Generate date key from date column
                result_df = result_df.withColumn(
                    date_key_col,
                    F.date_format(F.col(date_col), "yyyyMMdd").cast("int")
                )
                
                # Join with date dimension
                alias_suffix = f"_{date_col}"
                date_dim_aliased = date_dim_df.alias(f"dd{alias_suffix}")
                
                result_df = result_df.join(
                    date_dim_aliased,
                    F.col(date_key_col) == F.col(f"dd{alias_suffix}.DateKey"),
                    "left"
                ).select(
                    "*",
                    F.col(f"dd{alias_suffix}.Year").alias(f"{date_col}Year"),
                    F.col(f"dd{alias_suffix}.Quarter").alias(f"{date_col}Quarter"),
                    F.col(f"dd{alias_suffix}.Month").alias(f"{date_col}Month"),
                    F.col(f"dd{alias_suffix}.FiscalYear").alias(f"{date_col}FiscalYear"),
                    F.col(f"dd{alias_suffix}.IsBusinessDay").alias(f"{date_col}IsBusinessDay")
                )
        
        return result_df
        
    except Exception as e:
        logging.error(f"Failed to add date dimension joins: {e}")
        return df