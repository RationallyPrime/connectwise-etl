from __future__ import annotations
from pyspark.sql.session import classproperty
from pyspark.sql.dataframe import DataFrame
from typing import Any, LiteralString, Mapping, Sequence
import os
from datetime import date, datetime, timedelta
import logging

from pyspark.sql import SparkSession, DataFrame
from .spark_utils import read_table_safely
from .client import ConnectWiseClient
from .connectwise_models import Agreement, TimeEntry, ProductItem
from .extract import (
    invoices as extract_invoices,
    agreements as extract_agreements,
    time as extract_time,
    expenses as extract_expenses,
    products as extract_products,
)
from .transform import transform_and_load, TransformResult

__all__: list[str] = [
    "run_daily",
    "join_invoice_data",
]

# Configure logging
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

_DEFAULT_LAKEHOUSE_ROOT = "/lakehouse/default/Tables/connectwise"
_DEFAULT_WRITE_MODE = "append"


def _parse_date(text: str | None, default: date) -> date:  # noqa: D401 – util
    """Parse *text* (``YYYY‑MM‑DD``) or fall back to *default*."""
    return datetime.strptime(text, "%Y-%m-%d").date() if text else default


def _date_window(
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[date, date]:  # noqa: D401 – util
    """Return *(start, end)* ensuring ``start <= end``."""
    today: date = date.today()
    start: date = _parse_date(
        text=start_date, default=today - timedelta(days=30)
    )  # Default to 30 days back
    end: date = _parse_date(text=end_date, default=today)
    if start > end:
        raise ValueError("start_date cannot be after end_date")
    return start, end


def _client(client: ConnectWiseClient | None) -> ConnectWiseClient:  # noqa: D401 – util
    """Return *client* or instantiate from environment variables."""
    if client:
        return client
    return ConnectWiseClient()  # Direct construction instead of from_env()


# ---------------------------------------------------------------------------
# Extraction wrappers
# ---------------------------------------------------------------------------

def extract_daily_data(
    *,
    client: ConnectWiseClient | None = None,
    start_date: str | None = os.getenv("CW_START_DATE"),
    end_date: str | None = os.getenv("CW_END_DATE"),
    max_pages: int = int(os.getenv("CW_MAX_PAGES", "50")),
) -> dict[str, list[Any]]:
    """Extract ConnectWise data without writing to tables.

    This function extracts all data from ConnectWise but does not write to tables.
    It returns the raw data that can then be passed to write_daily_data.

    Args:
        client: ConnectWiseClient instance to use (will create one if None)
        start_date: Start date for extraction (format: YYYY-MM-DD)
        end_date: End date for extraction (format: YYYY-MM-DD)
        max_pages: Maximum number of pages to retrieve from each API endpoint

    Returns:
        Dictionary containing all extracted data
    """
    client = _client(client)
    logger.info("Starting ConnectWise data extraction process")

    # Process date range if provided
    start, end = _date_window(start_date, end_date)
    date_range_str = f"from {start} to {end}"
    logger.info(f"Extracting data {date_range_str}")

    # Create date conditions for filtering API calls
    date_condition = f"lastUpdated>=[{start}] AND lastUpdated<[{end}]"
    
    # Extract invoices
    logger.info(f"Extracting posted invoices with date filter: {date_condition}")
    posted_invoices = extract_invoices.fetch_posted_invoices_raw(
        client=client, 
        max_pages=max_pages,
        conditions=date_condition
    )
    
    logger.info(f"Extracting unposted invoices with date filter: {date_condition}")
    unposted_invoices = extract_invoices.fetch_unposted_invoices_raw(
        client=client,
        max_pages=max_pages,
        conditions=date_condition
    )
    
    # Extract time entries
    logger.info(f"Extracting time entries with date filter: {date_condition}")
    time_entries = extract_time.fetch_time_entries_raw(
        client=client,
        max_pages=max_pages,
        conditions=date_condition
    )
    
    # Extract expenses
    logger.info(f"Extracting expense entries with date filter: {date_condition}")
    expenses = extract_expenses.fetch_expense_entries_raw(
        client=client,
        max_pages=max_pages,
        conditions=date_condition
    )
    
    # Extract products
    logger.info(f"Extracting product items with date filter: {date_condition}")
    products = extract_products.fetch_product_items_raw(
        client=client,
        max_pages=max_pages,
        conditions=date_condition
    )
    
    # Extract agreements - these don't typically change as often
    logger.info(f"Extracting agreements")
    agreements = extract_agreements.fetch_agreements_raw(
        client=client,
        max_pages=max_pages
    )

    # Collect all data into a single dictionary
    all_data: dict[str, list[Any]] = {
        "posted_invoices": posted_invoices,
        "unposted_invoices": unposted_invoices,
        "time_entries": time_entries,
        "expenses": expenses,
        "products": products,
        "agreements": agreements,
    }

    # Log summary of extracted data
    logger.info("Data extraction completed. Summary:")
    for key, value in all_data.items():
        logger.info(f"- {key}: {len(value)} records")

    return all_data


def write_daily_data(
    data: dict[str, list[Any]],
    *,
    lakehouse_root: str = os.getenv("CW_LAKEHOUSE_ROOT", _DEFAULT_LAKEHOUSE_ROOT),
    mode: str = os.getenv("CW_WRITE_MODE", _DEFAULT_WRITE_MODE),
) -> dict[str, str]:
    """Write the extracted data to Delta tables.

    Args:
        data: Dictionary containing all extracted data (from extract_daily_data)
        lakehouse_root: Path to the lakehouse root where tables are stored
        mode: Delta write mode ('append', 'overwrite', etc.)

    Returns:
        Dictionary mapping table names to their OneLake paths
    """
    logger.info(f"Starting ConnectWise data write process to {lakehouse_root}")

    # Transform and load all data to Delta
    return transform_and_load(
        posted_invoices=data["posted_invoices"],
        unposted_invoices=data["unposted_invoices"],
        time_entries=data["time_entries"],
        expenses=data["expenses"],
        products=data["products"],
        agreements=data["agreements"],
        lakehouse_root=lakehouse_root,
        mode=mode,
    )


def run_daily(
    *,
    client: ConnectWiseClient | None = None,
    start_date: str | None = os.getenv("CW_START_DATE"),
    end_date: str | None = os.getenv("CW_END_DATE"),
    lakehouse_root: str = os.getenv("CW_LAKEHOUSE_ROOT", _DEFAULT_LAKEHOUSE_ROOT),
    mode: str = os.getenv("CW_WRITE_MODE", _DEFAULT_WRITE_MODE),
    max_pages: int = int(os.getenv("CW_MAX_PAGES", "50")),
) -> dict[str, str]:
    """End‑to‑end orchestration for one day (or specified window).

    This function combines extract_daily_data and write_daily_data for backward compatibility.

    Args:
        client: ConnectWiseClient instance to use (will create one if None)
        start_date: Start date for extraction (format: YYYY-MM-DD)
        end_date: End date for extraction (format: YYYY-MM-DD)
        lakehouse_root: Path to the lakehouse root where tables are stored
        mode: Delta write mode ('append', 'overwrite', etc.)
        max_pages: Maximum number of pages to retrieve from each API endpoint

    Returns:
        Dictionary mapping table names to their OneLake paths
    """
    # Extract all data
    all_data = extract_daily_data(
        client=client,
        start_date=start_date,
        end_date=end_date,
        max_pages=max_pages,
    )

    # Write data to tables
    return write_daily_data(
        data=all_data,
        lakehouse_root=lakehouse_root,
        mode=mode,
    )


# SQL-based analysis of invoice data
# ---------------------------------------------------------------------------


def join_invoice_data(
    *,
    spark: SparkSession | None = None,
    lakehouse_root: str = os.getenv("CW_LAKEHOUSE_ROOT", _DEFAULT_LAKEHOUSE_ROOT),
) -> DataFrame:
    """
    Efficiently join invoice data using Spark SQL after it's been loaded into OneLake.

    This approach is much more efficient than making multiple API calls to retrieve
    relationship data. Instead, we load all entities into OneLake tables, then use
    SQL joins to relate them.

    Args:
        spark: SparkSession to use (will create one if None)
        lakehouse_root: Path to the lakehouse root where tables are stored

    Returns:
        A Spark DataFrame with joined invoice data
    """

    # Get or create a SparkSession
    if spark is None:
        # Import in function to help with type checking
        from pyspark.sql import SparkSession as SS

        try:
            # Create a SparkSession explicitly
            builder: classproperty = SS.builder  # type: ignore # PySpark's builder is a classproperty
            spark = builder.appName("ConnectWiseInvoiceAnalysis").getOrCreate()  # type: ignore
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {str(e)}")
            # Re-raise for proper error handling
            raise

    # At this point, spark should not be None
    assert spark is not None, "Failed to create SparkSession"

    # Create table paths
    posted_invoices_path: LiteralString = f"connectwise.postedinvoice"
    unposted_invoices_path: LiteralString = f"connectwise.unpostedinvoice"
    time_entries_path: LiteralString = f"connectwise.timeentry"
    expenses_path: LiteralString = f"connectwise.expenseentry"
    products_path: LiteralString = f"connectwise.productitem"

    # Safely read tables, falling back to empty DataFrames if they don't exist
    logger.info(f"Reading invoice data from OneLake tables")

    # Create an empty DataFrame as a fallback
    empty_df: DataFrame | Any = spark.createDataFrame([], "string")

    # Read posted invoices - required table
    posted_invoices_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=posted_invoices_path, default_value=empty_df
    )
    if posted_invoices_df is None or posted_invoices_df.count() == 0:
        logger.warning(f"No posted invoices found in table {posted_invoices_path}")
        return empty_df

    # Register temporary views for SQL queries
    posted_invoices_df.createOrReplaceTempView("posted_invoices")

    # Read and register other tables - optional relationships
    unposted_invoices_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=unposted_invoices_path, default_value=empty_df
    )
    if unposted_invoices_df is not None and unposted_invoices_df.count() > 0:
        unposted_invoices_df.createOrReplaceTempView("unposted_invoices")
    else:
        # Create empty view to avoid SQL errors
        empty_df.createOrReplaceTempView("unposted_invoices")
        logger.warning(f"No unposted invoices found in table {unposted_invoices_path}")

    times_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=time_entries_path, default_value=empty_df
    )
    if times_df is not None and times_df.count() > 0:
        times_df.createOrReplaceTempView("time_entries")
    else:
        # Create empty view to avoid SQL errors
        empty_df.createOrReplaceTempView("time_entries")
        logger.warning(f"No time entries found in table {time_entries_path}")

    expenses_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=expenses_path, default_value=empty_df
    )
    if expenses_df is not None and expenses_df.count() > 0:
        expenses_df.createOrReplaceTempView("expenses")
    else:
        # Create empty view to avoid SQL errors
        empty_df.createOrReplaceTempView("expenses")
        logger.warning(f"No expenses found in table {expenses_path}")

    products_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=products_path, default_value=empty_df
    )
    if products_df is not None and products_df.count() > 0:
        products_df.createOrReplaceTempView("products")
    else:
        # Create empty view to avoid SQL errors
        empty_df.createOrReplaceTempView("products")
        logger.warning(f"No products found in table {products_path}")

    # Create a comprehensive SQL query to join all invoice data
    sql_query = """
    SELECT 
        p.invoiceNumber,
        p.id as invoice_id,
        p.date as invoice_date,
        p.status.name as invoice_status,
        p.total as invoice_total,
        p.company.id as company_id,
        
        -- Time entry details (if present)
        t.id as time_entry_id,
        t.notes as time_entry_notes,
        t.hours as hours_worked,
        t.date as work_date,
        
        -- Expense details (if present)
        e.id as expense_id,
        e.date as expense_date,
        e.amount as expense_amount,
        
        -- Product details (if present)
        pr.id as product_id,
        pr.description as product_name,
        pr.price as product_price,
        pr.quantity as product_quantity
    FROM 
        posted_invoices p
    LEFT JOIN 
        time_entries t ON p.id = t.invoice.id
    LEFT JOIN 
        expenses e ON p.id = e.invoice.id
    LEFT JOIN 
        products pr ON p.id = pr.invoice.id
    """

    # Execute SQL query to join all data
    logger.info(f"Executing SQL join query on invoice data")

    try:
        result_df = spark.sql(sql_query)

        # Testing if the result can be materialized without errors
        # but limiting to a small number to avoid memory issues
        test_count = min(result_df.limit(10).count(), 10)
        logger.info(f"SQL join query test - retrieved {test_count} sample rows")

        # Get actual row count once we know it works
        row_count = result_df.count()
        logger.info(f"SQL join query returned {row_count} total rows")

        return result_df

    except Exception as e:
        logger.error(f"Error executing SQL join query: {str(e)}")
        # Create a simple DataFrame with the error message
        error_df = spark.createDataFrame(
            [("Error executing SQL join", str(e))], ["error_type", "error_message"]
        )
        return error_df