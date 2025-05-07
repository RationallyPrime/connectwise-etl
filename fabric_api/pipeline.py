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
from .models import ManageTimeEntry, ManageProduct, ManageInvoiceError, ManageAgreement
from .extract import (
    invoices as extract_invoices,
    agreements as extract_agreements,
    time as extract_time,
    expenses as extract_expenses,
    products as extract_products,
)
from .extract._common import safe_validate
from .transform import transform_and_load, TransformResult

__all__: list[str] = [
    "run_invoices_with_details",
    "run_agreements",
    "run_time_entries",
    "run_expenses",
    "run_products",
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


def run_invoices_with_details(
    *,
    client: ConnectWiseClient | None = None,
    max_pages: int | None = 50,
    **filters: Any,
) -> Mapping[str, Sequence[Any]]:
    """Extract unposted invoices + every related entity in one go."""
    client = _client(client)
    (
        headers,
        lines,
        time_entries,
        expenses,
        products,
        errors,
    ) = extract_invoices.get_unposted_invoices_with_details(client, max_pages=max_pages, **filters)

    return {
        "headers": headers,
        "lines": lines,
        "time_entries": time_entries,
        "expenses": expenses,
        "products": products,
        "errors": errors,
    }


def run_agreements(
    *,
    client: ConnectWiseClient | None = None,
    agreement_ids: list[int] | None = None,
) -> Mapping[str, Sequence[Any]]:
    """Bulk fetch agreements for *agreement_ids* (deduplicated)."""
    client = _client(client)
    ids: list[int] = sorted(set(agreement_ids or []))
    if not ids:
        return {"agreements": [], "errors": []}

    agreements: list[ManageAgreement] = []
    errors: list[ManageInvoiceError] = []
    for agr_id in ids:
        agr, err = extract_agreements.get_agreement_with_relations(client, agr_id)
        if agr:
            # Convert the dictionary to a ManageAgreement model
            agreement_model: ManageAgreement = ManageAgreement.model_validate(agr)
            agreements.append(agreement_model)
        errors.extend(err)
    return {"agreements": agreements, "errors": errors}


def run_time_entries(
    *,
    client: ConnectWiseClient | None = None,
    time_entry_ids: list[int],
) -> Mapping[str, Sequence[Any]]:
    """Extract time entries with full relationship data."""
    client = _client(client)
    time_entries: list[ManageTimeEntry] = []
    errors: list[ManageInvoiceError] = []

    # Individual time entries don't have a direct extraction function
    # We'll need to fetch them directly from the API
    for time_id in time_entry_ids:
        try:
            # Fetch the time entry directly from the API
            response = client.get(endpoint=f"/time/entries/{time_id}")
            entry_data = response.json()

            # Map 'id' to 'time_entry_id'
            if "id" in entry_data:
                entry_data["time_entry_id"] = entry_data["id"]

            # Validate the time entry
            time_entry = safe_validate(model_cls=ManageTimeEntry, raw=entry_data, errors=errors)

            if time_entry:
                time_entries.append(time_entry)

        except Exception as e:
            logger.error(f"Error fetching time entry {time_id}: {str(e)}")
            errors.append(
                ManageInvoiceError(
                    error_message=f"Error fetching time entry {time_id}: {str(e)}",
                    invoice_number="",
                    error_table_id=ManageTimeEntry.__name__,
                    table_name=ManageTimeEntry.__name__,
                )
            )

    return {"time_entries": time_entries, "errors": errors}


def run_expenses(
    *,
    client: ConnectWiseClient | None = None,
    invoice_id: int,
    invoice_number: str,
    max_pages: int | None = 50,
) -> Mapping[str, Sequence[Any]]:
    """Extract expenses for a given invoice with relationship data."""
    client = _client(client)
    expenses, errors = extract_expenses.get_expense_entries_with_relations(
        client, invoice_id, invoice_number, max_pages=max_pages
    )

    return {"expenses": expenses, "errors": errors}


def run_products(
    *,
    client: ConnectWiseClient | None = None,
    product_ids: list[int],
    invoice_number: str,
) -> Mapping[str, Sequence[Any]]:
    """Extract products with full relationship data."""
    client = _client(client)
    products: list[ManageProduct] = []
    errors: list[ManageInvoiceError] = []

    for product_id in product_ids:
        product, err = extract_products.get_product_with_relations(
            client, product_id, invoice_number
        )
        if product:
            products.append(product)
        errors.extend(err)

    return {"products": products, "errors": errors}


# Full daily orchestration
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

    # Extract invoices with all related details - no filters
    logger.info(f"Extracting invoices with details (max_pages={max_pages})")

    # No date filter - just get all available invoices
    inv: Mapping[str, Sequence[Any]] = run_invoices_with_details(client=client, max_pages=max_pages)

    # Log the number of items extracted for debugging
    logger.info(f"Extracted {len(inv['headers'])} invoice headers")
    logger.info(f"Extracted {len(inv['lines'])} invoice lines")
    logger.info(f"Extracted {len(inv['time_entries'])} time entries from invoices")
    logger.info(f"Extracted {len(inv['expenses'])} expenses")
    logger.info(f"Extracted {len(inv['products'])} products")
    logger.info(f"Encountered {len(inv['errors'])} errors during extraction")

    if len(inv["headers"]) == 0:
        logger.warning(f"No invoices found. Check your API connection and permissions.")

    # Derive agreement IDs from all extracted entities
    agreement_ids: set[int] = set()
    for model in (
        *inv["headers"],
        *inv["lines"],
        *inv["time_entries"],
        *inv["expenses"],
        *inv["products"],
    ):
        agr_id: Any | None = getattr(model, "agreement_id", None)
        if agr_id:
            agreement_ids.add(int(agr_id))

    logger.info(f"Found {len(agreement_ids)} unique agreement IDs in invoices")

    # Extract all agreements, regardless of whether they're referenced in invoices
    # This ensures we have a complete picture of all agreements
    logger.info(f"Extracting all agreements (max_pages={max_pages})...")

    try:
        # Try to get all agreements directly from API
        all_agreements = client.paginate(
            endpoint="/finance/agreements", entity_name="all agreements", max_pages=max_pages
        )
        logger.info(f"Successfully extracted {len(all_agreements)} agreements directly")

        agreements = []
        agreement_errors = []

        # Process agreements and collect their IDs
        for agreement in all_agreements:
            agreement_id = agreement.get("id")
            if agreement_id:
                # Add to our set of agreement IDs
                agreement_ids.add(int(agreement_id))

                # Convert to our data model
                agreement_obj, errors = extract_agreements.get_agreement_with_relations(
                    client, agreement_id
                )
                if agreement_obj:
                    agreements.append(agreement_obj)
                agreement_errors.extend(errors)

        logger.info(f"Processed {len(agreements)} agreements with {len(agreement_errors)} errors")

    except Exception as e:
        logger.error(f"Error fetching all agreements: {str(e)}")
        logger.warning("Will try to fetch only agreements found in invoices")

        agreements = []
        agreement_errors = []

        # Fallback: extract only those agreements found in invoices
        for agreement_id in agreement_ids:
            try:
                agreement, errors = extract_agreements.get_agreement_with_relations(
                    client, agreement_id
                )
                if agreement:
                    agreements.append(agreement)
                agreement_errors.extend(errors)
            except Exception as e:
                logger.error(f"Error fetching agreement {agreement_id}: {str(e)}")
                agreement_errors.append(
                    ManageInvoiceError(
                        invoice_number="Unknown",
                        error_table_id=f"Agreement-{agreement_id}",
                        error_message=f"Cannot fetch agreement {agreement_id}: {str(e)}",
                        table_name="ManageAgreement",
                    )
                )

    # Collect all data into a single dictionary
    all_data: dict[str, list[Any]] = {
        "invoice_headers": list(inv["headers"]),
        "invoice_lines": list(inv["lines"]),
        "time_entries": list(inv["time_entries"]),
        "expenses": list(inv["expenses"]),
        "products": list(inv["products"]),
        "agreements": agreements,
        "errors": list(inv["errors"]) + agreement_errors,
    }

    # Log summary of extracted data
    logger.info("Data extraction completed. Summary:")
    for key, value in all_data.items():
        if key != "errors":
            logger.info(f"- {key}: {len(value)} records")
    logger.info(f"- errors: {len(all_data['errors'])} records")

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
        invoice_headers=data["invoice_headers"],
        invoice_lines=data["invoice_lines"],
        time_entries=data["time_entries"],
        expenses=data["expenses"],
        products=data["products"],
        agreements=data["agreements"],
        errors=data["errors"],
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
    invoice_headers_path: LiteralString = f"connectwise.invoice_headers"
    invoice_lines_path: LiteralString = f"connectwise.invoice_lines"
    time_entries_path: LiteralString = f"connectwise.time_entries"
    expenses_path: LiteralString = f"connectwise.expenses"
    products_path: LiteralString = f"connectwise.products"

    # Safely read tables, falling back to empty DataFrames if they don't exist
    logger.info(f"Reading invoice data from OneLake tables")

    # Create an empty DataFrame as a fallback
    empty_df: DataFrame | Any = spark.createDataFrame([], "string")

    # Read invoice headers - required table
    headers_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=invoice_headers_path, default_value=empty_df
    )
    if headers_df is None or headers_df.count() == 0:
        logger.warning(f"No invoice headers found in table {invoice_headers_path}")
        return empty_df

    # Register temporary views for SQL queries
    headers_df.createOrReplaceTempView("invoice_headers")

    # Read and register other tables - optional relationships
    lines_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=invoice_lines_path, default_value=empty_df
    )
    if lines_df is not None and lines_df.count() > 0:
        lines_df.createOrReplaceTempView("invoice_lines")
    else:
        # Create empty lines view to avoid SQL errors
        empty_df.createOrReplaceTempView("invoice_lines")
        logger.warning(f"No invoice lines found in table {invoice_lines_path}")

    times_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=time_entries_path, default_value=empty_df
    )
    if times_df is not None and times_df.count() > 0:
        times_df.createOrReplaceTempView("time_entries")
    else:
        # Create empty times view to avoid SQL errors
        empty_df.createOrReplaceTempView("time_entries")
        logger.warning(f"No time entries found in table {time_entries_path}")

    expenses_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=expenses_path, default_value=empty_df
    )
    if expenses_df is not None and expenses_df.count() > 0:
        expenses_df.createOrReplaceTempView("expenses")
    else:
        # Create empty expenses view to avoid SQL errors
        empty_df.createOrReplaceTempView("expenses")
        logger.warning(f"No expenses found in table {expenses_path}")

    products_df: DataFrame | None = read_table_safely(
        spark=spark, full_table_path=products_path, default_value=empty_df
    )
    if products_df is not None and products_df.count() > 0:
        products_df.createOrReplaceTempView("products")
    else:
        # Create empty products view to avoid SQL errors
        empty_df.createOrReplaceTempView("products")
        logger.warning(f"No products found in table {products_path}")

    # Check all the tables' schemas to ensure they have the expected join columns
    # This prevents runtime errors if schemas don't match expected structure
    try:
        # Validate header schema
        if "invoice_number" not in headers_df.columns:
            logger.error("Invoice headers table missing required 'invoice_number' column")
            return empty_df

        # Create a comprehensive SQL query to join all invoice data
        # Using LEFT JOIN to ensure we always get all invoices even if related data is missing
        sql_query = """
        SELECT 
            h.invoice_number,
            h.id as invoice_id,
            h.date as invoice_date,
            h.status as invoice_status,
            h.total as invoice_total,
            h.company_id,
            h.agreement_id,
            
            -- Invoice line details
            l.line_no,
            l.description as line_description,
            l.quantity,
            l.line_amount,
            
            -- Time entry details (if present)
            t.time_entry_id,
            t.notes as time_entry_notes,
            t.hours_worked,
            t.work_date,
            t.work_role_id,
            t.work_type,
            
            -- Expense details (if present)
            e.expense_id,
            e.expense_date,
            e.expense_type,
            e.expense_category,
            e.amount as expense_amount,
            
            -- Product details (if present)
            p.product_id,
            p.product_name,
            p.price as product_price,
            p.quantity as product_quantity
        FROM 
            invoice_headers h
        LEFT JOIN 
            invoice_lines l ON h.invoice_number = l.invoice_number
        LEFT JOIN 
            time_entries t ON l.time_entry_id = t.time_entry_id
        LEFT JOIN 
            expenses e ON l.expense_id = e.expense_id
        LEFT JOIN 
            products p ON l.product_id = p.product_id
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

            # Show the schema of the joined data
            logger.info("Schema of joined invoice data:")
            result_df.printSchema()

            return result_df

        except Exception as e:
            logger.error(f"Error executing SQL join query: {str(e)}")
            # Create a simple DataFrame with the error message
            error_df = spark.createDataFrame(
                [("Error executing SQL join", str(e))], ["error_type", "error_message"]
            )
            return error_df

    except Exception as e:
        logger.error(f"Error preparing or validating tables for SQL join: {str(e)}")
        error_df = spark.createDataFrame(
            [("Error preparing SQL join", str(e))], ["error_type", "error_message"]
        )
        return error_df
