from __future__ import annotations
from typing import Any, Mapping, Sequence
import os
from datetime import date, datetime, timedelta
import logging

from .client import ConnectWiseClient
from .models import (
    ManageTimeEntry, 
    ManageProduct, 
    ManageInvoiceError,
    ManageAgreement
)
from .extract import (
    invoices as extract_invoices,
    agreements as extract_agreements,
    time as extract_time,
    expenses as extract_expenses,
    products as extract_products
)
from .transform import transform_and_load, TransformResult

__all__: list[str] = [
    "run_invoices_with_details",
    "run_agreements",
    "run_time_entries",
    "run_expenses",
    "run_products",
    "run_daily",
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
    start: date = _parse_date(text=start_date, default=today - timedelta(days=30))  # Default to 30 days back
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
    ) = extract_invoices.get_unposted_invoices_with_details(
        client, max_pages=max_pages, **filters
    )

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
    
    for time_id in time_entry_ids:
        time_entry, err = extract_time.get_time_entry_with_relations(client, time_id)
        if time_entry:
            time_entries.append(time_entry)
        errors.extend(err)
    
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


def run_daily(
    *,
    client: ConnectWiseClient | None = None,
    start_date: str | None = os.getenv("CW_START_DATE"),
    end_date: str | None = os.getenv("CW_END_DATE"),
    lakehouse_root: str = os.getenv("CW_LAKEHOUSE_ROOT", _DEFAULT_LAKEHOUSE_ROOT),
    mode: str = os.getenv("CW_WRITE_MODE", _DEFAULT_WRITE_MODE),
) -> dict[str, str]:
    """End‑to‑end orchestration for one day (or specified window)."""
    client = _client(client)
    logger.info("Starting ConnectWise daily extraction process")

    # ---------------- Filter for invoice extractor -----------------------
    d_from, d_to = _date_window(start_date, end_date)
    logger.info(f"Extracting data from {d_from} to {d_to}")
    
    cw_filter: dict[str, Any] = {
        "conditions": f"invoiceDate >= [{d_from}] and invoiceDate <= [{d_to}]",
        "pageSize": 1000,
    }

    # Extract invoices with all related details
    logger.info("Extracting invoices with details")
    inv: Mapping[str, Sequence[Any]] = run_invoices_with_details(client=client, **cw_filter)
    
    # Derive agreement IDs from all extracted entities
    agreement_ids: set[int] = set()
    for model in (
        *inv["headers"],
        *inv["lines"],
        *inv["time_entries"],
        *inv["expenses"],
        *inv["products"],
    ):
        agr_id = getattr(model, "agreement_id", None)
        if agr_id:
            agreement_ids.add(int(agr_id))
    
    logger.info(f"Found {len(agreement_ids)} unique agreement IDs")

    # Extract additional time entries not included in invoices
    # This can be customized with specific filtering logic in v0.3.0
    time_entry_ids: list[int] = []
    for te in inv["time_entries"]:
        if te.time_entry_id not in time_entry_ids:
            time_entry_ids.append(te.time_entry_id)
    
    logger.info(f"Found {len(time_entry_ids)} time entries to extract")
    additional_time_entries: Mapping[str, Sequence[Any]] = run_time_entries(
        client=client, time_entry_ids=time_entry_ids
    )
    
    # Extract agreement data
    logger.info(f"Extracting {len(agreement_ids)} agreements")
    agr: Mapping[str, Sequence[Any]] = run_agreements(
        client=client, agreement_ids=list(agreement_ids)
    )

    # ---------------- Transform + Load ----------------------------------
    logger.info("Transforming and loading data to Delta tables")
    delta_paths: TransformResult = transform_and_load(
        invoice_headers=list(inv["headers"]),
        invoice_lines=list(inv["lines"]),
        time_entries=list(inv["time_entries"]) + list(additional_time_entries["time_entries"]),
        expenses=list(inv["expenses"]),
        products=list(inv["products"]),
        errors=list(inv["errors"]) + list(additional_time_entries["errors"]) + list(agr["errors"]),
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    # Also transform and store agreements separately
    # This would need additional transform support in the future
    if agr["agreements"]:
        logger.info(f"Processed {len(agr['agreements'])} agreements")
    
    logger.info("Daily extraction process completed successfully")
    return {
        **delta_paths,
        "agreements_extracted": str(len(agr["agreements"])),
        "time_entries_extracted": str(len(additional_time_entries["time_entries"])),
    }