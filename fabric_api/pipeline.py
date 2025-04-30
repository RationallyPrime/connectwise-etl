from __future__ import annotations
from fabric_api.models import ManageInvoiceError, ManageAgreement, ManageInvoiceHeader, ManageInvoiceLine, ManageTimeEntry, ManageInvoiceExpense, ManageProduct
from fabric_api.transform import TransformResult
from typing import cast
"""fabric_api.pipeline – orchestration entry‑points

This module supersedes the old *main.py* CLI script and is aligned with the
**new extraction helpers** that follow the ``*_with_relations`` naming
convention.  All public functions are import‑friendly so Fabric notebooks or
pipelines can invoke them directly.

Key points
~~~~~~~~~~
* Makes **zero** use of ``argparse`` or ``sys.argv`` – configuration is via
  kwargs or env‑vars only.
* Leverages :pyfunc:`fabric_api.extract.invoices.get_unposted_invoices_with_details`.
* Derives agreement IDs from extracted models and fetches them via
  :pyfunc:`fabric_api.extract.agreements.get_agreement_with_relations`.
* Hands everything off to :pyfunc:`fabric_api.transform.transform_and_load` to
  serialise into Delta tables under the configured lakehouse root.

Public API
----------
``run_invoices_with_details``
    Thin wrapper returning a ``dict`` with keys ``headers``, ``lines``,
    ``time_entries``, ``expenses``, ``products`` and ``errors``.
``run_agreements``
    Bulk fetch agreement JSON for a given ID list.
``run_daily``
    One‑shot orchestration covering the typical daily fabrication run.
"""

from datetime import date, datetime
import os
from typing import Any, Mapping, Sequence

from .client import ConnectWiseClient
from .extract import invoices as extract_invoices, agreements as extract_agreements
from .transform import transform_and_load

__all__: list[str] = [
    "run_invoices_with_details",
    "run_agreements",
    "run_daily",
]

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
    start: date = _parse_date(text=start_date, default=today)
    end: date = _parse_date(text=end_date, default=today)
    if start > end:
        raise ValueError("start_date cannot be after end_date")
    return start, end


def _client(client: ConnectWiseClient | None) -> ConnectWiseClient:  # noqa: D401 – util
    """Return *client* or instantiate from ``CW_*`` env‑vars."""
    return client or ConnectWiseClient.from_env()  # type: ignore[attr-defined]

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

    # ---------------- filter for invoice extractor -----------------------
    d_from, d_to = _date_window(start_date, end_date)
    cw_filter: dict[str, Any] = {
        "conditions": f"invoiceDate >= [{d_from}] and invoiceDate <= [{d_to}]",
        "pageSize": 1000,
    }

    inv: Mapping[str, Sequence[Any]] = run_invoices_with_details(client=client, **cw_filter)

    # Derive agreement IDs appearing anywhere in extracted models
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

    # Optionally pull agreement metadata – not passed to transform yet but
    # returned so notebooks can persist elsewhere if needed.
    agr = run_agreements(client=client, agreement_ids=list(agreement_ids))

    # ---------------- transform + load ----------------------------------
    delta_paths: TransformResult = transform_and_load(
        invoice_headers=list(inv["headers"]),
        invoice_lines=list(inv["lines"]),
        time_entries=list(inv["time_entries"]),
        expenses=list(inv["expenses"]),
        products=list(inv["products"]),
        errors=list(inv["errors"]),
        lakehouse_root=lakehouse_root,
        mode=mode,
    )

    return {
        **delta_paths,
        "agreements_extracted": str(len(agr["agreements"]))
    }
