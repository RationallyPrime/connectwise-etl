from __future__ import annotations

from fabric_api.models import ManageInvoiceHeader, ManageInvoiceLine

"""fabric_api.extract.invoices

Invoice‑centric extraction routines.
"""

from typing import Any
import logging

from ..client import ConnectWiseClient
from ..models import (
    ManageInvoiceHeader,
    ManageInvoiceLine,
    ManageTimeEntry,
    ManageInvoiceExpense,
    ManageProduct,
    ManageInvoiceError,
)
from ..utils import get_parent_agreement_data
from ._common import safe_validate, paginate

def get_unposted_invoices_with_details(
    client: ConnectWiseClient,
    *,
    max_pages: int | None = 50,
    **params: Any,
) -> tuple[
    list[ManageInvoiceHeader],
    list[ManageInvoiceLine],
    list[ManageTimeEntry],
    list[ManageInvoiceExpense],
    list[ManageProduct],
    list[ManageInvoiceError],
]:
    """High‑level invoice extractor matching AL ``GetInvoices``.

    Returns typed entity collections ready for the transform layer.
    """
    errors: list[ManageInvoiceError] = []

    # 1. Pull the invoices themselves ------------------------------------------------
    invoices_raw: list[dict[str, Any]] = paginate(
        client,
        endpoint="/finance/accounting/unpostedinvoices",
        entity_name="unposted invoices",
        params={"pageSize": 1000, **params},
        max_pages=max_pages,
    )

    invoice_headers: list[ManageInvoiceHeader] = []
    invoice_lines: list[ManageInvoiceLine] = []
    time_entries: list[ManageTimeEntry] = []
    expenses: list[ManageInvoiceExpense] = []
    products: list[ManageProduct] = []

    for inv in invoices_raw:
        header: ManageInvoiceHeader | None = safe_validate(model_cls=ManageInvoiceHeader, raw=inv, errors=errors, invoice_number=str(inv.get("invoiceNumber", "")))
        if not header:
            # Failed to validate header – skip full invoice but keep error record
            continue

        invoice_headers.append(header)

        # 2. Invoice lines -----------------------------------------------------------
        lines_raw = client.get(f"/finance/invoices/{inv['id']}/lines").json()
        for line in lines_raw:
            line_model: ManageInvoiceLine | None = safe_validate(model_cls=ManageInvoiceLine, raw=line, errors=errors, invoice_number=header.invoice_number)
            if line_model:
                invoice_lines.append(line_model)

        # 3. Time entries ------------------------------------------------------------
        te_raw = client.get(f"/finance/invoices/{inv['id']}/timeentries").json()
        for te in te_raw:
            te_model: ManageTimeEntry | None = safe_validate(model_cls=ManageTimeEntry, raw=te, errors=errors, invoice_number=header.invoice_number)
            if te_model:
                time_entries.append(te_model)

        # 4. Expenses ----------------------------------------------------------------
        exp_raw = client.get(f"/finance/invoices/{inv['id']}/expenses").json()
        for exp in exp_raw:
            exp_model: ManageInvoiceExpense | None = safe_validate(model_cls=ManageInvoiceExpense, raw=exp, errors=errors, invoice_number=header.invoice_number)
            if exp_model:
                expenses.append(exp_model)

        # 5. Products ----------------------------------------------------------------
        prod_raw = client.get(endpoint=f"/finance/invoices/{inv['id']}/products").json()
        for prod in prod_raw:
            prod_model: ManageProduct | None = safe_validate(model_cls=ManageProduct, raw=prod, errors=errors, invoice_number=header.invoice_number)
            if prod_model:
                products.append(prod_model)

    return invoice_headers, invoice_lines, time_entries, expenses, products, errors
