from __future__ import annotations

"""fabric_api.extract.expenses

Expense extraction helpers.
"""

import logging

from ..client import ConnectWiseClient
from ..models import ManageInvoiceExpense, ManageInvoiceError
from ..utils import get_parent_agreement_data
from ._common import paginate, safe_validate

__all__ = ["get_expense_entries_with_relations"]

_LOGGER = logging.getLogger(__name__)


def get_expense_entries_with_relations(
    client: ConnectWiseClient,
    invoice_id: int,
    invoice_number: str,
    *,
    max_pages: int | None = 50,
) -> tuple[list[ManageInvoiceExpense], list[ManageInvoiceError]]:
    errors: list[ManageInvoiceError] = []
    expenses_raw = paginate(
        client,
        "/expense/entries",
        entity_name="expense entries",
        params={"conditions": f"invoice/id={invoice_id}"},
        max_pages=max_pages,
    )

    expenses: list[ManageInvoiceExpense] = []
    for exp in expenses_raw:
        model = safe_validate(ManageInvoiceExpense, exp, errors=errors, invoice_number=invoice_number)
        if not model:
            continue

        if model.agreement_id:
            parent_id, agr_type = get_parent_agreement_data(client, model.agreement_id)
            model.parent_agreement_id = parent_id  # type: ignore[attr-defined]
            model.agreement_type = agr_type  # type: ignore[attr-defined]
        expenses.append(model)

    return expenses, errors
