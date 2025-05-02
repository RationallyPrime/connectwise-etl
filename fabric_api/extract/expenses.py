from __future__ import annotations

"""fabric_api.extract.expenses

Expense extraction helpers.
"""

import logging
from typing import Any

from ..client import ConnectWiseClient
from ..models import ManageInvoiceError, ManageInvoiceExpense
from ..utils import get_parent_agreement_data

__all__ = [
    "get_expense_entries_with_relations",
]

_LOGGER = logging.getLogger(__name__)


def get_expense_entries_with_relations(
    client: ConnectWiseClient,
    invoice_id: int,
    invoice_number: str,
    *,
    max_pages: int | None = 50,
) -> tuple[list[ManageInvoiceExpense], list[ManageInvoiceError]]:
    """Get expense entries for a specific invoice using the direct relationship endpoint."""
    _LOGGER.debug(f"Getting expense entries for invoice {invoice_number} (ID: {invoice_id})")
    
    expense_entries: list[ManageInvoiceExpense] = []
    errors: list[ManageInvoiceError] = []
    
    # Use the direct relationship endpoint for expenses
    endpoint = f"/finance/invoices/{invoice_id}/expenses"
    
    try:
        # Get expense entries directly related to this invoice
        expense_entries_raw: list[dict[str, Any]] = client.paginate(
            endpoint=endpoint,
            entity_name=f"expense entries for invoice {invoice_number}",
            max_pages=max_pages,
        )
        
        _LOGGER.debug(f"Found {len(expense_entries_raw)} expense entries for invoice {invoice_number}")
        
        # Validate and convert to Pydantic models
        for expense_raw in expense_entries_raw:
            # Add invoice number for reference
            expense_raw["invoiceNumber"] = invoice_number
            
            # Validate against our model
            expense = ManageInvoiceExpense(**expense_raw)
            
            if expense:
                expense_entries.append(expense)
                
                # Enhance with agreement data if available
                if expense.agreement_id:
                    parent_id, agr_type = get_parent_agreement_data(client, expense.agreement_id)
                    expense.parent_agreement_id = parent_id  # type: ignore[attr-defined]
                    expense.agreement_type = agr_type  # type: ignore[attr-defined]
    
    except Exception as e:
        _LOGGER.error(f"Error fetching expense entries for invoice {invoice_number}: {str(e)}")
        errors.append(
            ManageInvoiceError(
                invoice_number=invoice_number,
                error_table_id="0",
                error_type="ExpenseExtractionError",
                error_message=str(e),
                table_name="ManageInvoiceExpense",
            )
        )
    
    _LOGGER.info(f"Retrieved {len(expense_entries)} expense entries for invoice {invoice_number}")
    return expense_entries, errors
