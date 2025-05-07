from __future__ import annotations

"""fabric_api.extract.expenses

Expense extraction helpers.
"""

import logging
from typing import Any

from ..client import ConnectWiseClient
from ..models import ManageInvoiceError, ManageInvoiceExpense
from ..utils import get_parent_agreement_data
from ._common import safe_validate

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
    """Get expense entries for a specific invoice using expense entries endpoint with a filter."""
    _LOGGER.debug(f"Getting expense entries for invoice {invoice_number} (ID: {invoice_id})")
    
    expense_entries: list[ManageInvoiceExpense] = []
    errors: list[ManageInvoiceError] = []
    
    try:
        # WORKAROUND: Instead of using the relationship endpoint, query expenses
        # directly with a filter for the invoice ID
        query_params = {
            "pageSize": 1000,  # Use a larger page size to minimize API calls
            "conditions": f"invoice/id={invoice_id}"  # Filter by invoice ID
        }
        
        # Get expense entries filtered by invoice ID
        expense_entries_raw: list[dict[str, Any]] = client.paginate(
            endpoint="/expense/entries",
            entity_name=f"expense entries for invoice {invoice_number}",
            params=query_params,
            max_pages=max_pages,
        )
        
        _LOGGER.debug(f"Found {len(expense_entries_raw)} expense entries for invoice {invoice_number}")
        
        # Transform the entries into ManageInvoiceExpense objects
        for entry in expense_entries_raw:
            try:
                # Add the invoice_number to the entry
                entry["invoice_number"] = invoice_number
                
                # Handle missing line_no field - generate a sequential number
                if "line_no" not in entry and "id" in entry:
                    entry["line_no"] = entry["id"]  # Use ID as fallback
                
                # Fix type field if it's an object instead of string
                if "type" in entry and isinstance(entry["type"], dict):
                    if "name" in entry["type"]:
                        entry["type"] = entry["type"]["name"]
                    elif "id" in entry["type"]:
                        entry["type"] = f"Type-{entry['type']['id']}"
                    else:
                        entry["type"] = "Unknown"
                
                # Validate and create a ManageInvoiceExpense object
                expense = safe_validate(
                    model_cls=ManageInvoiceExpense, 
                    raw=entry, 
                    errors=errors,
                    invoice_number=invoice_number
                )
                
                if expense:
                    # Ensure the invoice_number is set on the model
                    expense.invoice_number = invoice_number
                    
                    # Enhance with agreement data if available
                    if expense.agreement_id:
                        try:
                            parent_id, agr_type = get_parent_agreement_data(client, expense.agreement_id)
                            expense.parent_agreement_id = parent_id  # type: ignore[attr-defined]
                            expense.agreement_type = agr_type  # type: ignore[attr-defined]
                        except Exception as e:
                            _LOGGER.warning(
                                f"Failed to get agreement data for expense entry: {str(e)}"
                            )
                    
                    expense_entries.append(expense)
                
            except Exception as e:
                _LOGGER.error(f"Error processing expense entry: {str(e)}")
                errors.append(
                    ManageInvoiceError(
                        error_message=f"Error processing expense entry: {str(e)}",
                        invoice_number=invoice_number,
                        error_table_id=ManageInvoiceExpense.__name__,
                        table_name=ManageInvoiceExpense.__name__,
                    )
                )
                
        _LOGGER.info(f"Retrieved {len(expense_entries)} expense entries for invoice {invoice_number}")
        return expense_entries, errors
    
    except Exception as e:
        error_msg = f"Error fetching expense entries for invoice {invoice_number}: {str(e)}"
        _LOGGER.error(error_msg)
        errors.append(
            ManageInvoiceError(
                invoice_number=invoice_number,
                error_table_id="0",
                error_type="ExpenseExtractionError",
                error_message=error_msg,
                table_name="ManageInvoiceExpense",
            )
        )
        return [], errors
