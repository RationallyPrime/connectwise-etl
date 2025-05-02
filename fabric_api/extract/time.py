from __future__ import annotations

"""fabric_api.extract.time

Time entry extraction helpers.
"""

from logging import getLogger, Logger
from typing import Any

from ..client import ConnectWiseClient
from ..models import ManageTimeEntry, ManageInvoiceError
from ._common import safe_validate

__all__ = [
    "get_time_entries_for_invoice",
]

_LOGGER: Logger = getLogger(name=__name__)


def get_time_entries_for_invoice(
    client: ConnectWiseClient,
    invoice_id: int,
    invoice_number: str,
    *,
    max_pages: int | None = 50,
) -> tuple[list[ManageTimeEntry], list[ManageInvoiceError]]:
    """Get time entries for a specific invoice using the direct relationship endpoint."""
    _LOGGER.debug(f"Getting time entries for invoice {invoice_number} (ID: {invoice_id})")
    
    time_entries: list[ManageTimeEntry] = []
    errors: list[ManageInvoiceError] = []
    
    # Use the direct relationship endpoint for time entries
    endpoint = f"/finance/invoices/{invoice_id}/timeentries"
    
    try:
        # Get time entries directly related to this invoice
        time_entries_raw: list[dict[str, Any]] = client.paginate(
            endpoint=endpoint,
            entity_name=f"time entries for invoice {invoice_number}",
            max_pages=max_pages,
        )
        
        _LOGGER.debug(f"Found {len(time_entries_raw)} time entries for invoice {invoice_number}")
        
        # Validate and convert to Pydantic models
        for time_entry_raw in time_entries_raw:
            # Add invoice number for reference
            time_entry_raw["invoiceNumber"] = invoice_number
            
            # Validate against our model
            time_entry = safe_validate(
                ManageTimeEntry, 
                time_entry_raw, 
                errors=errors, 
                invoice_number=invoice_number
            )
            
            if time_entry:
                time_entries.append(time_entry)
    
    except Exception as e:
        _LOGGER.error(f"Error fetching time entries for invoice {invoice_number}: {str(e)}")
        errors.append(
            ManageInvoiceError(
                invoice_number=invoice_number,
                error_table_id="0",
                error_type="TimeEntryExtractionError",
                error_message=str(e),
                table_name="ManageTimeEntry",
            )
        )
    
    _LOGGER.info(f"Retrieved {len(time_entries)} time entries for invoice {invoice_number}")
    return time_entries, errors
