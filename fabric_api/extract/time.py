from __future__ import annotations

"""fabric_api.extract.time

Time entry extraction helpers.
"""

from logging import getLogger, Logger
from typing import Any

from ..client import ConnectWiseClient
from ..models import ManageTimeEntry, ManageInvoiceError
from ..utils import is_timapottur_agreement, get_parent_agreement_data
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
    """Get time entries for an invoice using filtered queries."""
    time_entries: list[ManageTimeEntry] = []
    errors: list[ManageInvoiceError] = []
    
    try:
        # Use the standard time entries endpoint with a filter for this invoice
        conditions = f"invoice/id={invoice_id}"
        
        # Get time entries for this invoice
        entries = client.paginate(
            endpoint="/time/entries",
            entity_name=f"time entries for invoice {invoice_number} (API ID: {invoice_id})",
            params={"conditions": conditions},
            max_pages=max_pages,
        )
        
        # Transform the entries into ManageTimeEntry objects
        for entry in entries:
            try:
                # Map 'id' to 'time_entry_id' which is required by our model
                if "id" in entry:
                    entry["time_entry_id"] = entry["id"]
                
                # Add the invoice_number to the entry
                entry["invoice_number"] = invoice_number
                
                # Validate and create a ManageTimeEntry object
                time_entry = safe_validate(
                    model_cls=ManageTimeEntry, 
                    raw=entry, 
                    errors=errors,
                    invoice_number=invoice_number
                )
                
                if time_entry:
                    # Ensure the invoice_number is set on the model
                    time_entry.invoice_number = invoice_number
                    
                    # Enhance with agreement data
                    if time_entry.agreement_id:
                        try:
                            parent_id, agr_type = get_parent_agreement_data(
                                client, time_entry.agreement_id
                            )
                            time_entry.parent_agreement_id = parent_id
                            time_entry.agreement_type = agr_type
                        except Exception as e:
                            _LOGGER.warning(
                                f"Failed to get agreement data for time entry {time_entry.id}: {str(e)}"
                            )
                    
                    # Apply skip rule for "Tímapottur" agreements
                    if not is_timapottur_agreement(time_entry.agreement_type or ""):
                        time_entries.append(time_entry)
                
            except Exception as e:
                _LOGGER.error(f"Error processing time entry: {str(e)}")
                errors.append(
                    ManageInvoiceError(
                        error_message=f"Error processing time entry: {str(e)}",
                        invoice_number=invoice_number,
                        error_table_id=ManageTimeEntry.__name__,
                        table_name=ManageTimeEntry.__name__,
                    )
                )
                
        _LOGGER.info(f"Retrieved {len(time_entries)} time entries for invoice {invoice_number} (API ID: {invoice_id})")
        return time_entries, errors
    
    except Exception as e:
        error_msg = f"Error fetching time entries for invoice {invoice_number} (API ID: {invoice_id}): {str(e)}"
        _LOGGER.error(error_msg)
        errors.append(
            ManageInvoiceError(
                invoice_number=invoice_number,
                error_table_id="0",
                error_type="TimeEntryExtractionError",
                error_message=error_msg,
                table_name="ManageTimeEntry",
            )
        )
    
    _LOGGER.info(f"Retrieved {len(time_entries)} time entries for invoice {invoice_number} (API ID: {invoice_id})")
    return time_entries, errors
