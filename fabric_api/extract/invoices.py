from __future__ import annotations

"""fabric_api.extract.invoices - Invoice‑centric extraction routines."""

from typing import Any
import logging

logger = logging.getLogger(__name__)

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
from ._common import safe_validate
from .time import get_time_entries_for_invoice
from .expenses import get_expense_entries_with_relations
from .products import get_products_for_invoice

def get_invoices_with_details(
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
    """High‑level invoice extractor that gets both posted and unposted invoices."""
    errors: list[ManageInvoiceError] = []
    invoice_headers: list[ManageInvoiceHeader] = []
    invoice_lines: list[ManageInvoiceLine] = []
    time_entries: list[ManageTimeEntry] = []
    expenses: list[ManageInvoiceExpense] = []
    products: list[ManageProduct] = []

    # Remove any date conditions to avoid format issues
    clean_params = params.copy()
    if 'conditions' in clean_params:
        if 'invoiceDate' in clean_params['conditions']:
            logger.warning("Removing date conditions to avoid format issues")
            clean_params.pop('conditions')

    # 1. Pull unposted invoices
    logger.info("Extracting unposted invoices...")
    unposted_invoices_raw: list[dict[str, Any]] = client.paginate(
        endpoint="/finance/accounting/unpostedinvoices",
        entity_name="unposted invoices",
        params={"pageSize": 100, **clean_params},
        max_pages=max_pages,
    )
    logger.info(f"Found {len(unposted_invoices_raw)} unposted invoices")
    
    # 2. Pull regular (posted) invoices
    logger.info("Extracting posted invoices...")
    try:
        posted_invoices_raw: list[dict[str, Any]] = client.paginate(
            endpoint="/finance/invoices",
            entity_name="posted invoices",
            params={"pageSize": 100, **clean_params},
            max_pages=max_pages,
        )
        logger.info(f"Found {len(posted_invoices_raw)} posted invoices")
    except Exception as e:
        logger.error(f"Error fetching posted invoices: {str(e)}")
        posted_invoices_raw = []
    
    # 3. Combine both sets of invoices
    all_invoices_raw = unposted_invoices_raw + posted_invoices_raw
    logger.info(f"Processing {len(all_invoices_raw)} total invoices")
    
    # 4. Process all invoices
    for inv in all_invoices_raw:
        invoice_number = str(inv.get("invoiceNumber", ""))
        logger.debug(f"Processing invoice {invoice_number}")
        
        header: ManageInvoiceHeader | None = safe_validate(
            model_cls=ManageInvoiceHeader, 
            raw=inv, 
            errors=errors, 
            invoice_number=invoice_number
        )
        if not header:
            # Failed to validate header – skip full invoice but keep error record
            logger.warning(f"Failed to validate invoice header for {invoice_number}")
            continue

        invoice_headers.append(header)
        
        # Now get related entities for this invoice
        invoice_id = inv.get("id", 0)
        
        # 5. Get time entries for this invoice
        try:
            invoice_time_entries, time_errors = get_time_entries_for_invoice(
                client, 
                invoice_id, 
                invoice_number, 
                max_pages=max_pages
            )
            time_entries.extend(invoice_time_entries)
            errors.extend(time_errors)
        except Exception as e:
            logger.warning(f"Error getting time entries for invoice {invoice_number}: {str(e)}")
            errors.append(
                ManageInvoiceError(
                    invoice_number=invoice_number,
                    error_table_id="TimeEntryAccess",
                    error_message=f"Cannot access time entries: {str(e)}",
                    table_name="ManageTimeEntry",
                )
            )
        
        # 6. Get expenses for this invoice
        try:
            invoice_expenses, expense_errors = get_expense_entries_with_relations(
                client, 
                invoice_id, 
                invoice_number, 
                max_pages=max_pages
            )
            expenses.extend(invoice_expenses)
            errors.extend(expense_errors)
        except Exception as e:
            logger.warning(f"Error getting expenses for invoice {invoice_number}: {str(e)}")
            errors.append(
                ManageInvoiceError(
                    invoice_number=invoice_number,
                    error_table_id="ExpenseAccess",
                    error_message=f"Cannot access expenses: {str(e)}",
                    table_name="ManageInvoiceExpense",
                )
            )
        
        # 7. Get products for this invoice
        try:
            invoice_products, product_errors = get_products_for_invoice(
                client, 
                invoice_id, 
                invoice_number, 
                max_pages=max_pages
            )
            products.extend(invoice_products)
            errors.extend(product_errors)
        except Exception as e:
            logger.warning(f"Error getting products for invoice {invoice_number}: {str(e)}")
            errors.append(
                ManageInvoiceError(
                    invoice_number=invoice_number,
                    error_table_id="ProductAccess",
                    error_message=f"Cannot access products: {str(e)}",
                    table_name="ManageProduct",
                )
            )
    
    # 8. Create line items from time entries, expenses, and products
    line_no = 1
    for te in time_entries:
        invoice_number = getattr(te, "invoice_number", None)
        if invoice_number:
            line_data = {
                "invoice_number": invoice_number,
                "line_no": line_no,
                "description": getattr(te, "note", "Time entry"),
                "time_entry_id": getattr(te, "time_entry_id", 0),
            }
            line_model = safe_validate(
                model_cls=ManageInvoiceLine, 
                raw=line_data, 
                errors=errors, 
                invoice_number=invoice_number
            )
            if line_model:
                invoice_lines.append(line_model)
                line_no += 1
    
    # Create line items from expenses
    for exp in expenses:
        invoice_number = getattr(exp, "invoice_number", None)
        if invoice_number:
            line_data = {
                "invoice_number": invoice_number,
                "line_no": line_no,
                "description": f"Expense: {getattr(exp, 'type', 'Unknown')}",
            }
            line_model = safe_validate(
                model_cls=ManageInvoiceLine, 
                raw=line_data, 
                errors=errors, 
                invoice_number=invoice_number
            )
            if line_model:
                invoice_lines.append(line_model)
                line_no += 1
    
    # Create line items from products
    for prod in products:
        invoice_number = getattr(prod, "invoice_number", None)
        if invoice_number:
            line_data = {
                "invoice_number": invoice_number,
                "line_no": line_no,
                "description": getattr(prod, "description", "Product"),
                "product_id": getattr(prod, "product_id", 0),
            }
            line_model = safe_validate(
                model_cls=ManageInvoiceLine, 
                raw=line_data, 
                errors=errors, 
                invoice_number=invoice_number
            )
            if line_model:
                invoice_lines.append(line_model)
                line_no += 1
    
    logger.info(f"Found {len(invoice_headers)} invoice headers")
    logger.info(f"Found {len(time_entries)} related time entries")
    logger.info(f"Found {len(expenses)} related expenses")
    logger.info(f"Found {len(products)} related products")
    logger.info(f"Created {len(invoice_lines)} invoice lines")
    
    return invoice_headers, invoice_lines, time_entries, expenses, products, errors


# Keep the old function name for backward compatibility
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
    """Backward compatibility wrapper for get_invoices_with_details."""
    return get_invoices_with_details(client=client, max_pages=max_pages, **params)
