from __future__ import annotations

from fabric_api import schemas
from fabric_api.connectwise_models import PostedInvoice, UnpostedInvoice
from fabric_api.api_utils import get_fields_for_api_call

"""fabric_api.extract.invoices - Invoice‑centric extraction routines."""

from typing import Any, Dict, List
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
    if "conditions" in clean_params:
        if "invoiceDate" in clean_params["conditions"]:
            logger.warning("Removing date conditions to avoid format issues")
            clean_params.pop("conditions")

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

    # 4. Process all invoice headers first
    invoice_id_to_number = {}  # Map invoice IDs to invoice numbers for later lookup
    processed_invoice_ids = []  # Keep track of successfully processed invoice IDs

    for inv in all_invoices_raw:
        invoice_number = str(inv.get("invoiceNumber", ""))
        invoice_id = inv.get("id")

        if invoice_id:
            invoice_id_to_number[invoice_id] = invoice_number

        logger.debug(f"Processing invoice {invoice_number}")

        # Pre-process the invoice data to fix validation issues
        if "project" in inv and isinstance(inv["project"], dict):
            # Convert project field from dictionary to string
            if "name" in inv["project"]:
                inv["project"] = inv["project"]["name"]
            elif "id" in inv["project"]:
                inv["project"] = f"Project-{inv['project']['id']}"
            else:
                inv["project"] = None

        header: ManageInvoiceHeader | None = safe_validate(
            model_cls=ManageInvoiceHeader, raw=inv, errors=errors, invoice_number=invoice_number
        )
        if not header:
            # Failed to validate header – skip full invoice but keep error record
            logger.warning(f"Failed to validate invoice header for {invoice_number}")
            continue

        invoice_headers.append(header)
        processed_invoice_ids.append(invoice_id)

    if not processed_invoice_ids:
        logger.warning("No valid invoices were found to process")
        return invoice_headers, invoice_lines, time_entries, expenses, products, errors

    # 5. Batch fetch time entries instead of one by one
    logger.info(f"Batch fetching time entries for {len(processed_invoice_ids)} invoices")
    try:
        # Create a filter condition for all invoice IDs
        # Use batching to avoid making the URL too long
        batch_size = 50  # Adjust based on typical invoice ID length
        all_time_entries = []
        all_time_errors = []

        # Process in batches
        for i in range(0, len(processed_invoice_ids), batch_size):
            batch_ids = processed_invoice_ids[i : i + batch_size]
            batch_condition = " or ".join([f"invoice/id={invoice_id}" for invoice_id in batch_ids])

            logger.info(
                f"Fetching time entries batch {i // batch_size + 1}/{(len(processed_invoice_ids) + batch_size - 1) // batch_size}"
            )
            batch_entries = client.paginate(
                endpoint="/time/entries",
                entity_name=f"time entries batch {i // batch_size + 1}",
                params={"conditions": batch_condition, "pageSize": 100},
                max_pages=max_pages,
            )

            # Process the batch entries
            for entry in batch_entries:
                try:
                    # Get the invoice ID and number for this entry
                    invoice_id = entry.get("invoice", {}).get("id")
                    invoice_number = invoice_id_to_number.get(invoice_id, "Unknown")

                    # Map 'id' to 'time_entry_id' required by our model
                    if "id" in entry:
                        entry["time_entry_id"] = entry["id"]

                    # Add the invoice_number to the entry
                    entry["invoice_number"] = invoice_number

                    # Validate and create a ManageTimeEntry object
                    time_entry = safe_validate(
                        model_cls=ManageTimeEntry,
                        raw=entry,
                        errors=errors,
                        invoice_number=invoice_number,
                    )

                    if time_entry:
                        # Apply additional processing
                        try:
                            # Get parent agreement data if available
                            if hasattr(time_entry, "agreement_id") and time_entry.agreement_id:
                                parent_id, agr_type = get_parent_agreement_data(
                                    client, time_entry.agreement_id
                                )
                                time_entry.parent_agreement_id = parent_id
                                time_entry.agreement_type = agr_type
                        except Exception as e:
                            logger.warning(
                                f"Failed to get agreement data for time entry {time_entry.id}: {str(e)}"
                            )

                        # Apply skip rule for "Tímapottur" agreements
                        if (
                            not hasattr(time_entry, "agreement_type")
                            or time_entry.agreement_type is None
                            or "Tímapottur" not in time_entry.agreement_type
                        ):
                            all_time_entries.append(time_entry)

                except Exception as e:
                    logger.error(f"Error processing time entry: {str(e)}")
                    all_time_errors.append(
                        ManageInvoiceError(
                            error_message=f"Error processing time entry: {str(e)}",
                            invoice_number=invoice_id_to_number.get(invoice_id, "Unknown"),
                            error_table_id=ManageTimeEntry.__name__,
                            table_name=ManageTimeEntry.__name__,
                        )
                    )

        time_entries.extend(all_time_entries)
        errors.extend(all_time_errors)
        logger.info(
            f"Successfully processed {len(all_time_entries)} time entries across all invoices"
        )

    except Exception as e:
        logger.error(f"Error batch fetching time entries: {str(e)}")
        errors.append(
            ManageInvoiceError(
                invoice_number="BatchFetch",
                error_table_id="TimeEntryAccess",
                error_message=f"Cannot batch access time entries: {str(e)}",
                table_name="ManageTimeEntry",
            )
        )

    # 6. Batch fetch expense entries
    logger.info(f"Batch fetching expenses for {len(processed_invoice_ids)} invoices")
    try:
        # Create a filter condition for all invoice IDs
        batch_size = 50
        all_expenses = []
        all_expense_errors = []

        # Process in batches
        for i in range(0, len(processed_invoice_ids), batch_size):
            batch_ids = processed_invoice_ids[i : i + batch_size]
            batch_condition = " or ".join([f"invoice/id={invoice_id}" for invoice_id in batch_ids])

            logger.info(
                f"Fetching expenses batch {i // batch_size + 1}/{(len(processed_invoice_ids) + batch_size - 1) // batch_size}"
            )
            batch_entries = client.paginate(
                endpoint="/expense/entries",
                entity_name=f"expense entries batch {i // batch_size + 1}",
                params={"conditions": batch_condition, "pageSize": 100},
                max_pages=max_pages,
            )

            # Process the batch entries
            for entry in batch_entries:
                try:
                    # Get the invoice ID and number for this entry
                    invoice_id = entry.get("invoice", {}).get("id")
                    invoice_number = invoice_id_to_number.get(invoice_id, "Unknown")

                    # Preprocess the expense data to match the ManageInvoiceExpense table (PTEManageInvoiceExpense)
                    # Build a properly structured expense entry that matches the table fields
                    expense_data = {
                        "invoice_id": invoice_id,  # Field 1: "Invoice ID"
                        "line_no": len(all_expenses) + 1,  # Field 2: "Line No."
                        "invoice_number": invoice_number,  # Field 12: "Invoice Number"
                    }

                    # Field 3: Type (Text[50])
                    if "type" in entry and isinstance(entry["type"], dict):
                        if "name" in entry["type"]:
                            expense_data["type"] = entry["type"]["name"][:50]  # Limit to 50 chars
                        elif "id" in entry["type"]:
                            expense_data["type"] = f"ExpenseType-{entry['type']['id']}"
                        else:
                            expense_data["type"] = "Unknown"
                    elif "type" in entry and isinstance(entry["type"], str):
                        expense_data["type"] = entry["type"][:50]  # Limit to 50 chars
                    else:
                        expense_data["type"] = "Unknown"

                    # Field 4: Quantity
                    expense_data["quantity"] = float(entry.get("quantity", 1.0))

                    # Field 5: Amount
                    expense_data["amount"] = float(entry.get("amount", 0.0))

                    # Field 6: Employee
                    if "member" in entry and isinstance(entry["member"], dict):
                        expense_data["employee"] = entry["member"].get("identifier", "")[
                            :10
                        ]  # Limit to 10 chars
                    else:
                        expense_data["employee"] = ""

                    # Field 7: Job Journal Line No.
                    expense_data["job_journal_line_no"] = None

                    # Field 8: Work Date - MUST be a date without time component
                    if "date" in entry:
                        try:
                            from datetime import datetime

                            # Parse the date string
                            date_str = entry["date"]
                            # Extract ONLY the date part without any time component
                            if isinstance(date_str, str):
                                if "T" in date_str:
                                    # Format: "2018-10-03T15:57:48Z"
                                    date_part = date_str.split("T")[0]
                                    expense_data["work_date"] = (
                                        date_part  # Just the date "2018-10-03"
                                    )
                                else:
                                    expense_data["work_date"] = date_str
                            elif isinstance(date_str, datetime):
                                # If it's already a datetime object, convert to date
                                expense_data["work_date"] = date_str.date()
                            else:
                                expense_data["work_date"] = date_str
                        except Exception as e:
                            logger.debug(f"Failed to parse date: {str(e)}")
                            expense_data["work_date"] = None

                    # Field 9-11,13: Agreement fields
                    agreement_data = entry.get("agreement", {})
                    if isinstance(agreement_data, dict) and "id" in agreement_data:
                        expense_data["agreement_id"] = agreement_data["id"]
                        expense_data["agreement_number"] = agreement_data.get("name", "")[
                            :10
                        ]  # Limit to 10 chars

                        # Try to get parent agreement and type
                        try:
                            parent_id, agr_type = get_parent_agreement_data(
                                client, agreement_data["id"]
                            )
                            expense_data["parent_agreement_id"] = parent_id
                            expense_data["agreement_type"] = (
                                agr_type[:50] if agr_type else None
                            )  # Limit to 50 chars
                        except Exception as e:
                            logger.debug(f"Failed to get parent agreement: {str(e)}")

                    # For tracking, store the original expense ID
                    if "id" in entry:
                        expense_data["expense_id"] = entry["id"]

                    # Validate and create a ManageInvoiceExpense object
                    expense = safe_validate(
                        model_cls=ManageInvoiceExpense,
                        raw=expense_data,
                        errors=errors,
                        invoice_number=invoice_number,
                    )

                    if expense:
                        all_expenses.append(expense)

                except Exception as e:
                    logger.error(f"Error processing expense entry: {str(e)}")
                    all_expense_errors.append(
                        ManageInvoiceError(
                            error_message=f"Error processing expense entry: {str(e)}",
                            invoice_number=invoice_id_to_number.get(invoice_id, "Unknown"),
                            error_table_id=ManageInvoiceExpense.__name__,
                            table_name=ManageInvoiceExpense.__name__,
                        )
                    )

        expenses.extend(all_expenses)
        errors.extend(all_expense_errors)
        logger.info(
            f"Successfully processed {len(all_expenses)} expense entries across all invoices"
        )

    except Exception as e:
        logger.error(f"Error batch fetching expenses: {str(e)}")
        errors.append(
            ManageInvoiceError(
                invoice_number="BatchFetch",
                error_table_id="ExpenseEntryAccess",
                error_message=f"Cannot batch access expense entries: {str(e)}",
                table_name="ManageInvoiceExpense",
            )
        )

    # 7. Batch fetch product entries
    logger.info(f"Batch fetching products for {len(processed_invoice_ids)} invoices")
    try:
        # Create a filter condition for all invoice IDs
        batch_size = 50
        all_products = []
        all_product_errors = []

        # Process in batches
        for i in range(0, len(processed_invoice_ids), batch_size):
            batch_ids = processed_invoice_ids[i : i + batch_size]
            batch_condition = " or ".join([f"invoice/id={invoice_id}" for invoice_id in batch_ids])

            logger.info(
                f"Fetching products batch {i // batch_size + 1}/{(len(processed_invoice_ids) + batch_size - 1) // batch_size}"
            )
            batch_entries = client.paginate(
                endpoint="/procurement/products",
                entity_name=f"products batch {i // batch_size + 1}",
                params={"conditions": batch_condition, "pageSize": 100},
                max_pages=max_pages,
            )

            # Process the batch entries
            for entry in batch_entries:
                try:
                    # Get the invoice ID and number for this entry
                    invoice_id = entry.get("invoice", {}).get("id")
                    invoice_number = invoice_id_to_number.get(invoice_id, "Unknown")

                    # Map 'id' to 'product_id' required by our model
                    if "id" in entry:
                        entry["product_id"] = entry["id"]

                    # Add the invoice_number to the entry
                    entry["invoice_number"] = invoice_number

                    # Validate and create a ManageProduct object
                    product = safe_validate(
                        model_cls=ManageProduct,
                        raw=entry,
                        errors=errors,
                        invoice_number=invoice_number,
                    )

                    if product:
                        all_products.append(product)

                except Exception as e:
                    logger.error(f"Error processing product entry: {str(e)}")
                    all_product_errors.append(
                        ManageInvoiceError(
                            error_message=f"Error processing product entry: {str(e)}",
                            invoice_number=invoice_id_to_number.get(invoice_id, "Unknown"),
                            error_table_id=ManageProduct.__name__,
                            table_name=ManageProduct.__name__,
                        )
                    )

        products.extend(all_products)
        errors.extend(all_product_errors)
        logger.info(
            f"Successfully processed {len(all_products)} product entries across all invoices"
        )

    except Exception as e:
        logger.error(f"Error batch fetching products: {str(e)}")
        errors.append(
            ManageInvoiceError(
                invoice_number="BatchFetch",
                error_table_id="ProductAccess",
                error_message=f"Cannot batch access product entries: {str(e)}",
                table_name="ManageProduct",
            )
        )

    # 8. Process invoice lines - need to build these from the time entries and expenses
    # The invoice response doesn't include line items directly
    logger.info("Building invoice lines from time entries and expenses")

    # Create invoice lines from time entries
    for entry in time_entries:
        invoice_number = getattr(entry, "invoice_number", "Unknown")
        time_entry_id = getattr(entry, "time_entry_id", None)

        try:
            # Build line data with all required fields matching the Manage Invoice Line table structure
            line_data = {
                "invoice_number": invoice_number,
                "line_no": len(invoice_lines) + 1,  # Use sequential line numbers
                "description": getattr(entry, "notes", "Time Entry")[:50],  # Limit to 50 chars
                "time_entry_id": time_entry_id,
                "quantity": float(getattr(entry, "hours", 1.0)),
                "line_amount": float(getattr(entry, "billable_amount", 0.0)),
                "memo": (getattr(entry, "notes", "") or getattr(entry, "internal_notes", ""))[
                    :250
                ],  # Limit to 250 chars
                "price": float(getattr(entry, "hourly_rate", 0.0))
                if hasattr(entry, "hourly_rate")
                else None,
                "cost": float(getattr(entry, "cost_rate", 0.0))
                if hasattr(entry, "cost_rate")
                else None,
                "product_id": None,  # Not a product
                "item_identifier": None,  # Not an item
                "document_date": getattr(entry, "date", None),  # Use time entry date if available
            }

            # Validate and create a ManageInvoiceLine object
            invoice_line = safe_validate(
                model_cls=ManageInvoiceLine,
                raw=line_data,
                errors=errors,
                invoice_number=invoice_number,
            )

            if invoice_line:
                invoice_lines.append(invoice_line)
            else:
                logger.warning(f"Failed to validate line for time entry {time_entry_id}")
        except Exception as e:
            logger.error(f"Error creating line for time entry {time_entry_id}: {str(e)}")

    # Create invoice lines from expenses
    for expense in expenses:
        invoice_number = getattr(expense, "invoice_number", "Unknown")
        expense_id = getattr(expense, "expense_id", None)

        try:
            # Get expense amount and handle potential division by zero
            amount = float(getattr(expense, "amount", 0.0))
            quantity = float(getattr(expense, "quantity", 1.0))
            if quantity == 0:
                quantity = 1.0

            price = amount / quantity

            # Build line data with all required fields matching the Manage Invoice Line table structure
            line_data = {
                "invoice_number": invoice_number,
                "line_no": len(invoice_lines) + 1,  # Use sequential line numbers
                "description": f"Expense: {getattr(expense, 'type', 'Expense')}"[
                    :50
                ],  # Limit to 50 chars
                "time_entry_id": None,  # Not a time entry
                "quantity": quantity,
                "line_amount": amount,
                "memo": (getattr(expense, "notes", "") or getattr(expense, "description", ""))[
                    :250
                ],  # Limit to 250 chars
                "price": price,
                "cost": getattr(expense, "cost", amount),  # Use amount as cost if not available
                "product_id": None,  # Not a product
                "item_identifier": getattr(
                    expense, "identifier", None
                ),  # Use expense identifier if available
                "document_date": getattr(expense, "date", None),  # Use expense date if available
            }

            # Validate and create a ManageInvoiceLine object
            invoice_line = safe_validate(
                model_cls=ManageInvoiceLine,
                raw=line_data,
                errors=errors,
                invoice_number=invoice_number,
            )

            if invoice_line:
                invoice_lines.append(invoice_line)
            else:
                logger.warning(f"Failed to validate line for expense {expense_id}")
        except Exception as e:
            logger.error(f"Error creating line for expense {expense_id}: {str(e)}")

    # Create invoice lines from products if any
    for product in products:
        invoice_number = getattr(product, "invoice_number", "Unknown")
        product_id = getattr(product, "product_id", None)

        try:
            # Get product amount and handle potential division by zero
            price = float(getattr(product, "price", 0.0))
            quantity = float(getattr(product, "quantity", 1.0))
            if quantity == 0:
                quantity = 1.0

            line_amount = price * quantity

            # Build line data with all required fields matching the Manage Invoice Line table structure
            line_data = {
                "invoice_number": invoice_number,
                "line_no": len(invoice_lines) + 1,  # Use sequential line numbers
                "description": getattr(product, "description", "Product")[:50],  # Limit to 50 chars
                "time_entry_id": None,  # Not a time entry
                "quantity": quantity,
                "line_amount": line_amount,
                "memo": getattr(product, "notes", "")[:250],  # Limit to 250 chars
                "price": price,
                "cost": getattr(product, "cost", None),
                "product_id": product_id,
                "item_identifier": getattr(product, "identifier", None)
                or getattr(product, "catalog_id", None),
                "document_date": getattr(product, "date", None),  # Use product date if available
            }

            # Validate and create a ManageInvoiceLine object
            invoice_line = safe_validate(
                model_cls=ManageInvoiceLine,
                raw=line_data,
                errors=errors,
                invoice_number=invoice_number,
            )

            if invoice_line:
                invoice_lines.append(invoice_line)
            else:
                logger.warning(f"Failed to validate line for product {product_id}")
        except Exception as e:
            logger.error(f"Error creating line for product {product_id}: {str(e)}")

    logger.info(f"Created {len(invoice_lines)} invoice lines from time entries and expenses")

    return invoice_headers, invoice_lines, time_entries, expenses, products, errors


def fetch_unposted_invoices_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Fetch raw unposted invoice data from ConnectWise API using schema-based field selection.
    Targets: /finance/accounting/unpostedinvoices
    Validates against: schemas.UnpostedInvoice
    """
    logger.info("Fetching raw unposted invoices using schema-based field selection")

    fields_str = (
        fields_override if fields_override else get_fields_for_api_call(schemas.UnpostedInvoice, max_depth=2)
    )
    logger.debug(f"Using fields for unposted invoices (with nested objects): {fields_str}")

    raw_unposted_invoices = client.paginate(
        endpoint="/finance/accounting/unpostedinvoices",
        entity_name="unposted invoices",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_unposted_invoices)} raw unposted invoices")
    return raw_unposted_invoices


def fetch_posted_invoices_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Fetch raw posted invoice data from ConnectWise API using schema-based field selection.
    Targets: /finance/invoices
    Validates against: schemas.Invoice
    """
    logger.info("Fetching raw posted invoices using schema-based field selection")

    fields_str = fields_override if fields_override else get_fields_for_api_call(schemas.PostedInvoice, max_depth=2)
    logger.debug(f"Using fields for posted invoices (with nested objects): {fields_str}")

    raw_posted_invoices = client.paginate(
        endpoint="/finance/invoices",
        entity_name="posted invoices",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_posted_invoices)} raw posted invoices")
    return raw_posted_invoices


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
