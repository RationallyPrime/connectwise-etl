from datetime import datetime
from typing import Any

from .client import ConnectWiseClient

"""
extract.py - Functions for extracting data from ConnectWise Manage API

This module contains functions for extracting data from the ConnectWise Manage API.
Each function uses the ConnectWiseClient to make API calls and process the results.
"""

# --- Time Entry Functions ---


def get_time_entries(
    client: ConnectWiseClient, max_pages: int | None = 50, **filters: Any
) -> list[dict[str, Any]]:
    """List time entries from ConnectWise.

    Args:
        client: ConnectWiseClient instance
        max_pages: Maximum number of pages to retrieve (default: 50)
        **filters: Additional filter parameters

    Returns:
        list[dict[str, Any]]: List of time entry dictionaries
    """
    return client.get_entity_data(
        "/time/entries", filters, entity_name="time entries", max_pages=max_pages
    )


def get_time_entry(client: ConnectWiseClient, time_entry_id: int) -> dict[str, Any]:
    """Retrieve details for a specific time entry.

    Args:
        client: ConnectWiseClient instance
        time_entry_id: ID of the time entry to retrieve

    Returns:
        dict[str, Any]: Time entry details
    """
    response = client.get(f"/time/entries/{time_entry_id}")
    return response.json()


def get_work_role(client: ConnectWiseClient, work_role_id: int) -> dict[str, Any]:
    """Retrieve details for a specific work role.

    Args:
        client: ConnectWiseClient instance
        work_role_id: ID of the work role to retrieve

    Returns:
        dict[str, Any]: Work role details
    """
    response = client.get(f"/time/workRoles/{work_role_id}")
    return response.json()


# --- Invoice & Financial Functions ---


def get_invoices(
    client: ConnectWiseClient, max_pages: int | None = 50, **params: Any
) -> list[dict[str, Any]]:
    """List invoices from ConnectWise.

    Args:
        client: ConnectWiseClient instance
        max_pages: Maximum number of pages to retrieve (default: 50)
        **params: Additional filter parameters

    Returns:
        list[dict[str, Any]]: List of invoice dictionaries
    """
    return client.get_entity_data(
        "/finance/invoices", params, entity_name="invoices", max_pages=max_pages
    )


def get_unposted_invoices(
    client: ConnectWiseClient, page_size: int = 1000, max_pages: int | None = 50, **params: Any
) -> list[dict[str, Any]]:
    """List unposted invoices from ConnectWise.

    Args:
        client: ConnectWiseClient instance
        page_size: Number of results per page (default: 1000)
        max_pages: Maximum number of pages to retrieve (default: 50)
        **params: Additional filter parameters

    Returns:
        list[dict[str, Any]]: List of unposted invoice dictionaries
    """
    params["pageSize"] = page_size
    return client.get_entity_data(
        "/finance/accounting/unpostedinvoices",
        params,
        entity_name="unposted invoices",
        max_pages=max_pages,
    )


def export_invoice(
    client: ConnectWiseClient,
    billing_log_id: int,
    thru_date: str | None = None,
    summarize: str = "Detailed",
) -> dict[str, Any]:
    """Export invoice details.

    Args:
        client: ConnectWiseClient instance
        billing_log_id: The ID of the billing log to export
        thru_date: Optional date for the export in format YYYY-MM-DD
        summarize: Summarization level (default: "Detailed")

    Returns:
        dict[str, Any]: Exported invoice data
    """
    # Create batch identifier using current date/time
    batch_id = datetime.now().strftime("%Y%m%d-%H%M%S")

    # Set default thru_date to first day of next month if not provided
    if thru_date is None:
        today = datetime.now()
        first_of_next_month = datetime(
            today.year + (today.month == 12), ((today.month % 12) + 1), 1
        )
        thru_date = first_of_next_month.strftime("%Y-%m-%d") + "T00:00:00Z"

    # Create an array with the billing log ID
    invoice_ids = [billing_log_id]

    # Prepare the request payload
    data = {
        "batchIdentifier": batch_id,
        "locationId": 0,
        "includedInvoiceIds": invoice_ids,
        "thruDate": thru_date,
        "summarizeInvoices": summarize,
        "exportInvoicesFlag": True,
    }

    # Make the API request
    response = client.post("/finance/accounting/export", data=data)
    return response.json()


def manage_invoice_batches(
    client: ConnectWiseClient, batch_ids: list[int], summarize_expenses: bool = True
) -> dict[str, Any]:
    """Manage invoice batches.

    Args:
        client: ConnectWiseClient instance
        batch_ids: List of batch IDs to process
        summarize_expenses: Whether to summarize expenses (default: True)

    Returns:
        dict[str, Any]: Response from the batch management operation
    """
    # Create batch identifier using current date/time
    batch_id = datetime.now().strftime("%Y%m%d-%H%M%S")

    # Prepare the request payload
    data = {
        "batchIdentifier": batch_id,
        "exportInvoicesFlag": True,
        "exportExpensesFlag": True,
        "exportProductsFlag": False,
        "summarizeExpenses": summarize_expenses,
        "processedRecordIds": batch_ids,
    }

    # Make the API request
    response = client.post("/finance/accounting/batches", data=data)
    return response.json()


# --- Agreement & Contract Functions ---


def get_agreement(client: ConnectWiseClient, agreement_id: int) -> dict[str, Any]:
    """Retrieve details for a specific agreement.

    Args:
        client: ConnectWiseClient instance
        agreement_id: ID of the agreement to retrieve

    Returns:
        dict[str, Any]: Agreement details
    """
    response = client.get(f"/finance/agreements/{agreement_id}")
    return response.json()


def get_agreement_work_roles(
    client: ConnectWiseClient, agreement_id: int, work_role_id: int
) -> list[dict[str, Any]]:
    """Retrieve work roles for a specific agreement.

    Args:
        client: ConnectWiseClient instance
        agreement_id: ID of the agreement
        work_role_id: ID of the work role

    Returns:
        list[dict[str, Any]]: List of agreement work role dictionaries
    """
    params = {"conditions": f"workrole/id={work_role_id}"}
    response = client.get(f"/finance/agreements/{agreement_id}/workroles", params=params)
    return response.json()


def get_agreement_product_additions(
    client: ConnectWiseClient, agreement_id: int, product_identifier: str, invoice_description: str
) -> list[dict[str, Any]]:
    """Retrieve product additions for a specific agreement.

    Args:
        client: ConnectWiseClient instance
        agreement_id: ID of the agreement
        product_identifier: Product identifier
        invoice_description: Invoice description

    Returns:
        list[dict[str, Any]]: List of product addition dictionaries
    """
    params = {
        "conditions": (
            f'product/identifier = "{product_identifier}" and '
            f'invoiceDescription= "{invoice_description}"'
        )
    }
    response = client.get(f"/finance/agreements/{agreement_id}/additions", params=params)
    return response.json()


# --- Ticket & Project Functions ---


def get_project_ticket(client: ConnectWiseClient, ticket_id: int) -> dict[str, Any]:
    """Retrieve details for a specific project ticket.

    Args:
        client: ConnectWiseClient instance
        ticket_id: ID of the project ticket to retrieve

    Returns:
        dict[str, Any]: Project ticket details
    """
    response = client.get(f"/project/tickets/{ticket_id}")
    return response.json()


def get_service_ticket(client: ConnectWiseClient, ticket_id: int) -> dict[str, Any]:
    """Retrieve details for a specific service ticket.

    Args:
        client: ConnectWiseClient instance
        ticket_id: ID of the service ticket to retrieve

    Returns:
        dict[str, Any]: Service ticket details
    """
    response = client.get(f"/service/tickets/{ticket_id}")
    return response.json()


# --- Product & Expense Functions ---


def get_product(client: ConnectWiseClient, product_id: int) -> dict[str, Any]:
    """Retrieve details for a specific product.

    Args:
        client: ConnectWiseClient instance
        product_id: ID of the product to retrieve

    Returns:
        dict[str, Any]: Product details
    """
    response = client.get(f"/procurement/products/{product_id}")
    return response.json()


def get_expense_entries(
    client: ConnectWiseClient, invoice_id: int, max_pages: int | None = 50
) -> list[dict[str, Any]]:
    """List expense entries related to an invoice.

    Args:
        client: ConnectWiseClient instance
        invoice_id: ID of the invoice to retrieve expenses for
        max_pages: Maximum number of pages to retrieve (default: 50)

    Returns:
        list[dict[str, Any]]: List of expense entry dictionaries
    """
    return client.get_entity_data(
        "/expense/entries",
        {"conditions": f"invoice/id={invoice_id}"},
        entity_name="expense entries",
        max_pages=max_pages,
    )


# --- Report Functions ---


def get_reports(
    client: ConnectWiseClient,
    report_id: str | None = None,
    max_pages: int | None = 50,
) -> list[dict[str, Any]] | dict[str, Any]:
    """List reports from ConnectWise.

    Args:
        client: ConnectWiseClient instance
        report_id: Optional specific report ID to retrieve
        max_pages: Maximum number of pages to retrieve (default: 50)

    Returns:
        list[dict[str, Any]] | dict[str, Any]: List of reports or specific report data
    """
    if report_id:
        return client.get(f"/system/reports/{report_id}").json()

    return client.get_entity_data(
        "/system/reports", entity_name="reports", max_pages=max_pages
    )


# --- Higher-level Functions ---


def get_employee_utilization(
    client: ConnectWiseClient,
    start_date: str | None = None,
    end_date: str | None = None,
    max_pages: int | None = 50
) -> dict[str, dict[str, Any]]:
    """Calculate employee utilization based on time entries.

    This method aggregates time entries to calculate utilization metrics for employees.

    Args:
        client: ConnectWiseClient instance
        start_date: Optional start date (YYYY-MM-DD) for the time range
        end_date: Optional end date (YYYY-MM-DD) for the time range
        max_pages: Maximum number of pages to retrieve (default: 50)

    Returns:
        dict[str, dict[str, Any]]: Dictionary with employee utilization metrics
    """
    # Build filters for time entries
    filters = {}
    if start_date:
        date_condition = f"dateEntered >= [{start_date}T00:00:00Z]"
        filters["conditions"] = (
            f"{filters.get('conditions', '')} AND {date_condition}"
            if "conditions" in filters
            else date_condition
        )

    if end_date:
        date_condition = f"dateEntered <= [{end_date}T23:59:59Z]"
        filters["conditions"] = (
            f"{filters.get('conditions', '')} AND {date_condition}"
            if "conditions" in filters
            else date_condition
        )

    # Get time entries
    time_entries = get_time_entries(client, max_pages=max_pages, **filters)

    # Process and aggregate the data
    employee_data = {}
    for entry in time_entries:
        emp_id = entry.get("member", {}).get("identifier", "unknown")

        if emp_id not in employee_data:
            employee_data[emp_id] = {
                "total_hours": 0,
                "billable_hours": 0,
                "agreement_hours": 0,
                "tickets": set(),
                "agreements": set(),
                "work_types": {},
                "work_roles": {},
            }

        # Extract hours - different ConnectWise endpoints may have different field names
        hours = entry.get("hours", entry.get("actualHours", 0))

        # Update employee metrics
        employee_data[emp_id]["total_hours"] += hours

        # Track billable hours (may need to adjust logic based on your specific business rules)
        if entry.get("billableOption", "") == "Billable":
            employee_data[emp_id]["billable_hours"] += hours

        # Track agreement hours
        if "agreement" in entry and entry["agreement"] is not None and entry["agreement"].get("id"):
            employee_data[emp_id]["agreement_hours"] += hours
            employee_data[emp_id]["agreements"].add(entry["agreement"].get("id", 0))

        # Track tickets
        if "ticket" in entry and entry["ticket"] is not None and entry["ticket"].get("id"):
            employee_data[emp_id]["tickets"].add(entry["ticket"].get("id", 0))

        # Track work types
        work_type = entry.get("workType", {}).get("name", "unknown")
        if work_type in employee_data[emp_id]["work_types"]:
            employee_data[emp_id]["work_types"][work_type] += hours
        else:
            employee_data[emp_id]["work_types"][work_type] = hours

        # Track work roles
        work_role = entry.get("workRole", {}).get("name", "unknown")
        if work_role in employee_data[emp_id]["work_roles"]:
            employee_data[emp_id]["work_roles"][work_role] += hours
        else:
            employee_data[emp_id]["work_roles"][work_role] = hours

    # Calculate utilization metrics
    for emp_id in employee_data:
        data = employee_data[emp_id]

        # Calculate utilization percentage if we have total hours
        if data["total_hours"] > 0:
            data["billable_utilization"] = (data["billable_hours"] / data["total_hours"]) * 100
            data["agreement_utilization"] = (data["agreement_hours"] / data["total_hours"]) * 100
        else:
            data["billable_utilization"] = 0
            data["agreement_utilization"] = 0

        # Convert sets to counts for serialization
        data["ticket_count"] = len(data["tickets"])
        data["agreement_count"] = len(data["agreements"])

        # Remove the sets that aren't JSON serializable
        del data["tickets"]
        del data["agreements"]

    return employee_data
