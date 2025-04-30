#!/usr/bin/env python
"""
Main module for PSA data extraction pipeline.

This module orchestrates the extraction of data from ConnectWise Manage API,
transforms it, and loads it into Microsoft Fabric.

Usage:
    python -m fabric_api.main
"""

import argparse
import logging
from datetime import datetime, timedelta

from .client import ConnectWiseClient
from .extract import (
    export_invoice,
    get_agreement,
    get_employee_utilization,
    get_expense_entries,
    get_invoices,
    get_time_entries,
    get_unposted_invoices,
)
from .transform import process_fabric_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def extract_time_entries(
    client: ConnectWiseClient, start_date: str | None = None, end_date: str | None = None
) -> dict[str, str | None]:
    """Extract time entries and save to Fabric.

    Args:
        client: ConnectWiseClient instance
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format

    Returns:
        dict[str, str | None]: Dictionary with paths to saved data files
    """
    logger.info("Extracting time entries")

    # Set default dates if not provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if not start_date:
        # Default to 30 days before end_date
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=30)
        start_date = start_dt.strftime("%Y-%m-%d")

    # Build filter for time entries
    filters = {
        "conditions": (
            f"dateEntered >= [{start_date}T00:00:00Z] AND "
            f"dateEntered <= [{end_date}T23:59:59Z]"
        )
    }

    # Extract the data
    time_entries = get_time_entries(client, max_pages=50, **filters)

    # Save to Fabric
    path, _ = process_fabric_data(time_entries, "time_entries")

    # Calculate and save utilization metrics
    utilization_data = []
    emp_utilization = get_employee_utilization(
        client, start_date=start_date, end_date=end_date, max_pages=50
    )

    # Convert dictionary to list for DataFrame creation
    for emp_id, metrics in emp_utilization.items():
        utilization_data.append(
            {"employee_id": emp_id, "start_date": start_date, "end_date": end_date, **metrics}
        )

    util_path, _ = process_fabric_data(utilization_data, "employee_utilization")

    return {"time_entries": path, "employee_utilization": util_path}


def extract_invoices(client: ConnectWiseClient) -> dict[str, str | None]:
    """Extract invoices and save to Fabric.

    Args:
        client: ConnectWiseClient instance

    Returns:
        dict[str, str | None]: Dictionary with paths to saved data files
    """
    logger.info("Extracting invoices")

    # Extract posted invoices
    invoices = get_invoices(client, max_pages=50)
    inv_path, _ = process_fabric_data(invoices, "invoices")

    # Extract unposted invoices
    unposted_invoices = get_unposted_invoices(client, max_pages=50)
    unposted_path, _ = process_fabric_data(unposted_invoices, "unposted_invoices")

    # Extract detailed invoice data for a sample of invoices
    detailed_invoice_data = []

    # Just process the first 10 invoices as examples
    for invoice in invoices[:10]:
        invoice_id = invoice.get("id")
        if invoice_id:
            # Get invoice details
            try:
                detailed_data = export_invoice(client, invoice_id)
                detailed_invoice_data.append(detailed_data)

                # Get expense entries for this invoice
                expenses = get_expense_entries(client, invoice_id)
                if expenses:
                    exp_path, _ = process_fabric_data(expenses, f"invoice_expenses_{invoice_id}")
                    logger.info(f"Saved expense data for invoice {invoice_id} to {exp_path}")

            except Exception as e:
                logger.error(f"Error processing invoice {invoice_id}: {e}")

    # Save detailed invoice data
    if detailed_invoice_data:
        detail_path, _ = process_fabric_data(detailed_invoice_data, "invoice_details")
    else:
        detail_path = None

    return {
        "invoices": inv_path,
        "unposted_invoices": unposted_path,
        "invoice_details": detail_path,
    }


def extract_agreements(
    client: ConnectWiseClient, agreement_ids: list[int] | None = None
) -> dict[str, str | None]:
    """Extract agreements and save to Fabric.

    Args:
        client: ConnectWiseClient instance
        agreement_ids: Optional list of agreement IDs to extract

    Returns:
        dict[str, str | None]: Dictionary with paths to saved data files
    """
    logger.info("Extracting agreements")

    if not agreement_ids:
        # If no specific agreement IDs are provided, extract from invoices
        invoices = get_invoices(client, max_pages=10)
        agreement_ids = []

        for invoice in invoices:
            if "agreement" in invoice and invoice["agreement"] and "id" in invoice["agreement"]:
                agreement_ids.append(invoice["agreement"]["id"])

        # Remove duplicates
        agreement_ids = list(set(agreement_ids))

    # Extract agreement details
    agreement_data = []
    for agreement_id in agreement_ids:
        try:
            agreement = get_agreement(client, agreement_id)
            agreement_data.append(agreement)
        except Exception as e:
            logger.error(f"Error processing agreement {agreement_id}: {e}")

    if agreement_data:
        path, _ = process_fabric_data(agreement_data, "agreements")
    else:
        path = None

    return {"agreements": path}


def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="Extract PSA data from ConnectWise Manage API")
    parser.add_argument("--start-date", help="Start date for time entries (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for time entries (YYYY-MM-DD)")
    parser.add_argument("--extract-time", action="store_true", help="Extract time entries")
    parser.add_argument("--extract-invoices", action="store_true", help="Extract invoices")
    parser.add_argument("--extract-agreements", action="store_true", help="Extract agreements")
    parser.add_argument("--extract-all", action="store_true", help="Extract all data")
    args = parser.parse_args()

    logger.info("Starting PSA data extraction pipeline")

    # Create the client
    client = ConnectWiseClient()

    results = {}

    # Determine what to extract
    should_extract_time = args.extract_time or args.extract_all
    should_extract_invoices = args.extract_invoices or args.extract_all
    should_extract_agreements = args.extract_agreements or args.extract_all

    # Default to extracting everything if no specific extractions are specified
    if not any([should_extract_time, should_extract_invoices, should_extract_agreements]):
        should_extract_time = should_extract_invoices = should_extract_agreements = True

    # Extract the requested data
    if should_extract_time:
        time_results = extract_time_entries(client, args.start_date, args.end_date)
        results.update(time_results)

    if should_extract_invoices:
        invoice_results = extract_invoices(client)
        results.update(invoice_results)

    if should_extract_agreements:
        agreement_results = extract_agreements(client)
        results.update(agreement_results)

    # Log the results
    logger.info("Data extraction complete")
    for entity, path in results.items():
        if path:
            logger.info(f"{entity} data saved to {path}")

    return results


if __name__ == "__main__":
    main()
