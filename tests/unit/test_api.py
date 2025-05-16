#!/usr/bin/env python
"""
ConnectWise API Test Script

This script tests the ConnectWise API integration without writing data to Delta tables.
It provides detailed logging of requests and responses to help debug API issues.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from logging import Logger
from typing import Any

# Import the necessary modules
from fabric_api.client import ConnectWiseClient
from fabric_api.extract._common import paginate
from fabric_api.extract.invoices import get_unposted_invoices_with_details

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger: Logger = logging.getLogger("cw_test")

# Set credentials (replace with your actual credentials)
os.environ["CW_AUTH_USERNAME"] = "thekking+yemGyHDPdJ1hpuqx"
os.environ["CW_AUTH_PASSWORD"] = "yMqpe26Jcu55FbQk"
os.environ["CW_CLIENTID"] = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"
os.environ["CW_WRITE_MODE"] = "overwrite"


def pretty_print_json(data: Any) -> None:
    """Print data as formatted JSON."""
    print(json.dumps(data, indent=2, default=str))


def setup_client():
    """Set up and return a ConnectWiseClient instance."""
    # Authentication details from the notebook
    auth_username = "thekking+yemGyHDPdJ1hpuqx"
    auth_password = "yMqpe26Jcu55FbQk"
    client_id = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"

    # Override with environment variables if available
    auth_username = os.getenv("CW_AUTH_USERNAME", auth_username)
    auth_password = os.getenv("CW_AUTH_PASSWORD", auth_password)
    client_id = os.getenv("CW_CLIENTID", client_id)

    logger.info(f"Setting up client with auth username: {auth_username}")

    return ConnectWiseClient(
        basic_username=auth_username, basic_password=auth_password, client_id=client_id
    )


def test_api_connection() -> None:
    """Test basic API connectivity."""
    logger.info("Testing basic API connectivity...")
    client: ConnectWiseClient = setup_client()

    try:
        # Try a simple API call to check connectivity
        response = client.get("/system/info")
        logger.info(f"API connection successful: {response.status_code}")
        logger.info("Response data:")
        pretty_print_json(response.json())
    except Exception as e:
        logger.error(f"API connection failed: {e!s}")
        raise


def test_endpoints() -> None:
    """Test various API endpoints to understand their schema."""
    logger.info("Testing specific API endpoints...")
    client = setup_client()

    # Test endpoints
    endpoints = [
        "/finance/accounting/unpostedinvoices",
        "/time/entries",
        "/expense/entries",
        "/procurement/products",
        "/finance/agreements",
    ]

    for endpoint in endpoints:
        try:
            logger.info(f"Testing endpoint: {endpoint}")
            response = client.get(endpoint, params={"pageSize": 1})
            data = response.json()

            if isinstance(data, list) and len(data) > 0:
                logger.info(f"Found {len(data)} items. First item schema:")
                # Print the keys of the first item to understand the schema
                logger.info(f"Keys: {list(data[0].keys())}")
                # Print a sample item
                logger.info("Sample item:")
                pretty_print_json(data[0])
            else:
                logger.info(f"No data found or unexpected response format: {data}")
        except Exception as e:
            logger.error(f"Error testing endpoint {endpoint}: {e!s}")


def test_invoice_extraction() -> None:
    """Test invoice extraction with date filtering."""
    logger.info("Testing invoice extraction...")
    client = setup_client()

    # Set date parameters - try different date ranges
    date_ranges = [
        # Last 7 days
        (
            (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        ),
        # Last 30 days
        (
            (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        ),
        # Last 90 days
        (
            (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        ),
        # Last 180 days
        (
            (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d"),
        ),
    ]

    for start_date, end_date in date_ranges:
        logger.info(f"Testing date range: {start_date} to {end_date}")

        # Create filter for invoice extraction
        cw_filter = {
            "conditions": f"invoiceDate >= [{start_date}] and invoiceDate <= [{end_date}]",
            "pageSize": 10,  # Limit to 10 for testing
        }

        try:
            # Test raw API call first
            logger.info("Testing raw API call to unposted invoices...")
            invoices_raw = paginate(
                client,
                endpoint="/finance/accounting/unpostedinvoices",
                entity_name="unposted invoices",
                params=cw_filter,
                max_pages=1,  # Limit to 1 page for testing
            )

            logger.info(f"Found {len(invoices_raw)} invoices in raw API call")
            if invoices_raw:
                logger.info("Sample invoice schema:")
                pretty_print_json(invoices_raw[0])

                # Test getting details for the first invoice
                invoice_id = invoices_raw[0].get("id")
                invoice_number = invoices_raw[0].get("invoiceNumber", "")

                if invoice_id:
                    logger.info(
                        f"Testing related entities for invoice {invoice_number} (ID: {invoice_id})"
                    )

                    # Test time entries
                    try:
                        logger.info("Testing time entries endpoint...")
                        time_entries_response = client.get(
                            f"/finance/invoices/{invoice_id}/timeentries"
                        )
                        time_entries_data = time_entries_response.json()
                        logger.info(
                            f"Found {len(time_entries_data)} time entries via direct API call"
                        )
                        if time_entries_data:
                            logger.info("Sample time entry schema:")
                            pretty_print_json(time_entries_data[0])
                    except Exception as e:
                        logger.error(f"Error testing time entries: {e!s}")

                    # Test expenses
                    try:
                        logger.info("Testing expenses endpoint...")
                        expenses_response = client.get(f"/finance/invoices/{invoice_id}/expenses")
                        expenses_data = expenses_response.json()
                        logger.info(f"Found {len(expenses_data)} expenses via direct API call")
                        if expenses_data:
                            logger.info("Sample expense schema:")
                            pretty_print_json(expenses_data[0])
                    except Exception as e:
                        logger.error(f"Error testing expenses: {e!s}")

                    # Test products
                    try:
                        logger.info("Testing products endpoint...")
                        products_response = client.get(f"/finance/invoices/{invoice_id}/products")
                        products_data = products_response.json()
                        logger.info(f"Found {len(products_data)} products via direct API call")
                        if products_data:
                            logger.info("Sample product schema:")
                            pretty_print_json(products_data[0])
                    except Exception as e:
                        logger.error(f"Error testing products: {e!s}")

            # Now test the full extraction function
            logger.info("Testing full invoice extraction function...")
            invoice_headers, invoice_lines, time_entries, expenses, products, errors = (
                get_unposted_invoices_with_details(
                    client=client,
                    max_pages=1,  # Limit to 1 page for testing
                    **cw_filter,
                )
            )

            logger.info(msg="Extraction results:")
            logger.info(msg=f"- Invoice headers: {len(invoice_headers)}")
            logger.info(msg=f"- Invoice lines: {len(invoice_lines)}")
            logger.info(msg=f"- Time entries: {len(time_entries)}")
            logger.info(msg=f"- Expenses: {len(expenses)}")
            logger.info(msg=f"- Products: {len(products)}")
            logger.info(msg=f"- Errors: {len(errors)}")

            if errors:
                logger.warning(msg="Errors encountered during extraction:")
                for error in errors:
                    logger.warning(msg=f"- {error.error_type}: {error.error_message}")

            # Break after finding data
            if invoice_headers:
                logger.info(msg="Found data, stopping date range testing")
                break

        except Exception as e:
            logger.error(
                msg=f"Error during invoice extraction for date range {start_date} to {end_date}: {e!s}"
            )


def test_batch_invoice_extraction(max_pages: int = 5) -> None:
    """Test the batch invoice extraction with less verbosity."""
    print("\n--- TESTING BATCH INVOICE EXTRACTION ---")
    start_time = datetime.now()
    client = setup_client()

    try:
        from fabric_api.extract.invoices import get_invoices_with_details

        # Run the batch extraction
        print(f"Fetching invoices with max_pages={max_pages}...")
        invoice_headers, invoice_lines, time_entries, expenses, products, errors = (
            get_invoices_with_details(client=client, max_pages=max_pages)
        )

        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds()

        print("\n" + "=" * 60)
        print(f"EXTRACTION RESULTS | Completed in {execution_time:.2f} seconds")
        print("=" * 60)
        print(f"{'Data Type':<20} | {'Count':<8} | {'Sample ID'}")
        print("-" * 60)

        sample_header_id = (
            getattr(invoice_headers[0], "invoice_number", "N/A") if invoice_headers else "N/A"
        )
        print(f"{'Invoice Headers':<20} | {len(invoice_headers):<8} | {sample_header_id}")

        sample_line_id = (
            f"{getattr(invoice_lines[0], 'invoice_number', 'N/A')}:{getattr(invoice_lines[0], 'line_no', 'N/A')}"
            if invoice_lines
            else "N/A"
        )
        print(f"{'Invoice Lines':<20} | {len(invoice_lines):<8} | {sample_line_id}")

        sample_time_id = getattr(time_entries[0], "time_entry_id", "N/A") if time_entries else "N/A"
        print(f"{'Time Entries':<20} | {len(time_entries):<8} | {sample_time_id}")

        sample_expense_id = getattr(expenses[0], "expense_id", "N/A") if expenses else "N/A"
        print(f"{'Expenses':<20} | {len(expenses):<8} | {sample_expense_id}")

        sample_product_id = getattr(products[0], "product_id", "N/A") if products else "N/A"
        print(f"{'Products':<20} | {len(products):<8} | {sample_product_id}")

        # Make errors stand out
        error_style = "*** " if errors else ""
        print(f"{error_style}{'Errors':<20} | {len(errors):<8} | {'-'}")

        # ERROR ANALYSIS - only if errors exist
        if errors:
            print("\n" + "=" * 60)
            print(f"ERROR ANALYSIS | {len(errors)} errors found")
            print("=" * 60)

            # Group errors by table
            table_errors = {}
            for error in errors:
                table = getattr(error, "table_name", "Unknown")
                table_errors[table] = table_errors.get(table, 0) + 1

            print("ERRORS BY TABLE:")
            for table, count in sorted(table_errors.items(), key=lambda x: x[1], reverse=True):
                print(f"  {table:<25}: {count}")

            # Group errors by message pattern
            error_types = {}
            for error in errors:
                # Use just the first part of the error message to group similar errors
                msg = str(error.error_message)
                msg_start = (
                    msg.split(" (")[0]
                    if " (" in msg
                    else (msg[:50] + "..." if len(msg) > 50 else msg)
                )
                error_types[msg_start] = error_types.get(msg_start, 0) + 1

            print("\nTOP ERROR TYPES:")
            for i, (msg, count) in enumerate(
                sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:3]
            ):
                print(f"  {i + 1}. {count} errors: {msg}")

            # Show sample of most common error
            most_common_msg = max(error_types.items(), key=lambda x: x[1])[0]
            sample_error = next(
                (e for e in errors if most_common_msg in str(e.error_message)), errors[0]
            )

            print("\nSAMPLE ERROR DETAILS:")
            print(f"  Table:         {getattr(sample_error, 'table_name', 'Unknown')}")
            print(f"  Invoice:       {getattr(sample_error, 'invoice_number', 'Unknown')}")
            print(f"  Error Type:    {getattr(sample_error, 'error_type', 'Unknown')}")
            print(f"  Message:       {getattr(sample_error, 'error_message', 'Unknown')}")

        # Sample data validation check
        if invoice_lines:
            print("\n" + "=" * 60)
            print("INVOICE LINE VALIDATION CHECK")
            print("=" * 60)
            sample_line = invoice_lines[0]
            sample_dict = sample_line.model_dump()
            for field in [
                "invoice_number",
                "line_no",
                "description",
                "time_entry_id",
                "quantity",
                "line_amount",
            ]:
                print(f"{field + ':':<20} {sample_dict.get(field, 'None')}")

        print("\nBATCH EXTRACTION TEST COMPLETE")

    except Exception as e:
        print(f"\nERROR: {e!s}")


def test_all_extraction_methods(max_pages: int = 50) -> None:
    """Test all extraction methods with the specified max_pages limit."""
    logger.info(f"Testing all extraction methods with max_pages={max_pages}")
    client: ConnectWiseClient = setup_client()

    # Test invoice extraction (both posted and unposted)
    from fabric_api.extract.invoices import get_invoices_with_details

    logger.info("Running invoice extraction (both posted and unposted)...")
    invoice_headers, invoice_lines, time_entries, expenses, products, errors = (
        get_invoices_with_details(client=client, max_pages=max_pages)
    )

    logger.info(msg="Extraction results:")
    logger.info(msg=f"- Invoice headers: {len(invoice_headers)}")
    logger.info(msg=f"- Invoice lines: {len(invoice_lines)}")
    logger.info(msg=f"- Time entries: {len(time_entries)}")
    logger.info(msg=f"- Expenses: {len(expenses)}")
    logger.info(msg=f"- Products: {len(products)}")
    logger.info(msg=f"- Errors: {len(errors)}")

    if errors:
        logger.warning(msg="Errors encountered during extraction:")
        for error in errors[:5]:  # Show first 5 errors
            logger.warning(msg=f"- {error.error_type}: {error.error_message}")
        if len(errors) > 5:
            logger.warning(f"... and {len(errors) - 5} more errors")

    # If we found invoices, test agreement extraction for any agreements referenced
    agreement_ids = set()
    for model in (
        *invoice_headers,
        *invoice_lines,
        *time_entries,
        *expenses,
        *products,
    ):
        agr_id = getattr(model, "agreement_id", None)
        if agr_id:
            agreement_ids.add(int(agr_id))

    logger.info(f"Found {len(agreement_ids)} unique agreement IDs")

    if agreement_ids:
        from fabric_api.extract.agreements import get_agreement_with_relations

        logger.info("Testing agreement extraction...")
        agreements = []
        agreement_errors = []

        for agr_id in list(agreement_ids)[:10]:  # Test up to 10 agreements
            logger.info(f"Extracting agreement {agr_id}...")
            agreement, errors = get_agreement_with_relations(client, agr_id)
            if agreement:
                agreements.append(agreement)
            agreement_errors.extend(errors)

        logger.info(f"Successfully extracted {len(agreements)} agreements")
        if agreement_errors:
            logger.warning(
                f"Encountered {len(agreement_errors)} errors during agreement extraction"
            )


def save_sample_invoice_to_json():
    """Save a raw invoice response to a JSON file for inspection."""
    logger.info("Saving sample invoice to JSON file...")
    client = setup_client()

    try:
        # Get raw unposted invoices
        raw_invoices = client.paginate(
            endpoint="/finance/accounting/unpostedinvoices",
            entity_name="unposted invoices",
            params={"pageSize": 5},
            max_pages=1,
        )

        if not raw_invoices:
            logger.warning("No unposted invoices found")

            # Try posted invoices as backup
            raw_invoices = client.paginate(
                endpoint="/finance/invoices",
                entity_name="posted invoices",
                params={"pageSize": 5},
                max_pages=1,
            )

            if not raw_invoices:
                logger.error("No invoices found at all")
                return

        # Save the first invoice to a JSON file
        sample_invoice = raw_invoices[0]

        with open("sample_invoice.json", "w") as f:
            json.dump(sample_invoice, f, indent=2, default=str)

        logger.info(
            f"Saved sample invoice {sample_invoice.get('invoiceNumber', 'Unknown')} to sample_invoice.json"
        )

    except Exception as e:
        logger.error(f"Error saving sample invoice: {e!s}")


def main() -> None:
    """Run all tests."""
    logger.info("Starting ConnectWise API tests")

    try:
        # Test basic connectivity
        test_api_connection()

        # Test endpoints
        test_endpoints()

        # Test invoice extraction
        test_invoice_extraction()

        logger.info("All tests completed")
    except Exception as e:
        logger.error(f"Tests failed: {e!s}")


if __name__ == "__main__":
    try:
        test_batch_invoice_extraction(max_pages=5)
    except Exception as e:
        print(f"Test failed: {e!s}")
