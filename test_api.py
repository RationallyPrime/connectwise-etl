#!/usr/bin/env python
"""
ConnectWise API Test Script

This script tests the ConnectWise API integration without writing data to Delta tables.
It provides detailed logging of requests and responses to help debug API issues.
"""

from logging import Logger


import os
import json
import logging
from datetime import datetime, timedelta
from typing import Any

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger: Logger = logging.getLogger("cw_test")

# Set credentials (replace with your actual credentials)
os.environ["CW_AUTH_USERNAME"] = "thekking+yemGyHDPdJ1hpuqx"
os.environ["CW_AUTH_PASSWORD"] = "yMqpe26Jcu55FbQk"
os.environ["CW_CLIENTID"] = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"
os.environ["CW_WRITE_MODE"] = "overwrite"

# Import the necessary modules
from fabric_api.client import ConnectWiseClient
from fabric_api.extract._common import paginate
from fabric_api.extract.invoices import get_unposted_invoices_with_details
from fabric_api.extract.time import get_time_entries_for_invoice
from fabric_api.extract.expenses import get_expense_entries_with_relations
from fabric_api.extract.products import get_products_for_invoice
from fabric_api.extract.agreements import get_agreement_with_relations


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
        basic_username=auth_username,
        basic_password=auth_password,
        client_id=client_id
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
        logger.error(f"API connection failed: {str(e)}")
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
        "/finance/agreements"
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
            logger.error(f"Error testing endpoint {endpoint}: {str(e)}")


def test_invoice_extraction() -> None:
    """Test invoice extraction with date filtering."""
    logger.info("Testing invoice extraction...")
    client = setup_client()
    
    # Set date parameters - try different date ranges
    date_ranges = [
        # Last 7 days
        ((datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")),
        # Last 30 days
        ((datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")),
        # Last 90 days
        ((datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")),
        # Last 180 days
        ((datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d")),
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
                    logger.info(f"Testing related entities for invoice {invoice_number} (ID: {invoice_id})")
                    
                    # Test time entries
                    try:
                        logger.info("Testing time entries endpoint...")
                        time_entries_response = client.get(f"/finance/invoices/{invoice_id}/timeentries")
                        time_entries_data = time_entries_response.json()
                        logger.info(f"Found {len(time_entries_data)} time entries via direct API call")
                        if time_entries_data:
                            logger.info("Sample time entry schema:")
                            pretty_print_json(time_entries_data[0])
                    except Exception as e:
                        logger.error(f"Error testing time entries: {str(e)}")
                    
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
                        logger.error(f"Error testing expenses: {str(e)}")
                    
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
                        logger.error(f"Error testing products: {str(e)}")
            
            # Now test the full extraction function
            logger.info("Testing full invoice extraction function...")
            invoice_headers, invoice_lines, time_entries, expenses, products, errors = get_unposted_invoices_with_details(
                client=client,
                max_pages=1,  # Limit to 1 page for testing
                **cw_filter
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
            logger.error(msg=f"Error during invoice extraction for date range {start_date} to {end_date}: {str(e)}")


def test_all_extraction_methods(max_pages: int = 50) -> None:
    """Test all extraction methods with the specified max_pages limit."""
    logger.info(f"Testing all extraction methods with max_pages={max_pages}")
    client: ConnectWiseClient = setup_client()
    
    # Test invoice extraction (both posted and unposted)
    from fabric_api.extract.invoices import get_invoices_with_details
    
    logger.info("Running invoice extraction (both posted and unposted)...")
    invoice_headers, invoice_lines, time_entries, expenses, products, errors = get_invoices_with_details(
        client=client,
        max_pages=max_pages
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
            logger.warning(f"Encountered {len(agreement_errors)} errors during agreement extraction")


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
        logger.error(f"Tests failed: {str(e)}") 


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="ConnectWise API Test Tool")
    parser.add_argument("--test", choices=["connection", "endpoints", "invoices", "all"], 
                        default="all", help="Test to run")
    parser.add_argument("--max-pages", type=int, default=50, 
                        help="Maximum number of pages to retrieve")
    parser.add_argument("--debug", action="store_true", 
                        help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    logger.info(f"Starting ConnectWise API test: {args.test}")
    
    try:
        if args.test == "connection" or args.test == "all":
            test_api_connection()
        
        if args.test == "endpoints" or args.test == "all":
            test_endpoints()
        
        if args.test == "invoices" or args.test == "all":
            test_invoice_extraction()
        
        if args.test == "all":
            test_all_extraction_methods(max_pages=args.max_pages)
        
        logger.info("Tests completed successfully")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        exit(1)
