#!/usr/bin/env python
"""
Test script to verify the updated ConnectWiseClient with notebook-style authentication.
"""

import os
import sys
import logging
import argparse
from typing import Any

from fabric_api.client import ConnectWiseClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("cw_notebook_test")

# Authentication details from the notebook
AUTH_USERNAME = "thekking+yemGyHDPdJ1hpuqx"
AUTH_PASSWORD = "yMqpe26Jcu55FbQk"
CLIENT_ID = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"


def test_api_connection():
    """Test basic API connectivity with notebook-style authentication."""
    logger.info("Testing API connection with notebook-style authentication")
    
    # Create client with notebook credentials
    client = ConnectWiseClient(
        basic_username=AUTH_USERNAME,
        basic_password=AUTH_PASSWORD,
        client_id=CLIENT_ID
    )
    
    # Test a simple endpoint
    try:
        logger.info("Testing system/info endpoint...")
        response = client.get("system/info")
        logger.info(f"Success! API responded with status code: {response.status_code}")
        logger.info(f"Response data: {response.json()}")
        return True
    except Exception as e:
        logger.error(f"API connection failed: {str(e)}")
        return False


def test_companies(max_pages=1):
    """Test retrieving companies with notebook-style authentication."""
    logger.info(f"Testing company retrieval (max_pages={max_pages})...")
    
    # Create client with notebook credentials
    client = ConnectWiseClient(
        basic_username=AUTH_USERNAME,
        basic_password=AUTH_PASSWORD,
        client_id=CLIENT_ID
    )
    
    # Test companies endpoint
    try:
        companies = client.paginate(
            "company/companies", 
            "companies",
            max_pages=max_pages
        )
        logger.info(f"Success! Retrieved {len(companies)} companies")
        
        # Show first company if available
        if companies:
            logger.info(f"First company: {companies[0]}")
        return True
    except Exception as e:
        logger.error(f"Company retrieval failed: {str(e)}")
        return False


def test_invoices(max_pages=1):
    """Test retrieving invoices with notebook-style authentication."""
    logger.info(f"Testing invoice retrieval (max_pages={max_pages})...")
    
    # Create client with notebook credentials
    client = ConnectWiseClient(
        basic_username=AUTH_USERNAME,
        basic_password=AUTH_PASSWORD,
        client_id=CLIENT_ID
    )
    
    # Test invoices endpoint
    try:
        # Try posted invoices
        logger.info("Testing posted invoices...")
        posted_invoices = client.paginate(
            "finance/invoices", 
            "posted invoices",
            max_pages=max_pages
        )
        logger.info(f"Retrieved {len(posted_invoices)} posted invoices")
        
        # Try unposted invoices
        logger.info("Testing unposted invoices...")
        unposted_invoices = client.paginate(
            "finance/accounting/unpostedinvoices", 
            "unposted invoices",
            max_pages=max_pages
        )
        logger.info(f"Retrieved {len(unposted_invoices)} unposted invoices")
        
        # Show first invoice of each type if available
        if posted_invoices:
            logger.info(f"First posted invoice: {posted_invoices[0]}")
        if unposted_invoices:
            logger.info(f"First unposted invoice: {unposted_invoices[0]}")
            
        return True
    except Exception as e:
        logger.error(f"Invoice retrieval failed: {str(e)}")
        return False


def main():
    """Main entry point for the test script."""
    parser = argparse.ArgumentParser(description="Test ConnectWise API with notebook-style authentication")
    parser.add_argument("--test", choices=["connection", "companies", "invoices", "all"], default="all",
                        help="Test to run (default: all)")
    parser.add_argument("--max-pages", type=int, default=1,
                        help="Maximum number of pages to retrieve (default: 1)")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Set log level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("fabric_api").setLevel(logging.DEBUG)
    
    # Run selected test
    success = True
    if args.test in ["connection", "all"]:
        success = test_api_connection() and success
    
    if args.test in ["companies", "all"]:
        success = test_companies(max_pages=args.max_pages) and success
    
    if args.test in ["invoices", "all"]:
        success = test_invoices(max_pages=args.max_pages) and success
    
    # Return appropriate exit code
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
