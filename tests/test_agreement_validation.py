"""Test script for validating agreement data against schemas.Agreement model.

This script demonstrates the use of the schema-based agreement fetching and validation
as part of the ConnectWise Delta load pipeline improvements.
"""

import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import ValidationError

# Load environment variables from .env file in the root directory
# This must be done before importing modules that use environment variables
root_dir = Path(__file__).parent.parent
env_path = root_dir / '.env'
load_dotenv()

# Import from parent directory
sys.path.append('..')
from fabric_api.client import ConnectWiseClient
from fabric_api.extract.agreements import fetch_agreements_raw
from fabric_api import schemas

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def test_agreement_validation():
    """
    Test the fetch_agreements_raw function and validate each agreement.
    
    This function:
    1. Creates a ConnectWiseClient
    2. Calls fetch_agreements_raw to get raw agreement data
    3. Attempts to validate each agreement using schemas.Agreement
    4. Logs success or validation errors
    """
    # Create a client using environment variables loaded from .env
    logger.info("Creating ConnectWiseClient using environment variables from .env")
    
    # Set up authentication details, similar to the approach in test_api.py
    # Default test credentials (from test_api.py example - these should be replaced with actual credentials)
    default_auth_username = "thekking+yemGyHDPdJ1hpuqx"  # Replace with actual test username
    default_auth_password = "yMqpe26Jcu55FbQk"          # Replace with actual test password
    default_client_id = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81"  # Replace with actual client ID
    
    # Override with environment variables if available
    auth_username = os.getenv("CW_AUTH_USERNAME", default_auth_username)
    auth_password = os.getenv("CW_AUTH_PASSWORD", default_auth_password)
    client_id = os.getenv("CW_CLIENTID", default_client_id)
    
    logger.info(f"Using auth username: {auth_username}")
    
    # Create the client with explicit authentication parameters
    client = ConnectWiseClient(
        basic_username=auth_username,
        basic_password=auth_password,
        client_id=client_id
    )
    
    # Optional: Use a small page_size for testing
    page_size = 10
    max_pages = 1
    
    # Optional: Add conditions for testing specific agreements
    # conditions = "id=12345"  # Example: only fetch a specific agreement
    
    # Call the new function to fetch raw agreements
    logger.info(f"Fetching agreements (page_size={page_size}, max_pages={max_pages})")
    raw_agreements = fetch_agreements_raw(
        client=client,
        page_size=page_size,
        max_pages=max_pages,
        # conditions=conditions  # Uncomment to use conditions
    )
    
    logger.info(f"Retrieved {len(raw_agreements)} agreements")
    
    # Validate each agreement
    valid_count = 0
    invalid_count = 0
    
    for i, raw_agreement in enumerate(raw_agreements):
        agreement_id = raw_agreement.get("id", f"Unknown-{i}")
        logger.info(f"Validating agreement {agreement_id} ({i+1}/{len(raw_agreements)})")
        
        try:
            # Attempt to validate using the Pydantic model
            validated_agreement = schemas.Agreement.model_validate(raw_agreement)
            valid_count += 1
            logger.info(f"✅ SUCCESS: Agreement {agreement_id} validated successfully")
            
        except ValidationError as e:
            invalid_count += 1
            logger.error(f"❌ ERROR: Agreement {agreement_id} validation failed")
            
            # Print detailed validation errors
            for error in e.errors():
                location = ".".join(str(loc) for loc in error["loc"])
                logger.error(f"  - Field: {location}")
                logger.error(f"    Error: {error['msg']}")
                logger.error(f"    Type: {error['type']}")
    
    # Print summary
    logger.info(f"Validation complete: {valid_count} valid, {invalid_count} invalid agreements")
    
    # Return a non-zero exit code if any agreements failed validation
    if invalid_count > 0:
        return 1
    return 0

if __name__ == "__main__":
    # Ensure we're running the script with .env file access
    if not env_path.exists():
        logger.error(f".env file not found at {env_path}")
        logger.error("Please create a .env file with ConnectWise credentials in the root directory")
        sys.exit(1)
    
    sys.exit(test_agreement_validation())
