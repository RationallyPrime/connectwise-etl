from __future__ import annotations

"""fabric_api.client

Enhanced ConnectWise REST client optimized for Microsoft Fabric environment.

Key features:
* **Simplified authentication** - Uses environment variables directly from Fabric
* **Robust retry/back-off** - Shared HTTP session transparently retries 5× on 429/50x
* **Clean logging** - Detailed logging of API interactions
"""

import base64
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from requests.models import Response
from urllib3.util.retry import Retry

from .core.utils import create_batch_identifier

__all__ = [
    "ApiErrorRecord",
    "ConnectWiseClient",
]

# Logger setup
logger = logging.getLogger(name=__name__)
logger.addHandler(logging.NullHandler())


class ApiErrorRecord(dict):
    """Dictionary sub-class that captures structured API-level errors.

    The shape purposefully mirrors *ManageInvoiceError* for downstream processing.
    """
    invoice_number: str  # type: ignore[assignment]
    error_table_id: int  # type: ignore[assignment]
    error_type: Optional[str]
    error_message: Optional[str]
    table_name: Optional[str]


class ConnectWiseClient:
    """Thin, resilient wrapper around the ConnectWise Manage REST API.
    
    Optimized for Microsoft Fabric environment with simplified authentication.
    """

    BASE_URL: str = os.getenv(
        "CW_BASE_URL",
        "https://verk.thekking.is/v4_6_release/apis/3.0",  # sensible default for Wise
    )

    def __init__(self):
        """Initialize client using environment variables from Fabric."""
        # Get credentials from environment variables
        self.basic_username = os.getenv("CW_AUTH_USERNAME")
        self.basic_password = os.getenv("CW_AUTH_PASSWORD")
        self.client_id = os.getenv("CW_CLIENTID")

        # Validate required credentials
        if not self.basic_username or not self.basic_password:
            raise ValueError("Basic auth credentials not configured in environment variables")
            
        if not self.client_id:
            raise ValueError("Client ID not configured in environment variables")

        # Set up a session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def _headers(self) -> Dict[str, str]:
        """Return the headers needed for API requests."""
        if not self.client_id:
            raise ValueError("Client ID is required")
            
        return {
            "clientId": self.client_id,
            "Accept": "application/vnd.connectwise.com+json; version=2025.1",
            "Content-Type": "application/json",
        }

    @staticmethod
    def create_batch_identifier(ts: Optional[datetime] = None) -> str:
        """Return UTC timestamp formatted as YYYYMMDD-HHMMSS."""
        return create_batch_identifier(timestamp=ts)

    def get(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        fields: Optional[str] = None,
        conditions: Optional[str] = None,
        child_conditions: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> Response:
        """Perform a GET request to the ConnectWise API.

        Args:
            endpoint: API endpoint to call (e.g. "/finance/agreements")
            params: Dictionary of query parameters
            fields: Comma-separated list of fields to return (e.g. "id,name,type")
            conditions: Query conditions using ConnectWise syntax
            child_conditions: Conditions for child objects/relationships
            order_by: Field(s) to sort results by (e.g. "id desc" or "name asc")

        Returns:
            Response object from the API call
        """
        # Combine params with fields and conditions
        all_params = params.copy() if params else {}

        # Add standard filtering parameters if provided
        if fields:
            all_params["fields"] = fields

        if conditions:
            all_params["conditions"] = conditions

        if child_conditions:
            all_params["childconditions"] = child_conditions

        if order_by:
            all_params["orderBy"] = order_by

        # Build request URL and headers
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._headers()

        # These values are validated in __init__, so they're guaranteed to be non-None
        assert self.basic_username is not None
        assert self.basic_password is not None
        auth = (self.basic_username, self.basic_password)

        logger.debug(f"GET {url} with params={all_params}")
        resp = self.session.get(url, headers=headers, params=all_params, auth=auth)
        resp.raise_for_status()
        return resp

    def post(
        self,
        endpoint: str,
        *,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Response:
        """Perform a POST request to the ConnectWise API."""
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._headers()
        
        # These values are validated in __init__, so they're guaranteed to be non-None
        assert self.basic_username is not None
        assert self.basic_password is not None
        auth = (self.basic_username, self.basic_password)

        logger.debug(f"POST {url}")
        resp = self.session.post(
            url, headers=headers, json=json_data, params=params, auth=auth
        )
        resp.raise_for_status()
        return resp

    def put(
        self,
        endpoint: str,
        *,
        json_data: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Response:
        """Perform a PUT request to the ConnectWise API."""
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._headers()
        
        # These values are validated in __init__, so they're guaranteed to be non-None
        assert self.basic_username is not None
        assert self.basic_password is not None
        auth = (self.basic_username, self.basic_password)

        logger.debug(f"PUT {url}")
        resp = self.session.put(url, headers=headers, json=json_data, params=params, auth=auth)
        resp.raise_for_status()
        return resp

    def delete(self, endpoint: str, *, params: Optional[Dict[str, Any]] = None) -> Response:
        """Perform a DELETE request to the ConnectWise API."""
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._headers()
        
        # These values are validated in __init__, so they're guaranteed to be non-None
        assert self.basic_username is not None
        assert self.basic_password is not None
        auth = (self.basic_username, self.basic_password)

        logger.debug(f"DELETE {url}")
        resp = self.session.delete(url, headers=headers, params=params, auth=auth)
        resp.raise_for_status()
        return resp

    def paginate(
        self,
        endpoint: str,
        entity_name: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        fields: Optional[str] = None,
        conditions: Optional[str] = None,
        child_conditions: Optional[str] = None,
        order_by: Optional[str] = None,
        max_pages: Optional[int] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get all pages of data from a paginated endpoint.

        Args:
            endpoint: API endpoint to call (e.g. "/finance/agreements")
            entity_name: Name of the entity being fetched (for logging)
            params: Dictionary of query parameters
            fields: Comma-separated list of fields to return
            conditions: Query conditions using ConnectWise syntax
            child_conditions: Conditions for child objects/relationships
            order_by: Field(s) to sort results by
            max_pages: Maximum number of pages to fetch
            page_size: Number of records per page (default 100, max typically 1000)

        Returns:
            List of entity dictionaries from the API
        """
        # Combine params with fields and conditions
        all_params = params.copy() if params else {}

        # Set page size 
        if "pageSize" not in all_params:
            all_params["pageSize"] = page_size
        else:
            page_size = all_params["pageSize"]

        # Add standard filtering parameters if provided
        if fields:
            all_params["fields"] = fields
            logger.debug(f"Using fields filter: {fields}")

        if conditions:
            all_params["conditions"] = conditions
            logger.debug(f"Using conditions filter: {conditions}")

        if child_conditions:
            all_params["childconditions"] = child_conditions
            logger.debug(f"Using child conditions filter: {child_conditions}")

        if order_by:
            all_params["orderBy"] = order_by
            logger.debug(f"Using order by: {order_by}")

        items: List[Dict[str, Any]] = []
        page = 1

        while True:
            # Check if we've reached max pages limit
            if max_pages is not None and page > max_pages:
                logger.info(f"Reached maximum page limit of {max_pages}. Stopping.")
                break

            # Create a copy of params for this page
            query = all_params.copy()
            query.update({"page": page, "pageSize": page_size})

            url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
            logger.debug(f"Requesting {entity_name} page {page}: {url}")

            try:
                headers = self._headers()
                
                # These values are validated in __init__, so they're guaranteed to be non-None
                assert self.basic_username is not None
                assert self.basic_password is not None
                auth = (self.basic_username, self.basic_password)
                
                response = self.session.get(url, headers=headers, params=query, auth=auth)
                response.raise_for_status()

                data = response.json()
                items.extend(data)
                logger.debug(f"Retrieved {len(data)} {entity_name} on page {page}")

                # Check if we've retrieved all data
                if len(data) < page_size:
                    break

                page += 1
            except Exception as e:
                logger.error(f"Error fetching {entity_name} page {page}: {e!s}")
                break

        logger.info(f"Total {entity_name} retrieved from {endpoint}: {len(items)}")
        return items