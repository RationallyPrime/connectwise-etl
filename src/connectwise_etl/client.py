"""ConnectWise client and extractor - unified API access and data extraction.

Enhanced ConnectWise REST client optimized for Microsoft Fabric environment.
Combines low-level API access with high-level data extraction capabilities.

Key features:
* **Simplified authentication** - Uses environment variables directly from Fabric
* **Robust retry/back-off** - Shared HTTP session transparently retries 5x on 429/50x
* **Pydantic integration** - Automatic field selection and validation
* **Clean logging** - Detailed logging of API interactions
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

import requests
from pyspark.sql import DataFrame
from requests.adapters import HTTPAdapter
from requests.models import Response
from .utils.base import ErrorCode
from .utils.decorators import with_etl_error_handling
from .utils.exceptions import ETLConfigError, ETLProcessingError
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class ApiErrorRecord(dict):
    """Dictionary sub-class that captures structured API-level errors.

    The shape purposefully mirrors *ManageInvoiceError* for downstream processing.
    """

    invoice_number: str  # type: ignore[assignment]
    error_table_id: int  # type: ignore[assignment]
    error_type: str | None
    error_message: str | None
    table_name: str | None


class ConnectWiseClient:
    """Unified ConnectWise client providing both API access and data extraction.

    Combines low-level HTTP operations with high-level data extraction capabilities.
    Optimized for Microsoft Fabric environment with simplified authentication.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize client using environment variables from Fabric.

        Args:
            config: Optional configuration (currently unused, credentials from env)
        """
        self.config = config or {}

        # Get base URL from environment or use default
        self.base_url = os.getenv(
            "CW_BASE_URL",
            "https://verk.thekking.is/v4_6_release/apis/3.0",
        )

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
        retry_strategy: Retry = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
        self._spark = None

    @property
    def spark(self):
        """Get SparkSession - assumes running in Fabric with global spark."""
        if self._spark is None:
            # In Fabric notebooks, spark is available globally
            import sys

            self._spark = sys.modules["__main__"].spark
        return self._spark

    def _headers(self) -> dict[str, str]:
        """Return the headers needed for API requests."""
        if not self.client_id:
            raise ValueError("Client ID is required")

        return {
            "clientId": self.client_id,
            "Accept": "application/vnd.connectwise.com+json; version=2025.1",
            "Content-Type": "application/json",
        }

    @staticmethod
    def create_batch_identifier(ts: datetime | None = None) -> str:
        """Return UTC timestamp formatted as YYYYMMDD-HHMMSS."""
        if ts is None:
            ts = datetime.utcnow()
        return ts.strftime("%Y%m%d-%H%M%S")

    def get(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        fields: str | None = None,
        conditions: str | None = None,
        child_conditions: str | None = None,
        order_by: str | None = None,
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
        all_params: dict[str, Any] = params.copy() if params else {}

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
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers: dict[str, str] = self._headers()

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
        json_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Response:
        """Perform a POST request to the ConnectWise API."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers: dict[str, str] = self._headers()

        # These values are validated in __init__, so they're guaranteed to be non-None
        assert self.basic_username is not None
        assert self.basic_password is not None
        auth = (self.basic_username, self.basic_password)

        logger.debug(f"POST {url}")
        resp = self.session.post(url, headers=headers, json=json_data, params=params, auth=auth)
        resp.raise_for_status()
        return resp

    def put(
        self,
        endpoint: str,
        *,
        json_data: dict[str, Any],
        params: dict[str, Any] | None = None,
    ) -> Response:
        """Perform a PUT request to the ConnectWise API."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers: dict[str, str] = self._headers()

        # These values are validated in __init__, so they're guaranteed to be non-None
        assert self.basic_username is not None
        assert self.basic_password is not None
        auth: tuple[str, str] = (self.basic_username, self.basic_password)

        logger.debug(f"PUT {url}")
        resp = self.session.put(url, headers=headers, json=json_data, params=params, auth=auth)
        resp.raise_for_status()
        return resp

    def delete(self, endpoint: str, *, params: dict[str, Any] | None = None) -> Response:
        """Perform a DELETE request to the ConnectWise API."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers: dict[str, str] = self._headers()

        # These values are validated in __init__, so they're guaranteed to be non-None
        assert self.basic_username is not None
        assert self.basic_password is not None
        auth: tuple[str, str] = (self.basic_username, self.basic_password)

        logger.debug(f"DELETE {url}")
        resp = self.session.delete(url, headers=headers, params=params, auth=auth)
        resp.raise_for_status()
        return resp

    @with_etl_error_handling(operation="paginate_api")
    def paginate(
        self,
        endpoint: str,
        entity_name: str,
        *,
        params: dict[str, Any] | None = None,
        fields: str | None = None,
        conditions: str | None = None,
        child_conditions: str | None = None,
        order_by: str | None = None,
        max_pages: int | None = None,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
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
        all_params: dict[str, Any] = params.copy() if params else {}

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

        items: list[dict[str, Any]] = []
        page = 1

        while True:
            # Check if we've reached max pages limit
            if max_pages is not None and page > max_pages:
                logger.info(f"Reached maximum page limit of {max_pages}. Stopping.")
                break

            # Create a copy of params for this page
            query = all_params.copy()
            query.update({"page": page, "pageSize": page_size})

            url = f"{self.base_url}/{endpoint.lstrip('/')}"
            logger.debug(f"Requesting {entity_name} page {page}: {url}")

            try:
                headers: dict[str, str] = self._headers()

                # These values are validated in __init__, so they're guaranteed to be non-None
                assert self.basic_username is not None
                assert self.basic_password is not None
                auth: tuple[str, str] = (self.basic_username, self.basic_password)

                response = self.session.get(url, headers=headers, params=query, auth=auth)
                response.raise_for_status()

                data = response.json()
                items.extend(data)
                logger.debug(f"Retrieved {len(data)} {entity_name} on page {page}")

                # Check if we've retrieved all data
                if len(data) < page_size:
                    break

                page += 1
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    raise ETLProcessingError(
                        f"Rate limited while fetching {entity_name}",
                        code=ErrorCode.API_RATE_LIMITED,
                        details={"entity_name": entity_name, "page": page, "endpoint": endpoint}
                    ) from e
                elif e.response.status_code >= 500:
                    raise ETLProcessingError(
                        f"Server error while fetching {entity_name}",
                        code=ErrorCode.API_RESPONSE_INVALID,
                        details={"entity_name": entity_name, "page": page, "status_code": e.response.status_code}
                    ) from e
                else:
                    logger.error(f"HTTP error fetching {entity_name} page {page}: {e}")
                    break
            except Exception as e:
                logger.error(f"Error fetching {entity_name} page {page}: {e!s}")
                break

        logger.info(f"Total {entity_name} retrieved from {endpoint}: {len(items)}")
        return items

    def get_model_class(self, entity_name: str) -> type:
        """Get Pydantic model class for entity.

        Args:
            entity_name: Name of the entity

        Returns:
            Pydantic model class
        """
        # Dynamic import to avoid importing all models upfront
        import importlib

        models_module = importlib.import_module("connectwise_etl.models")

        # Map entity names to model class names
        model_name_mapping = {
            "Agreement": "Agreement",
            "TimeEntry": "TimeEntry",
            "ExpenseEntry": "ExpenseEntry",
            "ProductItem": "ProductItem",
            "PostedInvoice": "PostedInvoice",
            "UnpostedInvoice": "Invoice",  # Note: UnpostedInvoice uses Invoice model
        }

        model_name = model_name_mapping.get(entity_name)
        if not model_name:
            raise ETLConfigError(
                f"Unknown entity: {entity_name}. Available entities: {list(model_name_mapping.keys())}",
                code=ErrorCode.CONFIG_INVALID,
                details={"entity_name": entity_name, "available_entities": list(model_name_mapping.keys())}
            )

        # Get the model class from the module
        model_class = getattr(models_module, model_name, None)
        if not model_class:
            raise ETLConfigError(
                f"Model class {model_name} not found in models module",
                code=ErrorCode.CONFIG_MISSING,
                details={"model_name": model_name}
            )

        return model_class

    @with_etl_error_handling(operation="extract_data")
    def extract(
        self,
        endpoint: str,
        page_size: int = 1000,
        conditions: str | None = None,
        fields: list[str] | None = None,
        **kwargs,
    ) -> DataFrame:
        """Extract data from ConnectWise API and return as Spark DataFrame.

        Args:
            endpoint: API endpoint (e.g., "/finance/agreements")
            page_size: Number of records per page
            conditions: API query conditions
            fields: Fields to request (if None, derives from model)
            **kwargs: Additional parameters

        Returns:
            Spark DataFrame with extracted data
        """
        # Determine entity name from endpoint
        entity_name = self._get_entity_name(endpoint)

        # Get model class
        model_class = self.get_model_class(entity_name)

        # Generate fields string if not provided
        if fields is None:
            from .api_utils import get_fields_for_api_call

            fields_str = get_fields_for_api_call(model_class, max_depth=2)
        else:
            fields_str = ",".join(fields)

        logger.info(f"Extracting {entity_name} from {endpoint}")

        # Use the client's paginate method
        all_data = self.paginate(
            endpoint=endpoint,
            entity_name=f"{entity_name}s",  # Pluralize for API logging
            fields=fields_str,
            conditions=conditions,
            page_size=page_size,
            **kwargs,  # Pass through any additional parameters
        )

        logger.info(f"Extracted {len(all_data)} {entity_name} records")

        # Validate data through Pydantic models
        validated_data = []
        for record in all_data:
            try:
                # Pydantic handles datetime parsing automatically!
                model_instance = model_class(**record)
                validated_data.append(model_instance.model_dump())
            except Exception as e:
                logger.warning(f"Validation error for {entity_name} record: {e}")
                # Optionally skip invalid records or handle differently
                continue

        # Create Spark DataFrame using SparkDantic schema
        spark_schema = model_class.model_spark_schema()
        return self.spark.createDataFrame(validated_data, schema=spark_schema)

    def _get_entity_name(self, endpoint: str) -> str:
        """Derive entity name from endpoint.

        Args:
            endpoint: API endpoint

        Returns:
            Entity name
        """
        # Comprehensive endpoint mapping
        endpoint_mapping = {
            "/finance/agreements": "Agreement",
            "/finance/invoices": "UnpostedInvoice",  # Unposted invoices
            "/finance/invoices/posted": "PostedInvoice",  # Posted invoices
            "/time/entries": "TimeEntry",
            "/expense/entries": "ExpenseEntry",
            "/procurement/products": "ProductItem",
        }

        # Try exact match first
        if endpoint in endpoint_mapping:
            return endpoint_mapping[endpoint]

        # Try to extract entity name from endpoint
        # e.g., "/finance/invoices/123" -> "invoice"
        for ep, entity in endpoint_mapping.items():
            if endpoint.startswith(ep):
                return entity

        raise ETLConfigError(
            f"Cannot determine entity name from endpoint: {endpoint}",
            code=ErrorCode.CONFIG_INVALID,
            details={"endpoint": endpoint}
        )

    @with_etl_error_handling(operation="extract_all")
    def extract_all(self) -> dict[str, list[dict[str, Any]]]:
        """Extract all supported entities and return as dictionary of entity data.

        Returns:
            Dictionary mapping entity names to lists of record dictionaries
        """
        # Define all supported endpoints
        endpoints = {
            "agreement": "/finance/agreements",
            "time_entry": "/time/entries",
            "invoice": "/finance/invoices",
            "expense_entry": "/expense/entries",
            "product_item": "/procurement/products",
        }

        results = {}

        for entity_name, endpoint in endpoints.items():
            try:
                logger.info(f"Extracting {entity_name} from {endpoint}")

                # Get model class for field selection
                model_class = self.get_model_class(self._get_entity_name(endpoint))
                from .api_utils import get_fields_for_api_call

                fields_str = get_fields_for_api_call(model_class, max_depth=2)

                # Extract raw data using client
                raw_data = self.paginate(
                    endpoint=endpoint,
                    entity_name=f"{entity_name}s",
                    fields=fields_str,
                    page_size=1000,
                )

                results[entity_name] = raw_data
                logger.info(f"Extracted {len(raw_data)} {entity_name} records")

            except Exception as e:
                logger.error(f"Failed to extract {entity_name}: {e}")
                results[entity_name] = []  # Empty list for failed extractions

        return results


# Alias for backwards compatibility
ConnectWiseExtractor = ConnectWiseClient


__all__ = [
    "ApiErrorRecord",
    "ConnectWiseClient",
    "ConnectWiseExtractor",
]
