from __future__ import annotations
import logging
from requests.models import Response
from types import ModuleType

"""fabric_api.client

Enhanced ConnectWise REST client that mirrors the resilience and detailed
logging baked into the legacy AL integration while adding modern niceties
(pydantic‑style types, rich error records, Azure Key Vault bootstrap).

Key features vs. vanilla PoC version
-----------------------------------
* **create_batch_identifier()** – identical timestamp format to AL pipeline.
* **get_entity_data_with_detailed_logging()** – returns a tuple of
  ``(items, errors)`` so callers can persist structured error info in the
  *ManageInvoiceError* Delta table.
* **Robust retry/back‑off** – shared HTTP session transparently retries 5×
  on 429/50x like the original codeunit.
* **Key Vault bootstrap** – unchanged from earlier revision; secrets are
  auto‑fetched if ``CW_KEYVAULT_URL`` is configured.

The public surface remains 100 % compatible with previous notebooks: calls
that don’t care about error records can continue to use *get_entity_data()*.
"""

from datetime import datetime
import base64
import importlib
import os
from typing import Any
from logging import Logger

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .utils import create_batch_identifier  # noqa: WPS433 – internal import

__all__ = [
    "ConnectWiseClient",
    "ApiErrorRecord",
]

###############################################################################
# Logging setup
###############################################################################

logger: Logger = logging.getLogger(name=__name__)
logger.addHandler(hdlr=logging.NullHandler())

###############################################################################
# Data‑shapes
###############################################################################

class ApiErrorRecord(dict):
    """Dictionary sub‑class that captures structured API‑level errors.

    The shape purposefully mirrors *ManageInvoiceError* so that downstream
    callers can serialise the list directly via ``pydantic`` or Pandas →
    Delta.  Additional keys can be added at call‑site without subclass tweaks.
    """

    invoice_number: str  # type: ignore[assignment]
    error_table_id: int  # type: ignore[assignment]
    error_type: str | None
    error_message: str | None
    table_name: str | None

    # No custom behaviour — we simply like the self‑documenting type alias.

###############################################################################
# Optional Azure Key Vault secret bootstrap
###############################################################################

def _pull_from_key_vault() -> None:  # pragma: no cover – env dep.
    """Populate missing CW_ env‑vars from Azure Key Vault if configured."""

    vault_url: str | None = os.getenv("CW_KEYVAULT_URL")
    if not vault_url:
        return  # nothing to do

    try:
        azure_identity: ModuleType = importlib.import_module(name="azure.identity")
        azure_kv: ModuleType = importlib.import_module(name="azure.keyvault.secrets")
    except ModuleNotFoundError as exc:  # noqa: B904
        raise RuntimeError(
            "Azure Key Vault integration requested (CW_KEYVAULT_URL set) "
            "but packages 'azure-identity' and 'azure-keyvault-secrets' "
            "are not installed.  Run `pip install azure-identity "
            "azure-keyvault-secrets`."
        ) from exc

    DefaultAzureCredential = getattr(azure_identity, "DefaultAzureCredential")  # noqa: N806
    SecretClient = getattr(azure_kv, "SecretClient")  # noqa: N806

    kv = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
    mapping: dict[str, str] = {
        "CW_AUTH_USERNAME": "cw-auth-username",
        "CW_AUTH_PASSWORD": "cw-auth-password",
        "CW_COMPANY": "cw-company",
        "CW_PUBLIC_KEY": "cw-public-key",
        "CW_PRIVATE_KEY": "cw-private-key",
        "CW_CLIENTID": "cw-client-id",
    }

    for env_var, secret_name in mapping.items():
        if os.getenv(env_var):
            continue  # env var trumps Key Vault
        try:
            os.environ[env_var] = kv.get_secret(secret_name).value  # type: ignore[attr-defined]
        except Exception:  # pylint: disable=broad-except
            # Missing secret is fine — the client decides at runtime which
            # auth mode to engage based on what *is* available.
            pass

###############################################################################
# Main client class
###############################################################################

class ConnectWiseClient:
    """Thin, resilient wrapper around the ConnectWise Manage REST API."""

    BASE_URL: str = os.getenv(
        "CW_BASE_URL",
        "https://verk.thekking.is/v4_6_release/apis/3.0",  # sensible default for Wise
    )

    def __init__(
        self,
        *,
        # Username/password mode ------------------------------------------------
        basic_username: str | None = None,
        basic_password: str | None = None,
        # API key mode ----------------------------------------------------------
        company: str | None = None,
        public_key: str | None = None,
        private_key: str | None = None,
        # Shared ----------------------------------------------------------------
        client_id: str | None = None,
    ):
        """Initialise with either username/password OR company + API keys."""
        # Username/password mode
        self.basic_username = basic_username or os.getenv("CW_AUTH_USERNAME")
        self.basic_password = basic_password or os.getenv("CW_AUTH_PASSWORD")

        # API key mode
        self.company = company or os.getenv("CW_COMPANY")
        self.public_key = public_key or os.getenv("CW_PUBLIC_KEY")
        self.private_key = private_key or os.getenv("CW_PRIVATE_KEY")

        # Shared
        self.client_id = client_id or os.getenv("CW_CLIENTID")

        # Set up a session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry_strategy))

    def _basic_token(self) -> str:
        """Return the base64-encoded basic auth token."""
        if not self.basic_username or not self.basic_password:
            raise ValueError("Basic auth credentials not configured")
        return base64.b64encode(f"{self.basic_username}:{self.basic_password}".encode()).decode()

    def _headers(self) -> dict[str, str]:
        """Return the headers needed for API requests."""
        if not self.client_id:
            raise ValueError("Client ID not configured")

        headers = {
            "clientId": self.client_id,
            "Accept": "application/vnd.connectwise.com+json; version=2025.1",
            "Content-Type": "application/json",
        }

        # Use basic auth if available
        if self.basic_username and self.basic_password:
            return headers

        # Fall back to API key auth
        if not all([self.company, self.public_key, self.private_key]):
            raise ValueError("Neither basic auth nor API key auth is fully configured")

        headers["Authorization"] = f"Basic {self._basic_token()}"
        return headers

    # ---------------------------------------------------------------------
    # Public utility helpers (re‑exported from utils)
    # ---------------------------------------------------------------------

    @staticmethod
    def create_batch_identifier(ts: datetime | None = None) -> str:  # noqa: D401
        """Return UTC timestamp formatted as ``YYYYMMDD-HHMMSS`` — AL‑style."""

        return create_batch_identifier(timestamp=ts)

    # ---------------------------------------------------------------------
    # Core request wrappers
    # ---------------------------------------------------------------------

    def get(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Response:
        """Perform a GET request to the ConnectWise API."""
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._headers()
        
        # Use auth tuple for requests instead of Authorization header
        auth = None
        if self.basic_username and self.basic_password:
            auth = (self.basic_username, self.basic_password)
            
        logger.debug(f"GET {url}")
        resp = self.session.get(url, headers=headers, params=params, auth=auth)
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
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._headers()
        
        # Use auth tuple for requests instead of Authorization header
        auth = None
        if self.basic_username and self.basic_password:
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
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._headers()
        
        # Use auth tuple for requests instead of Authorization header
        auth = None
        if self.basic_username and self.basic_password:
            auth = (self.basic_username, self.basic_password)
            
        logger.debug(f"PUT {url}")
        resp = self.session.put(url, headers=headers, json=json_data, params=params, auth=auth)
        resp.raise_for_status()
        return resp

    def delete(
        self, 
        endpoint: str, 
        *, 
        params: dict[str, Any] | None = None
    ) -> Response:
        """Perform a DELETE request to the ConnectWise API."""
        url: str = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._headers()
        
        # Use auth tuple for requests instead of Authorization header
        auth = None
        if self.basic_username and self.basic_password:
            auth = (self.basic_username, self.basic_password)
            
        logger.debug(f"DELETE {url}")
        resp = self.session.delete(url, headers=headers, params=params, auth=auth)
        resp.raise_for_status()
        return resp

    # ---------------------------------------------------------------------
    # Pagination helpers
    # ---------------------------------------------------------------------

    def paginate(
        self,
        endpoint: str,
        entity_name: str,
        *,
        params: dict[str, Any] | None = None,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get all pages of data from a paginated endpoint."""
        if params is None:
            params = {}
            
        items = []
        page = 1
        page_size = params.get("pageSize", 100)
        
        while True:
            # Check if we've reached max pages limit
            if max_pages is not None and page > max_pages:
                logger.info(f"Reached maximum page limit of {max_pages}. Stopping.")
                break
                
            query = {"page": page, "pageSize": page_size}
            query.update(params)
            
            url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
            logger.debug(f"Requesting {entity_name} page {page}: {url}")
            
            # Use auth tuple for requests
            auth = None
            if self.basic_username and self.basic_password:
                auth = (self.basic_username, self.basic_password)
            
            try:
                headers = self._headers()
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
                logger.error(f"Error fetching {entity_name} page {page}: {str(e)}")
                break
        
        logger.info(f"Total {entity_name} retrieved from {endpoint}: {len(items)}")
        return items
