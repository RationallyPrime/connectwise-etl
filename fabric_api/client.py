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

class ConnectWiseClient:  # noqa: WPS110 – past FF naming convention
    """Thin, resilient wrapper around the ConnectWise Manage REST API."""

    BASE_URL: str = os.getenv(
        "CW_BASE_URL",
        "https://verk.thekking.is/v4_6_release/apis/3.0",  # sensible default for Wise
    )

    # ---------------------------------------------------------------------
    # Construction
    # ---------------------------------------------------------------------

    def __init__(
        self,
        *,
        # Username/password mode ------------------------------------------------
        basic_username: str | None = None,
        basic_password: str | None = None,
        # Key‑pair mode ---------------------------------------------------------
        company: str | None = None,
        public_key: str | None = None,
        private_key: str | None = None,
        # Shared ----------------------------------------------------------------
        client_id: str | None = None,
    ) -> None:
        _pull_from_key_vault()

        # Prefer explicit kwargs over environment variables
        self.basic_username = basic_username or os.getenv("CW_AUTH_USERNAME")
        self.basic_password = basic_password or os.getenv("CW_AUTH_PASSWORD")

        self.company = company or os.getenv("CW_COMPANY", "")
        self.public_key = public_key or os.getenv("CW_PUBLIC_KEY", "")
        self.private_key = private_key or os.getenv("CW_PRIVATE_KEY", "")
        self.client_id = client_id or os.getenv("CW_CLIENTID", "")

        if not self.client_id:
            raise RuntimeError("ConnectWiseClient — missing CW_CLIENTID (or secret 'cw-client-id')")

        if not (self.basic_username and self.basic_password) and not (
            self.company and self.public_key and self.private_key
        ):
            raise RuntimeError(
                "ConnectWiseClient — supply either username/password (CW_AUTH_*) "
                "or company/public/private key triplet (CW_*).  Secrets can live in "
                "Azure Key Vault; set CW_KEYVAULT_URL to enable auto‑bootstrap."
            )

        # HTTP session with automatic retry/back‑off
        retry: Retry = Retry(total=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
        self.session = requests.Session()
        self.session.mount(prefix="https://", adapter=HTTPAdapter(max_retries=retry))

    # ---------------------------------------------------------------------
    # Helpers — auth & headers
    # ---------------------------------------------------------------------

    def _basic_token(self) -> str:  # noqa: D401
        if self.basic_username and self.basic_password:
            token = f"{self.basic_username}:{self.basic_password}"
        else:
            token = f"{self.company}+{self.public_key}:{self.private_key}"
        return base64.b64encode(token.encode()).decode()

    def _headers(self) -> dict[str, str]:  # noqa: D401
        return {
            "Authorization": f"Basic {self._basic_token()}",
            "clientId": self.client_id,
            "Accept": "application/vnd.connectwise.com+json; version=2025.1",
            "Content-Type": "application/json",
        }

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

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> requests.Response:
        """Make a **GET** request with automatic retry/back‑off."""

        url: str = f"{self.BASE_URL}{endpoint}"
        logger.debug("GET %s", url)
        resp: Response = self.session.get(url, headers=self._headers(), params=params or {})
        resp.raise_for_status()
        return resp

    def post(
        self,
        endpoint: str,
        data: dict[str, Any],
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        url: str = f"{self.BASE_URL}{endpoint}"
        logger.debug("POST %s", url)
        resp: Response = self.session.post(url, headers=self._headers(), json=data, params=params or {})
        resp.raise_for_status()
        return resp

    def put(
        self,
        endpoint: str,
        data: dict[str, Any],
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        url = f"{self.BASE_URL}{endpoint}"
        logger.debug("PUT %s", url)
        resp: Response = self.session.put(url, headers=self._headers(), json=data, params=params or {})
        resp.raise_for_status()
        return resp

    def delete(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> requests.Response:
        url: str = f"{self.BASE_URL}{endpoint}"
        logger.debug("DELETE %s", url)
        resp: Response = self.session.delete(url, headers=self._headers(), params=params or {})
        resp.raise_for_status()
        return resp

    # ---------------------------------------------------------------------
    # Pagination helpers
    # ---------------------------------------------------------------------

    def get_entity_data(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        entity_name: str = "items",
        max_pages: int | None = 50,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Iterate through paginated GET responses and return **items** only."""

        items, _ = self.get_entity_data_with_detailed_logging(
            endpoint,
            params=params,
            entity_name=entity_name,
            max_pages=max_pages,
            page_size=page_size,
            error_tracking=False,
        )
        return items

    def get_entity_data_with_detailed_logging(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        entity_name: str = "items",
        max_pages: int | None = 50,
        page_size: int = 100,
        error_tracking: bool = True,
    ) -> tuple[list[dict[str, Any]], list[ApiErrorRecord]]:
        """Paginate **GET** requests and capture API‑level failures.

        Returns a tuple ``(items, errors)`` where *errors* is a list of
        :class:`ApiErrorRecord` — callers can decide whether to persist or
        ignore.
        """

        page = 1
        items: list[dict[str, Any]] = []
        errors: list[ApiErrorRecord] = []
        params = params or {}

        while True:
            if max_pages is not None and page > max_pages:
                logger.warning(
                    "Aborting pagination after %s pages for endpoint %s", max_pages, endpoint
                )
                break

            query: dict[str, Any] = {"page": page, "pageSize": page_size, **params}
            url: str = f"{self.BASE_URL}{endpoint}"
            logger.debug("Requesting %s page %s: %s", entity_name, page, url)

            try:
                response: Response = self.get(endpoint, params=query)
                data = response.json()
                if not isinstance(data, list):
                    logger.error("Unexpected JSON shape for %s page %s: %s", entity_name, page, data)
                    raise ValueError("Expected list response from ConnectWise API")

                items.extend(data)
                logger.debug("Retrieved %s %s on page %s", len(data), entity_name, page)

                if len(data) < page_size:  # last page
                    break
                page += 1
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Error fetching %s page %s: %s", entity_name, page, exc)
                if error_tracking:
                    errors.append(
                        ApiErrorRecord(
                            invoice_number=params.get("invoiceNumber", ""),
                            error_table_id=0,
                            error_type="PaginationError",
                            error_message=str(exc),
                            table_name=entity_name,
                        )
                    )
                break  # bail on consistent failures to avoid infinite loop

        logger.info("Total %s retrieved from %s: %s", entity_name, endpoint, len(items))
        return items, errors
