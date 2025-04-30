from __future__ import annotations  # noqa: I001

import base64
import importlib
import os
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

__all__ = ["ConnectWiseClient"]

"""fabric_api.client

ConnectWiseClient — single entry‑point that hides every authentication
edge‑case we've encountered so far:

* **Username / Password** (what you have working today)
* **Public + Private Integration Keys** (classic CW)
* **Azure Key Vault auto‑bootstrap** – if any secret is missing and the
  environment variable ``CW_KEYVAULT_URL`` is set, the client will use
  the Fabric workspace's managed identity (``DefaultAzureCredential``)
  to fetch the missing secrets at runtime.  Nothing to hard‑code in the
  notebook.

Secrets expected in Key Vault (override names via env‑vars if you like):

| Secret name            | Injected env‑var         |
|------------------------|---------------------------|
| ``cw-auth-username``   | ``CW_AUTH_USERNAME``      |
| ``cw-auth-password``   | ``CW_AUTH_PASSWORD``      |
| ``cw-company``         | ``CW_COMPANY``            |
| ``cw-public-key``      | ``CW_PUBLIC_KEY``         |
| ``cw-private-key``     | ``CW_PRIVATE_KEY``        |
| ``cw-client-id``       | ``CW_CLIENTID``           |

If both username/password **and** key‑pair are present the client will
prefer username/password.
"""


def _pull_from_key_vault() -> None:  # pragma: no cover
    """Populate missing CW_ env‑vars from Azure Key Vault if configured."""

    vault_url = os.getenv("CW_KEYVAULT_URL")
    if not vault_url:
        return  # nothing to do

    # Only import the Azure packages if we actually need them — avoids
    # adding hard deps for people who don't use Key Vault.
    try:
        azure_identity = importlib.import_module("azure.identity")
        azure_kv = importlib.import_module("azure.keyvault.secrets")
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

    mapping = {
        "CW_AUTH_USERNAME": "cw-auth-username",
        "CW_AUTH_PASSWORD": "cw-auth-password",
        "CW_COMPANY": "cw-company",
        "CW_PUBLIC_KEY": "cw-public-key",
        "CW_PRIVATE_KEY": "cw-private-key",
        "CW_CLIENTID": "cw-client-id",
    }

    for env_var, secret_name in mapping.items():
        if os.getenv(env_var):
            continue  # already set – env trumps Key Vault
        try:
            os.environ[env_var] = kv.get_secret(secret_name).value  # type: ignore[attr-defined]
        except Exception:  # pylint: disable=broad-except
            # Silently ignore missing secrets so the client can still
            # decide which mode to use based on what *is* present.
            pass


# ----------------------------------------------------------------------
# Main client class -----------------------------------------------------
# ----------------------------------------------------------------------


class ConnectWiseClient:
    """Thin, resilient wrapper around the ConnectWise Manage REST API."""

    BASE_URL: str = os.getenv("CW_BASE_URL", "https://verk.thekking.is/v4_6_release/apis/3.0")

    # ------------------------------------------------------------------
    # Construction ------------------------------------------------------
    # ------------------------------------------------------------------

    def __init__(
        self,
        *,
        # username / password mode -----------------------------------------
        basic_username: str | None = None,
        basic_password: str | None = None,
        # key‑pair mode ------------------------------------------------------
        company: str | None = None,
        public_key: str | None = None,
        private_key: str | None = None,
        # shared ------------------------------------------------------------
        client_id: str | None = None,
    ) -> None:
        # First opportunity to pull secrets from Key Vault → env‑vars
        _pull_from_key_vault()

        # Prefer explicit kwargs over environment variables.
        self.basic_username: str | None = basic_username or os.getenv("CW_AUTH_USERNAME")
        self.basic_password: str | None = basic_password or os.getenv("CW_AUTH_PASSWORD")

        self.company: str = company or os.getenv("CW_COMPANY", "")
        self.public_key: str = public_key or os.getenv("CW_PUBLIC_KEY", "")
        self.private_key: str = private_key or os.getenv("CW_PRIVATE_KEY", "")
        self.client_id: str = client_id or os.getenv("CW_CLIENTID", "")

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

        # HTTP session with automatic retry/back-off
        retry = Retry(total=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    # ------------------------------------------------------------------
    # Authentication helpers -------------------------------------------
    # ------------------------------------------------------------------

    def _basic_token(self) -> str:
        if self.basic_username and self.basic_password:
            token = f"{self.basic_username}:{self.basic_password}"
        else:  # fallback to key‑pair style
            token = f"{self.company}+{self.public_key}:{self.private_key}"
        return base64.b64encode(token.encode()).decode()

    def _headers(self) -> dict[str, str]:  # noqa: D401
        return {
            "Authorization": f"Basic {self._basic_token()}",
            "clientId": self.client_id,
            "Accept": "application/vnd.connectwise.com+json; version=2025.1",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Low‑level request wrappers ---------------------------------------
    # ------------------------------------------------------------------

    def get(self, endpoint: str, params: dict[str, Any] | None = None) -> requests.Response:
        """Make a GET request to the API."""
        url = f"{self.BASE_URL}{endpoint}"
        resp = self.session.get(url, headers=self._headers(), params=params or {})
        resp.raise_for_status()
        return resp

    def post(
        self, endpoint: str, data: dict[str, Any], params: dict[str, Any] | None = None
    ) -> requests.Response:
        """Make a POST request to the API."""
        url = f"{self.BASE_URL}{endpoint}"
        resp = self.session.post(url, headers=self._headers(), json=data, params=params or {})
        resp.raise_for_status()
        return resp

    def put(
        self, endpoint: str, data: dict[str, Any], params: dict[str, Any] | None = None
    ) -> requests.Response:
        """Make a PUT request to the API."""
        url = f"{self.BASE_URL}{endpoint}"
        resp = self.session.put(url, headers=self._headers(), json=data, params=params or {})
        resp.raise_for_status()
        return resp

    def delete(self, endpoint: str, params: dict[str, Any] | None = None) -> requests.Response:
        """Make a DELETE request to the API."""
        url = f"{self.BASE_URL}{endpoint}"
        resp = self.session.delete(url, headers=self._headers(), params=params or {})
        resp.raise_for_status()
        return resp

    def get_entity_data(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        entity_name: str = "items",
        max_pages: int | None = 50,
    ) -> list[dict[str, Any]]:
        """Generic function to get paginated data from any ConnectWise endpoint."""
        items = []
        page = 1
        page_size = 100

        while True:
            # Check if we've reached max pages limit
            if max_pages is not None and page > max_pages:
                print(f"Reached maximum page limit of {max_pages}. Stopping.")
                break

            url = f"{self.BASE_URL}{endpoint}"
            print(f"Requesting {entity_name} page {page}: {url}")

            try:
                query = {"page": page, "pageSize": page_size, **(params or {})}
                response = self.get(endpoint, query)

                if response.status_code == 200:
                    data = response.json()
                    items.extend(data)
                    print(f"Retrieved {len(data)} {entity_name} on page {page}")

                    # Check if we've retrieved all data
                    if len(data) < page_size:
                        break

                    page += 1
                else:
                    print(f"Error: {response.status_code}")
                    print(response.text)
                    break
            except Exception as e:
                print(f"Request failed: {e}")
                break

        print(f"Total {entity_name} retrieved: {len(items)}")
        return items
