from __future__ import annotations

import base64
import os
from collections.abc import Generator
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

"""fabric_api.extract

Registry‑centric extraction layer with an embedded ConnectWiseClient.
The client handles authentication (Basic with public/private keys),
retry logic, paging, and exposes helper methods that mirror the
ConnectWise Manage REST endpoints used by the legacy AL code.
"""


class ConnectWiseClient:  # pylint: disable=too-few-public-methods
    """Thin wrapper around the ConnectWise Manage REST API."""

    _BASE: str = os.getenv("CW_BASE_URL", "https://verk.thekking.is/v4_6_release/apis/3.0")
    _COMPANY: str = os.getenv("CW_COMPANY", "")
    _PUBLIC: str = os.getenv("CW_PUBLIC_KEY", "")
    _PRIVATE: str = os.getenv("CW_PRIVATE_KEY", "")
    _CLIENT: str = os.getenv("CW_CLIENTID", "")

    def __init__(self) -> None:
        retry = Retry(total=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
        self._session = requests.Session()
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------

    def _auth_header(self) -> str:
        """Return the *Basic* auth header value required by CW."""
        token = f"{self._COMPANY}+{self._PUBLIC}:{self._PRIVATE}"
        return "Basic " + base64.b64encode(token.encode()).decode()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": self._auth_header(),
            "clientId": self._CLIENT,
            "Accept": "application/vnd.connectwise.com+json; version=2025.1",
            "Content-Type": "application/json",
        }

    def _get(self, endpoint: str, params: Dict[str, Any] | None = None) -> requests.Response:
        url = f"{self._BASE}{endpoint}"
        response = self._session.get(url, headers=self._headers(), params=params or {})
        response.raise_for_status()
        return response

    def _paginate(
        self,
        endpoint: str,
        params: Dict[str, Any] | None = None,
        page_size: int = 100,
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield items across *all* pages for a given endpoint."""
        page = 1
        while True:
            query = {"page": page, "pageSize": page_size, **(params or {})}
            batch: List[Dict[str, Any]] = self._get(endpoint, query).json()
            if not batch:
                break
            yield from batch
            if len(batch) < page_size:
                break
            page += 1

    # ------------------------------------------------------------------
    # Domain‑specific convenience wrappers
    # ------------------------------------------------------------------

    def list_time_entries(
        self, date_from: str, date_to: str, **filters: Any
    ) -> List[Dict[str, Any]]:
        """Return all time entries between two dates (inclusive).

        A *conditions* string in **filters overrides the default date filter.
        """
        cond = filters.pop("conditions", "")
        range_cond = (
            f"timeStart>='{date_from}' and timeEnd<='{date_to}'" if date_from and date_to else ""
        )
        full_cond = f"{range_cond} and {cond}" if range_cond and cond else (cond or range_cond)
        params = {"conditions": full_cond} if full_cond else {}
        params.update(filters)
        return list(self._paginate("/time/entries", params))

    def list_invoices(self, **params: Any) -> List[Dict[str, Any]]:  # noqa: D401
        """Return all invoices (optionally filtered with **params)."""
        return list(self._paginate("/finance/invoices", params))

    def list_reports(self, report_id: str | None = None, **params: Any):
        """If *report_id* is supplied, get that report once; otherwise list all."""
        if report_id:
            return self._get(f"/system/reports/{report_id}", params).json()
        return list(self._paginate("/system/reports", params))


# -------------------------------------------------------------------------
# Public extraction registry
# -------------------------------------------------------------------------

_cw = ConnectWiseClient()

REGISTRY: Dict[str, Any] = {
    "reports": lambda **kw: _cw.list_reports(**kw),
    "companies": lambda **kw: _cw._paginate("/company/companies", kw),
    "contacts": lambda **kw: _cw._paginate("/company/contacts", kw),
    "tickets": lambda **kw: _cw._paginate("/service/tickets", kw),
    "time_entries": lambda **kw: _cw.list_time_entries(**kw),
    "invoices": lambda **kw: _cw.list_invoices(**kw),
}


def extract(entity: str, **params) -> List[Dict[str, Any]]:  # noqa: D401
    """Return a fully‑materialised list for *entity* using the registry."""
    try:
        return list(REGISTRY[entity](**params))
    except KeyError as exc:  # pragma: no cover
        raise ValueError(f"Unsupported entity: {entity}") from exc
