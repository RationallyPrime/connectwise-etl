"""ConnectWise data fetcher — adapts the generic HttpxFetcher to the ETL protocol."""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

from etl_core.domain.protocols import TConfigDict, TLoadMode
from etl_core.fetch import BasicAuth, EndpointConfig, HttpxFetcher, PageNumberPagination
from etl_core.utils.errors import FetchError


class ConnectWiseFetcher:
    """Fetches data from ConnectWise API via the generic HttpxFetcher.

    Builds an EndpointConfig per entity from the registry config,
    wiring in ConnectWise-specific auth, pagination, and headers.
    """

    CW_ACCEPT_HEADER = "application/vnd.connectwise.com+json; version=2025.1"

    def __init__(self, page_size: int = 1000) -> None:
        base_url = os.getenv(
            "CW_BASE_URL",
            "https://eu.myconnectwise.net/v4_6_release/apis/3.0",
        )
        client_id = os.getenv("CW_CLIENTID", "")

        self._base_url = base_url
        self._auth = BasicAuth(
            username_env="CW_AUTH_USERNAME",
            password_env="CW_AUTH_PASSWORD",
        )
        self._default_headers = {
            "clientId": client_id,
            "Accept": self.CW_ACCEPT_HEADER,
            "Content-Type": "application/json",
        }
        self._default_page_size = page_size
        self._fetcher = HttpxFetcher(timeout=30.0, retries=5)

    def _build_endpoint_config(
        self,
        entity_name: str,
        config: TConfigDict,
        *,
        conditions: str | None = None,
        fields: str | None = None,
        page_size: int | None = None,
    ) -> EndpointConfig:
        """Build an EndpointConfig from registry entity config."""
        endpoint = config.get("endpoint")
        if not endpoint:
            raise FetchError(
                f"No endpoint configured for entity: {entity_name}",
                details={
                    "source": "ConnectWiseFetcher",
                    "operation": "build_endpoint_config",
                },
            )

        query_params: dict[str, str] = {}
        if conditions:
            query_params["conditions"] = conditions
        if fields:
            query_params["fields"] = fields

        return EndpointConfig(
            base_url=self._base_url,
            path=f"/{endpoint.lstrip('/')}",
            entity_name=entity_name,
            auth=self._auth,
            pagination=PageNumberPagination(
                page_size=page_size or self._default_page_size,
                page_param="page",
                size_param="pageSize",
            ),
            headers=self._default_headers,
            query_params=query_params,
        )

    def fetch_raw(
        self,
        entity_name: str,
        mode: TLoadMode,
        config: TConfigDict,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        """Fetch raw records from ConnectWise API.

        Builds an EndpointConfig from the entity's registry config,
        then delegates to HttpxFetcher for paginated retrieval.
        """
        conditions: str | None = None
        if mode == "incremental":
            from datetime import datetime, timedelta

            lookback_days = kwargs.get("lookback_days", 7)
            since_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            conditions = f"lastUpdated > [{since_date}]"

        fields_str: str | None = config.get("fields")
        page_size: int | None = kwargs.get("page_size")

        endpoint_config = self._build_endpoint_config(
            entity_name,
            config,
            conditions=conditions,
            fields=fields_str,
            page_size=page_size,
        )

        yield from self._fetcher.fetch(endpoint_config)

    def test_connection(self) -> bool:
        """Verify connectivity with ConnectWise API."""
        try:
            test_config = EndpointConfig(
                base_url=self._base_url,
                path="/system/info",
                entity_name="_test",
                auth=self._auth,
                pagination=PageNumberPagination(page_size=1),
                headers=self._default_headers,
            )
            self._fetcher._request(test_config, {})
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._fetcher.close()
