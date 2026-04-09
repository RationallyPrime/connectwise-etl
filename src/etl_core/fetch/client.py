"""Synchronous httpx data fetcher — generic executor for EndpointConfig.

Reads configuration, resolves auth from environment, paginates according
to the configured strategy, and yields raw dicts.

Ported from sokrates/hyle (async → sync for Spark/Fabric compatibility).
"""

from __future__ import annotations

import base64
import logging
import os
from collections.abc import Iterator
from typing import Any

import httpx

from etl_core.utils.errors import APIErrorDetails, ETLConfigError, FetchError

from .models import (
    ApiKeyAuth,
    Auth,
    BasicAuth,
    BearerAuth,
    CursorPagination,
    EndpointConfig,
    OffsetLimitPagination,
    PageNumberPagination,
)

logger = logging.getLogger(__name__)


# -- Response helpers --------------------------------------------------------


def _extract_data(response_body: Any, data_path: str) -> list[dict[str, Any]]:
    """Extract the data array from a response body using a dot-separated path.

    Empty string means the root IS the array.
    """
    if not data_path:
        return response_body if isinstance(response_body, list) else []

    current = response_body
    for key in data_path.split("."):
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return []

    return current if isinstance(current, list) else []


def _extract_cursor(response_body: Any, cursor_path: str) -> str | None:
    """Extract the next cursor value from a response body."""
    current = response_body
    for key in cursor_path.split("."):
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current if isinstance(current, str) else None


# -- Fetcher -----------------------------------------------------------------


class HttpxFetcher:
    """Sync data fetcher driven by EndpointConfig.

    Resolves auth from environment, paginates per strategy, yields raw dicts.
    Uses httpx with transport-level retries for resilience.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        retries: int = 5,
    ) -> None:
        transport = httpx.HTTPTransport(retries=retries)
        self._client = httpx.Client(timeout=timeout, transport=transport)

    # -- Auth resolution -----------------------------------------------------

    @staticmethod
    def _resolve_auth_headers(auth: Auth) -> dict[str, str]:
        """Resolve auth credentials from environment variables."""
        if isinstance(auth, ApiKeyAuth):
            value = os.environ.get(auth.env_var)
            if not value:
                raise ETLConfigError(
                    f"Environment variable {auth.env_var} not set",
                    details={
                        "source": "etl_core.fetch",
                        "operation": "resolve_auth",
                    },
                )
            return {auth.header: value}

        if isinstance(auth, BearerAuth):
            value = os.environ.get(auth.env_var)
            if not value:
                raise ETLConfigError(
                    f"Environment variable {auth.env_var} not set",
                    details={
                        "source": "etl_core.fetch",
                        "operation": "resolve_auth",
                    },
                )
            return {"Authorization": f"Bearer {value}"}

        if isinstance(auth, BasicAuth):
            username = os.environ.get(auth.username_env)
            password = os.environ.get(auth.password_env)
            if not username or not password:
                raise ETLConfigError(
                    f"Environment variables {auth.username_env}/{auth.password_env} not set",
                    details={
                        "source": "etl_core.fetch",
                        "operation": "resolve_auth",
                    },
                )
            encoded = base64.b64encode(f"{username}:{password}".encode()).decode()
            return {"Authorization": f"Basic {encoded}"}

        return {}

    # -- HTTP request --------------------------------------------------------

    def _request(
        self,
        config: EndpointConfig,
        params: dict[str, Any],
    ) -> Any:
        """Make a single HTTP request and return parsed JSON."""
        url = f"{config.base_url}{config.path}"
        auth_headers = self._resolve_auth_headers(config.auth)
        headers = {**config.headers, **auth_headers}
        merged_params = {**config.query_params, **params}

        response = self._client.request(
            method=config.method,
            url=url,
            headers=headers,
            params=merged_params,
        )

        if response.status_code >= 400:
            raise FetchError(
                f"HTTP {response.status_code} from {url}",
                details=APIErrorDetails(
                    source="etl_core.fetch",
                    operation="request",
                    endpoint=config.path,
                    status_code=response.status_code,
                ),
            )

        return response.json()

    # -- Pagination strategies -----------------------------------------------

    def _fetch_offset_limit(
        self,
        config: EndpointConfig,
        pagination: OffsetLimitPagination,
    ) -> Iterator[dict[str, Any]]:
        offset = 0
        while True:
            params = {
                pagination.offset_param: offset,
                pagination.limit_param: pagination.page_size,
            }
            body = self._request(config, params)
            records = _extract_data(body, config.data_path)
            if not records:
                break
            yield from records
            if len(records) < pagination.page_size:
                break
            offset += len(records)

    def _fetch_cursor(
        self,
        config: EndpointConfig,
        pagination: CursorPagination,
    ) -> Iterator[dict[str, Any]]:
        cursor: str | None = None
        while True:
            params: dict[str, Any] = {}
            if cursor:
                params[pagination.cursor_param] = cursor
            body = self._request(config, params)
            records = _extract_data(body, config.data_path)
            yield from records
            cursor = _extract_cursor(body, pagination.cursor_path)
            if not cursor:
                break

    def _fetch_page_number(
        self,
        config: EndpointConfig,
        pagination: PageNumberPagination,
    ) -> Iterator[dict[str, Any]]:
        page = 1
        while True:
            params = {
                pagination.page_param: page,
                pagination.size_param: pagination.page_size,
            }
            body = self._request(config, params)
            records = _extract_data(body, config.data_path)
            if not records:
                break
            yield from records
            if len(records) < pagination.page_size:
                break
            page += 1

    # -- Public API ----------------------------------------------------------

    def fetch(self, config: EndpointConfig) -> Iterator[dict[str, Any]]:
        """Fetch all records from an endpoint, paginating automatically."""
        pagination = config.pagination

        if isinstance(pagination, OffsetLimitPagination):
            yield from self._fetch_offset_limit(config, pagination)
        elif isinstance(pagination, CursorPagination):
            yield from self._fetch_cursor(config, pagination)
        elif isinstance(pagination, PageNumberPagination):
            yield from self._fetch_page_number(config, pagination)

    def fetch_all(
        self, configs: list[EndpointConfig]
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch records from multiple endpoints.

        Returns dict mapping entity_name to list of raw records.
        """
        result: dict[str, list[dict[str, Any]]] = {}
        for config in configs:
            records = list(self.fetch(config))
            result[config.entity_name] = records
            logger.info(
                "fetch_complete: entity=%s records=%d",
                config.entity_name,
                len(records),
            )
        return result

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()
