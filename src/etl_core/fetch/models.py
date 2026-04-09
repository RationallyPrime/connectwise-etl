"""Declarative endpoint configuration with pluggable auth and pagination.

EndpointConfig encodes everything needed to fetch data from any API endpoint.
Auth and Pagination are discriminated unions — extend with new strategies by
adding a new variant to the union.

Ported from sokrates/hyle.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

# -- Auth --------------------------------------------------------------------


class ApiKeyAuth(BaseModel):
    """API key sent in a named header (resolved from env at runtime)."""

    model_config = ConfigDict(frozen=True)

    auth_type: Literal["api_key"] = "api_key"
    header: str = "X-API-Key"
    env_var: str


class BearerAuth(BaseModel):
    """Bearer token in Authorization header (resolved from env at runtime)."""

    model_config = ConfigDict(frozen=True)

    auth_type: Literal["bearer"] = "bearer"
    env_var: str


class BasicAuth(BaseModel):
    """HTTP Basic Auth (credentials resolved from env at runtime)."""

    model_config = ConfigDict(frozen=True)

    auth_type: Literal["basic"] = "basic"
    username_env: str
    password_env: str


Auth = Annotated[
    ApiKeyAuth | BearerAuth | BasicAuth,
    Field(discriminator="auth_type"),
]


# -- Pagination --------------------------------------------------------------


class OffsetLimitPagination(BaseModel):
    """Skip/limit pagination (e.g. many REST APIs)."""

    model_config = ConfigDict(frozen=True)

    strategy: Literal["offset_limit"] = "offset_limit"
    page_size: int = 100
    offset_param: str = "skip"
    limit_param: str = "limit"


class CursorPagination(BaseModel):
    """Cursor-based pagination (e.g. Stripe, Slack)."""

    model_config = ConfigDict(frozen=True)

    strategy: Literal["cursor"] = "cursor"
    page_size: int = 100
    cursor_param: str = "cursor"
    cursor_path: str  # dot-path to next cursor in response body


class PageNumberPagination(BaseModel):
    """Page-number pagination (e.g. ConnectWise, DRF, legacy APIs)."""

    model_config = ConfigDict(frozen=True)

    strategy: Literal["page_number"] = "page_number"
    page_size: int = 100
    page_param: str = "page"
    size_param: str = "pageSize"


Pagination = Annotated[
    OffsetLimitPagination | CursorPagination | PageNumberPagination,
    Field(discriminator="strategy"),
]


# -- Endpoint Configuration --------------------------------------------------


class EndpointConfig(BaseModel):
    """Declarative configuration for fetching data from an API endpoint.

    Encodes everything needed to drive an HTTP client: URL construction,
    authentication strategy, pagination strategy, response parsing,
    rate limiting, and static headers/query params.

    Serialisable to JSON/YAML for versioning alongside API specs.
    """

    model_config = ConfigDict(frozen=True)

    base_url: str
    path: str
    method: str = "GET"
    entity_name: str  # logical name in the ETL pipeline (e.g. "agreement")

    auth: Auth
    pagination: Pagination

    data_path: str = ""  # dot-path to array in response (empty = root is array)
    rate_limit_rpm: int | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    query_params: dict[str, str] = Field(default_factory=dict)
