"""Generic, config-driven data fetcher for the ETL framework.

Ported from sokrates/hyle — provides declarative endpoint configuration
with pluggable auth and pagination strategies via discriminated unions.
"""

from .client import HttpxFetcher
from .models import (
    ApiKeyAuth,
    Auth,
    BasicAuth,
    BearerAuth,
    CursorPagination,
    EndpointConfig,
    OffsetLimitPagination,
    PageNumberPagination,
    Pagination,
)

__all__ = [
    # Auth
    "ApiKeyAuth",
    "Auth",
    "BasicAuth",
    "BearerAuth",
    # Pagination
    "CursorPagination",
    # Config
    "EndpointConfig",
    # Client
    "HttpxFetcher",
    "OffsetLimitPagination",
    "PageNumberPagination",
    "Pagination",
]
