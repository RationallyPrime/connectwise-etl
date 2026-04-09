"""ETL Core Framework.

A protocol-based ETL framework for medallion architecture with plugin support.
"""

from .config import LakehouseConfig, RuntimeContext
from .domain.protocols import (
    BronzeProcessorProtocol,
    BronzeResult,
    DataFetcherProtocol,
    DataValidatorProtocol,
    GoldProcessorProtocol,
    GoldResult,
    IntegrationPluginProtocol,
    MergeResult,
    ModelRegistryProtocol,
    Processors,
    SilverProcessorProtocol,
    SilverResult,
    TConfigDict,
    TLayer,
    TLoadMode,
    TSparkModelClass,
    ValidationResult,
)
from .fetch import (
    ApiKeyAuth,
    Auth,
    BasicAuth,
    BearerAuth,
    CursorPagination,
    EndpointConfig,
    HttpxFetcher,
    OffsetLimitPagination,
    PageNumberPagination,
    Pagination,
)

__version__ = "0.1.0"

__all__ = [
    # Fetch layer
    "ApiKeyAuth",
    "Auth",
    "BasicAuth",
    "BearerAuth",
    "BronzeProcessorProtocol",
    "BronzeResult",
    "CursorPagination",
    # Protocols
    "DataFetcherProtocol",
    "DataValidatorProtocol",
    "EndpointConfig",
    "GoldProcessorProtocol",
    "GoldResult",
    "HttpxFetcher",
    "IntegrationPluginProtocol",
    # Configuration
    "LakehouseConfig",
    "MergeResult",
    "ModelRegistryProtocol",
    "OffsetLimitPagination",
    "PageNumberPagination",
    "Pagination",
    "Processors",
    "RuntimeContext",
    "SilverProcessorProtocol",
    "SilverResult",
    "TConfigDict",
    "TLayer",
    "TLoadMode",
    # Type aliases
    "TSparkModelClass",
    # Result types
    "ValidationResult",
]
