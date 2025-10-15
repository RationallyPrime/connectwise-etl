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

__version__ = "0.1.0"

__all__ = [
    # Configuration
    "LakehouseConfig",
    "RuntimeContext",
    # Protocols
    "DataFetcherProtocol",
    "DataValidatorProtocol",
    "ModelRegistryProtocol",
    "BronzeProcessorProtocol",
    "SilverProcessorProtocol",
    "GoldProcessorProtocol",
    "IntegrationPluginProtocol",
    # Result types
    "ValidationResult",
    "BronzeResult",
    "SilverResult",
    "GoldResult",
    "MergeResult",
    "Processors",
    # Type aliases
    "TSparkModelClass",
    "TConfigDict",
    "TLayer",
    "TLoadMode",
]
