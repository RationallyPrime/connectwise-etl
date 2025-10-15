"""Domain protocols for ETL framework.

These protocols define the contracts that integrations must implement.
They have no dependencies on infrastructure or implementation details.
"""

from .client import DataFetcherProtocol, DataValidatorProtocol, ValidationResult
from .plugin import IntegrationPluginProtocol, Processors
from .processor import BronzeProcessorProtocol, GoldProcessorProtocol, SilverProcessorProtocol
from .registry import ModelRegistryProtocol
from .types import (
    BronzeResult,
    GoldResult,
    MergeResult,
    SilverResult,
    TConfigDict,
    TDataFrameRegistryDict,
    TLayer,
    TLoadMode,
    TModelRegistryDict,
    TSCDType,
    TSparkModelClass,
    TTableNameStrategy,
)

__all__ = [
    # Client protocols
    "DataFetcherProtocol",
    "DataValidatorProtocol",
    "ValidationResult",
    # Plugin protocol
    "IntegrationPluginProtocol",
    "Processors",
    # Processor protocols
    "BronzeProcessorProtocol",
    "SilverProcessorProtocol",
    "GoldProcessorProtocol",
    # Registry protocol
    "ModelRegistryProtocol",
    # Types and results
    "BronzeResult",
    "SilverResult",
    "GoldResult",
    "MergeResult",
    "TConfigDict",
    "TDataFrameRegistryDict",
    "TLayer",
    "TLoadMode",
    "TModelRegistryDict",
    "TSCDType",
    "TSparkModelClass",
    "TTableNameStrategy",
]
