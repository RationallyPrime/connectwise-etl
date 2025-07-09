"""Unified ETL configuration system with typed models and ZERO backwards compatibility."""

from .models import (
    ETLConfig,
    LayerConfig,
    IntegrationConfig,
    SparkConfig,
    TableNamingConvention,
)
from .entity import EntityConfig, ColumnMapping, SCDConfig
from .fact import FactConfig, DimensionMapping
from .dimension import DimensionConfig, DimensionType

__all__ = [
    "ETLConfig",
    "LayerConfig",
    "IntegrationConfig",
    "SparkConfig",
    "TableNamingConvention",
    "EntityConfig",
    "ColumnMapping",
    "SCDConfig",
    "FactConfig",
    "DimensionMapping",
    "DimensionConfig",
    "DimensionType",
]