"""Unified ETL configuration system with typed models and ZERO backwards compatibility."""

from .dimension import DimensionConfig, DimensionType
from .entity import ColumnMapping, EntityConfig, SCDConfig
from .fact import DimensionMapping, FactConfig
from .models import (
    ETLConfig,
    IntegrationConfig,
    LayerConfig,
    SparkConfig,
    TableNamingConvention,
)

__all__ = [
    "ColumnMapping",
    "DimensionConfig",
    "DimensionMapping",
    "DimensionType",
    "ETLConfig",
    "EntityConfig",
    "FactConfig",
    "IntegrationConfig",
    "LayerConfig",
    "SCDConfig",
    "SparkConfig",
    "TableNamingConvention",
]
