"""
Business Central ETL package - specialized business logic for BC systems.

This package implements a BC-specific ETL pipeline that handles BC2ADLS exported data.
It does NOT use the core framework's generic silver transformations, instead providing:

- Bronze layer: Reads pre-exported BC2ADLS data with Pydantic validation
- Silver layer: BC-specific column operations (drop, rename, calculate, surrogate keys)
- Gold layer: Dimensional modeling (dimension bridges, facts with company-aware joins)
"""

from .config import BC_FACT_CONFIGS, SILVER_CONFIG

# BC-specific processors
from .processor import BusinessCentralBronzeProcessor
from .silver_processor import BusinessCentralSilverProcessor

# BC pipeline orchestration
from .bc_pipeline import run_bc_pipeline

# Gold layer transformations
from .transforms.facts import create_agreement_fact, create_purchase_fact
from .transforms.gold_utils import (
    build_bc_account_hierarchy,
    create_bc_dimension_bridge,
    create_bc_item_attribute_bridge,
    create_bc_item_attribute_dimension,
    join_bc_dimension,
)

# Models remain available for BC-specific logic but NOT wrapped in EntityConfig
# Import all models for type hints and validation
from . import models

__all__ = [
    # Configuration
    "BC_FACT_CONFIGS",
    "SILVER_CONFIG",
    # Pipeline entry point
    "run_bc_pipeline",
    # Processors
    "BusinessCentralBronzeProcessor",
    "BusinessCentralSilverProcessor",
    # Gold transformations
    "build_bc_account_hierarchy",
    "create_agreement_fact",
    "create_bc_dimension_bridge", 
    "create_bc_item_attribute_bridge",
    "create_bc_item_attribute_dimension",
    "create_purchase_fact",
    "join_bc_dimension",
    # Models for type hints
    "models",
]
