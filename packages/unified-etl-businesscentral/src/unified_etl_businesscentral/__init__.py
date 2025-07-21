"""
Business Central ETL package - specialized business logic for BC systems.

Contains BC-specific transformations that were moved from unified-etl-core:
- Gold layer dimensional modeling (dimension bridges, item attributes)
- Silver layer global dimension resolution
- Account hierarchy building for BC chart of accounts
- Fact table creation (purchase, agreement)
"""

from .config import BC_FACT_CONFIGS, SILVER_CONFIG
from .transforms.facts import create_agreement_fact, create_purchase_fact
from .transforms.gold_utils import (
    build_bc_account_hierarchy,
    create_bc_dimension_bridge,
    create_bc_item_attribute_bridge,
    create_bc_item_attribute_dimension,
    join_bc_dimension,
)

# Integration interface for core framework
extractor = None  # BC doesn't need extractor - data comes from BC2ADLS

# Import models for framework integration
from .models.models import (
    CompanyInformation,
    Currency,
    Customer,
    Dimension,
    DimensionSetEntry,
    DimensionValue,
    GLAccount,
    GLEntry,
    Item,
    Vendor,
)

models = {
    "customer": Customer,
    "vendor": Vendor,
    "item": Item,
    "glaccount": GLAccount,
    "glentry": GLEntry,
    "currency": Currency,
    "companyinformation": CompanyInformation,
    "dimension": Dimension,
    "dimensionvalue": DimensionValue,
    "dimensionsetentry": DimensionSetEntry,
}

# Export entity configurations for framework integration
def get_entity_configs():
    """Get Business Central entity configurations for framework integration."""
    from unified_etl_core.config import EntityConfig
    
    entity_configs = {}
    
    for entity_name, config in SILVER_CONFIG.items():
        # Convert to lowercase for framework compatibility
        key = entity_name.lower()
        
        entity_configs[key] = EntityConfig(
            name=key,
            source="businesscentral",
            model_class_name=config["model"],
            flatten_nested=True,
            flatten_max_depth=3,
            preserve_columns=[],
            json_columns=[],
            column_mappings={},  # BC SILVER_CONFIG format is different - skip for now
            scd=None,
            business_keys=config.get("business_keys", ["No", "$Company"]),
            add_audit_columns=True,
            strip_null_columns=True
        )
    
    return entity_configs

# Provide entity configs to framework
entity_configs = get_entity_configs()

__all__ = [
    "BC_FACT_CONFIGS",
    "SILVER_CONFIG",
    "build_bc_account_hierarchy",
    "create_agreement_fact",
    "create_bc_dimension_bridge",
    "create_bc_item_attribute_bridge",
    "create_bc_item_attribute_dimension",
    "create_purchase_fact",
    "extractor",
    "join_bc_dimension",
    "models",
    "entity_configs",
]
