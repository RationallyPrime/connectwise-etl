"""
Business Central ETL package - specialized business logic for BC systems.

Contains BC-specific transformations that were moved from unified-etl-core:
- Gold layer dimensional modeling (dimension bridges, item attributes)
- Silver layer global dimension resolution
- Account hierarchy building for BC chart of accounts
"""

from .transforms.gold import (
    build_bc_account_hierarchy,
    create_bc_dimension_bridge,
    create_bc_item_attribute_bridge,
    create_bc_item_attribute_dimension,
    join_bc_dimension,
)

__all__ = [
    "build_bc_account_hierarchy",
    "create_bc_dimension_bridge",
    "create_bc_item_attribute_bridge",
    "create_bc_item_attribute_dimension",
    "join_bc_dimension",
]
