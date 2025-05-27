"""
Fact table creation functions for the gold layer.
Now using generic fact creator instead of individual functions.
"""

from unified_etl_core.gold.facts.utils import (
    get_fact_table_name,
    process_fact_table,
    process_gold_layer,
)
from unified_etl_core.gold.generic_fact import create_fact_table, create_generic_fact_tables

__all__ = [
    "create_fact_table",
    "create_generic_fact_tables",
    "get_fact_table_name",
    "process_fact_table",
    "process_gold_layer",
]
