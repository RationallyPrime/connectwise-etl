"""
Fact table creation functions for the gold layer.
"""

from bc_etl.gold.facts.fact_accounts_payable import create_accounts_payable_fact
from bc_etl.gold.facts.fact_accounts_receivable import create_accounts_receivable_fact
from bc_etl.gold.facts.fact_agreement import create_agreement_fact
from bc_etl.gold.facts.fact_finance import create_finance_fact
from bc_etl.gold.facts.fact_inventory_value import create_inventory_value_fact
from bc_etl.gold.facts.fact_item import create_item_fact
from bc_etl.gold.facts.fact_jobs import create_jobs_fact
from bc_etl.gold.facts.fact_purchase import create_purchase_fact
from bc_etl.gold.facts.fact_sales import create_sales_fact
from bc_etl.gold.facts.utils import (
    get_fact_table_name,
    process_fact_table,
    process_gold_layer,
    resolve_fact_function,
)

__all__ = [
    "create_accounts_payable_fact",
    "create_accounts_receivable_fact",
    "create_agreement_fact",
    "create_finance_fact",
    "create_inventory_value_fact",
    "create_item_fact",
    "create_jobs_fact",
    "create_purchase_fact",
    "create_sales_fact",
    "get_fact_table_name",
    "process_fact_table",
    "process_gold_layer",
    "resolve_fact_function",
]
