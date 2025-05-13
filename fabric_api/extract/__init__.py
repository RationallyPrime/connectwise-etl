from __future__ import annotations

"""fabric_api.extract

Extraction layer for fetching raw data from the ConnectWise API.
Each module provides functions to fetch data for different entity types.

Public API
----------
* Raw data fetching functions - return lists of raw JSON dictionaries:
  - fetch_posted_invoices_raw - fetch posted invoices
  - fetch_unposted_invoices_raw - fetch unposted invoices
  - fetch_time_entries_raw - fetch time entries
  - fetch_agreements_raw - fetch agreements
  - fetch_expense_entries_raw - fetch expense entries
  - fetch_product_items_raw - fetch product items

All helpers are stateless and side‑effect‑free; they leverage ``fabric_api.api_utils``
for field selection and filtering, and the enhanced ``ConnectWiseClient`` for
request/response handling.
"""

from .products import fetch_product_items_raw, fetch_products_by_catalog_id, fetch_products_by_type
from .time import fetch_time_entries_raw, fetch_time_entries_by_date_range, fetch_billable_time_entries
from .agreements import fetch_agreements_raw, fetch_active_agreements, fetch_agreement_by_id
from .expenses import fetch_expense_entries_raw
from .invoices import fetch_posted_invoices_raw, fetch_unposted_invoices_raw

__all__ = [
    "fetch_posted_invoices_raw",
    "fetch_unposted_invoices_raw",
    "fetch_time_entries_raw",
    "fetch_time_entries_by_date_range",
    "fetch_billable_time_entries",
    "fetch_agreements_raw",
    "fetch_active_agreements",
    "fetch_agreement_by_id",
    "fetch_expense_entries_raw",
    "fetch_product_items_raw",
    "fetch_products_by_catalog_id",
    "fetch_products_by_type",
]
