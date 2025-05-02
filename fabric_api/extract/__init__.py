from __future__ import annotations

"""fabric_api.extract

Extraction layer ‑ converts raw JSON from ConnectWise into strongly‑typed
Pydantic domain objects that mirror the original AL tables.

Public API
----------
* ``get_invoices_with_details`` – pulls both posted and unposted invoices + every related
  entity and returns ``tuple[lists]`` of models.
* ``get_time_entries_for_invoice`` – get time entries for a specific invoice.
* ``get_agreement_with_relations`` – fetch agreement incl. parent data.
* ``get_expense_entries_with_relations`` – expenses for an invoice.
* ``get_products_for_invoice`` – get products for a specific invoice.

All helpers are stateless and side‑effect‑free; they leverage ``fabric_api
.utils`` for common patterns and the enhanced ``ConnectWiseClient`` for
request/response plumbing.
"""

from .products import get_products_for_invoice
from .time import get_time_entries_for_invoice
from .agreements import get_agreement_with_relations
from .expenses import get_expense_entries_with_relations
from .invoices import get_invoices_with_details, get_unposted_invoices_with_details

__all__ = [
    "get_invoices_with_details",
    "get_unposted_invoices_with_details",  # Kept for backward compatibility
    "get_time_entries_for_invoice",
    "get_agreement_with_relations",
    "get_expense_entries_with_relations",
    "get_products_for_invoice",
]
