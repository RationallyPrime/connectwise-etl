from __future__ import annotations

"""fabric_api.extract

Extraction layer ‑ converts raw JSON from ConnectWise into strongly‑typed
Pydantic domain objects that mirror the original AL tables.

Public API
----------
* ``get_unposted_invoices_with_details`` – pulls invoices + every related
  entity and returns ``tuple[lists]`` of models.
* ``get_time_entry_with_relations`` – single time‑entry + look‑ups.
* ``get_agreement_with_relations`` – fetch agreement incl. parent data.
* ``get_expense_entries_with_relations`` – expenses for an invoice.
* ``get_product_with_relations`` – single product with discount + parent.

All helpers are stateless and side‑effect‑free; they leverage ``fabric_api
.utils`` for common patterns and the enhanced ``ConnectWiseClient`` for
request/response plumbing.
"""

from .products import get_product_with_relations
from .time import get_time_entry_with_relations
from .agreements import get_agreement_with_relations
from .expenses import get_expense_entries_with_relations
from .invoices import get_unposted_invoices_with_details

__all__ = [
    "get_unposted_invoices_with_details",
    "get_time_entry_with_relations",
    "get_agreement_with_relations",
    "get_expense_entries_with_relations",
    "get_product_with_relations",
]
