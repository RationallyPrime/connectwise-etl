from __future__ import annotations

"""fabric_api.extract.time

Time entry extraction helpers.
"""

from logging import getLogger, Logger
from typing import Any

from ..client import ConnectWiseClient
from ..models import ManageTimeEntry, ManageInvoiceError
from ..utils import get_parent_agreement_data
from ._common import safe_validate

__all__ = ["get_time_entry_with_relations"]

_LOGGER: Logger = getLogger(name=__name__)


def get_time_entry_with_relations(
    client: ConnectWiseClient,
    time_entry_id: int,
) -> tuple[ManageTimeEntry | None, list[ManageInvoiceError]]:
    """Fetch a time‑entry and enrich it with agreement relations + errors."""
    errors: list[ManageInvoiceError] = []
    raw = client.get(endpoint=f"/time/entries/{time_entry_id}").json()
    model: Any | None = safe_validate(ManageTimeEntry, raw, errors=errors)

    if model and model.agreement_id:
        parent_id, agr_type = get_parent_agreement_data(client, agreement_id=model.agreement_id)
        model.parent_agreement_id = parent_id  # type: ignore[attr-defined]
        model.agreement_type = agr_type  # type: ignore[attr-defined]

    return model, errors