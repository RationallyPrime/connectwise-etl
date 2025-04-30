from __future__ import annotations

"""fabric_api.extract.agreements

Agreement‑level helpers.
"""

import logging
from typing import Any

from ..client import ConnectWiseClient
from ..models import ManageInvoiceError
from ..utils import get_parent_agreement_data

__all__ = ["get_agreement_with_relations"]

_LOGGER = logging.getLogger(__name__)


def get_agreement_with_relations(
    client: ConnectWiseClient,
    agreement_id: int,
) -> tuple[dict[str, Any] | None, list[ManageInvoiceError]]:
    """Raw agreement JSON + error list so callers can decide what to do."""
    errors: list[ManageInvoiceError] = []
    try:
        agr = client.get(f"/finance/agreements/{agreement_id}").json()
    except Exception as exc:  # noqa: BLE001
        _LOGGER.error("Failed to fetch agreement %s – %s", agreement_id, exc)
        errors.append(
            ManageInvoiceError(
                invoice_number="",
                error_table_id="Agreement",
                table_name="Agreement",
                error_message=str(exc),
            )
        )
        return None, errors

    # Enrich with parent + type if available
    parent_id, agr_type = get_parent_agreement_data(client, agreement_id)
    agr["parentAgreementId"] = parent_id
    agr["agreementType"] = agr_type
    return agr, errors
