from __future__ import annotations

from fabric_api.models import ManageProduct



"""fabric_api.extract.products

Product extraction helpers.
"""

import logging

from ..client import ConnectWiseClient
from ..models import ManageProduct, ManageInvoiceError
from ..utils import get_parent_agreement_data
from ._common import safe_validate

__all__ = ["get_product_with_relations"]

_LOGGER = logging.getLogger(__name__)


def get_product_with_relations(
    client: ConnectWiseClient,
    product_id: int,
    invoice_number: str,
) -> tuple[ManageProduct | None, list[ManageInvoiceError]]:
    errors: list[ManageInvoiceError] = []
    raw = client.get(endpoint=f"/procurement/products/{product_id}").json()
    model: ManageProduct | None = safe_validate(model_cls=ManageProduct, raw=raw, errors=errors, invoice_number=invoice_number)

    if model and model.agreement_id:
        parent_id, agr_type = get_parent_agreement_data(client, model.agreement_id)
        model.parent_agreement_id = parent_id  # type: ignore[attr-defined]
        model.agreement_type = agr_type  # type: ignore[attr-defined]

    return model, errors
