from __future__ import annotations

"""fabric_api.extract._common

Shared helpers used by the extraction sub‑modules.
All utilities are side‑effect‑free so they can be mocked in unit tests.
"""

from typing import Any
from logging import Logger, getLogger

from pydantic import ValidationError

from ..client import ConnectWiseClient
from ..models import ManageInvoiceError

__all__ = [
    "safe_validate",
    "paginate",
]

_LOGGER: Logger = getLogger(name=__name__)

# ---------------------------------------------------------------------------
# Validation wrapper --------------------------------------------------------
# ---------------------------------------------------------------------------

def safe_validate(model_cls, raw: dict[str, Any], *, errors: list[ManageInvoiceError], invoice_number: str | None = None) -> Any | None:
    """Validate *raw* against *model_cls* or capture ``ManageInvoiceError``."""
    try:
        return model_cls.model_validate(raw)
    except ValidationError as exc:  # noqa: BLE001
        _LOGGER.warning("Validation failed for %s – %s", model_cls.__name__, exc)
        errors.append(
            ManageInvoiceError(
                invoice_number=invoice_number or str(raw.get("invoiceNumber", "")),
                error_table_id=model_cls.__name__,
                table_name=model_cls.__name__,
                error_message=str(exc),
            )
        )
        return None

# ---------------------------------------------------------------------------
# Pagination wrapper --------------------------------------------------------
# ---------------------------------------------------------------------------

def paginate(
    client: ConnectWiseClient,
    endpoint: str,
    *,
    entity_name: str,
    params: dict[str, Any] | None = None,
    max_pages: int | None = 50,
) -> list[dict[str, Any]]:
    """Thin wrapper around ``get_entity_data_with_detailed_logging`` with sane defaults."""
    items, _ = client.get_entity_data_with_detailed_logging(
        endpoint, params or {}, entity_name=entity_name, max_pages=max_pages
    )
    return items
