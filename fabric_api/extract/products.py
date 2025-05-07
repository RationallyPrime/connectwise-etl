from __future__ import annotations

from fabric_api import schemas
from fabric_api.api_utils import get_fields_for_api_call

"""fabric_api.extract.products

Product extraction helpers.
"""

import logging
from typing import Any, Dict, List

from ..client import ConnectWiseClient
from ..models import ManageProduct, ManageInvoiceError
from ..utils import get_parent_agreement_data
from ._common import safe_validate

__all__ = ["get_product_with_relations", "get_products_for_invoice"]

_LOGGER = logging.getLogger(__name__)


def get_product_with_relations(
    client: ConnectWiseClient,
    product_id: int,
    invoice_number: str,
) -> tuple[ManageProduct | None, list[ManageInvoiceError]]:
    """Get product with agreement relations for a specific product ID."""
    errors: list[ManageInvoiceError] = []

    try:
        raw = client.get(endpoint=f"/procurement/products/{product_id}").json()
        model: ManageProduct | None = safe_validate(
            model_cls=ManageProduct, raw=raw, errors=errors, invoice_number=invoice_number
        )

        # Set invoice_number if not already present
        if model and hasattr(model, "invoice_number") and not getattr(model, "invoice_number"):
            setattr(model, "invoice_number", invoice_number)

        # Enhance with agreement data if available
        if model and model.agreement_id:
            parent_id, agr_type = get_parent_agreement_data(client, model.agreement_id)
            model.parent_agreement_id = parent_id  # type: ignore[attr-defined]
            model.agreement_type = agr_type  # type: ignore[attr-defined]

        return model, errors
    except Exception as e:
        _LOGGER.warning(f"Error retrieving product {product_id}: {str(e)}")
        errors.append(
            ManageInvoiceError(
                invoice_number=invoice_number,
                error_table_id="ProductAccess",
                error_message=f"Cannot access product {product_id}: {str(e)}",
                table_name="ManageProduct",
            )
        )
        return None, errors


def get_products_for_invoice(
    client: ConnectWiseClient,
    invoice_id: int,
    invoice_number: str,
    *,
    max_pages: int | None = 50,
) -> tuple[list[ManageProduct], list[ManageInvoiceError]]:
    """Get products for an invoice using filtered queries."""
    logger: logging.Logger = logging.getLogger(name=__name__)
    logger.debug(f"Getting products for invoice {invoice_number} (ID: {invoice_id})")

    products: list[ManageProduct] = []
    errors: list[ManageInvoiceError] = []

    try:
        # Use the standard products endpoint with a filter for this invoice
        conditions = f"invoice/id={invoice_id}"

        # Get products for this invoice
        entries = client.paginate(
            endpoint="/procurement/products",
            entity_name=f"products for invoice {invoice_id}",
            params={"conditions": conditions},
            max_pages=max_pages,
        )

        # Transform the entries into ManageProduct objects
        for entry in entries:
            try:
                # Map 'id' to 'product_id' which is required by our model
                if "id" in entry:
                    entry["product_id"] = entry["id"]

                # Add the invoice_number to the entry
                entry["invoice_number"] = invoice_number

                # Validate and create a ManageProduct object
                product = safe_validate(
                    model_cls=ManageProduct, raw=entry, errors=errors, invoice_number=invoice_number
                )

                if product:
                    # Ensure the invoice_number is set on the model
                    product.invoice_number = invoice_number
                    products.append(product)

            except Exception as e:
                logger.error(f"Error processing product: {str(e)}")
                errors.append(
                    ManageInvoiceError(
                        error_message=f"Error processing product: {str(e)}",
                        invoice_number=invoice_number,
                        error_table_id=ManageProduct.__name__,
                        table_name=ManageProduct.__name__,
                    )
                )

        logger.info(f"Retrieved {len(products)} products for invoice {invoice_number}")

    except Exception as e:
        error_msg = f"Error fetching products for invoice {invoice_number}: {str(e)}"
        logger.error(error_msg)
        errors.append(
            ManageInvoiceError(
                invoice_number=invoice_number,
                error_table_id="0",
                error_type="ProductExtractionError",
                error_message=error_msg,
                table_name="ManageProduct",
            )
        )

    return products, errors


def fetch_product_items_raw(  # Renamed to ProductItem to match schema model
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Fetch raw product item data from ConnectWise API using schema-based field selection.
    ConnectWise often refers to these as "products" in endpoints like /procurement/products,
    but individual items within (e.g. on a sales order or invoice) might be better
    represented by a "ProductItem" schema.
    Targets: /procurement/products (commonly used, or could be others like /sales/orders/{id}/details)
    Validates against: schemas.ProductItem
    """
    _LOGGER.info("Fetching raw product items using schema-based field selection")

    fields_str = (
        fields_override if fields_override else get_fields_for_api_call(schemas.ProductItem)
    )
    _LOGGER.debug(f"Using fields for product items: {fields_str}")

    # NOTE: The endpoint for "general products/catalog items" might be different from
    # "products on an invoice/order". Adjust endpoint as needed.
    # Using /procurement/products as a common example for general product listing.
    raw_product_items = client.paginate(
        endpoint="/procurement/products",  # Or e.g., /sales/orders/{id}/details if fetching line items
        entity_name="product items",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    _LOGGER.info(f"Successfully fetched {len(raw_product_items)} raw product items")
    return raw_product_items
