from __future__ import annotations

"""fabric_api.extract.products

Product extraction helpers.
"""

import logging
from typing import Any

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
            model_cls=ManageProduct, 
            raw=raw, 
            errors=errors, 
            invoice_number=invoice_number
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
    """Get products for a specific invoice using the direct relationship endpoint."""
    logger: logging.Logger = logging.getLogger(name=__name__)
    logger.debug(f"Getting products for invoice {invoice_number} (ID: {invoice_id})")
    
    products: list[ManageProduct] = []
    errors: list[ManageInvoiceError] = []
    
    # Use the direct relationship endpoint for products
    endpoint = f"/finance/invoices/{invoice_id}/products"
    
    try:
        # Get products directly related to this invoice
        products_raw: list[dict[str, Any]] = client.paginate(
            endpoint=endpoint,
            entity_name=f"products for invoice {invoice_number}",
            max_pages=max_pages,
        )
        
        logger.debug(f"Found {len(products_raw)} products for invoice {invoice_number}")
        
        # Validate and convert to Pydantic models
        for product_raw in products_raw:
            # Add invoice number for reference
            product_raw["invoiceNumber"] = invoice_number
            
            # Validate against our model
            product = safe_validate(
                ManageProduct, 
                product_raw, 
                errors=errors, 
                invoice_number=invoice_number
            )
            
            if product:
                products.append(product)
    
    except Exception as e:
        logger.error(f"Error fetching products for invoice {invoice_number}: {str(e)}")
        errors.append(
            ManageInvoiceError(
                invoice_number=invoice_number,
                error_table_id="0",
                error_type="ProductExtractionError",
                error_message=str(e),
                table_name="ManageProduct",
            )
        )
    
    logger.info(f"Retrieved {len(products)} products for invoice {invoice_number}")
    return products, errors
