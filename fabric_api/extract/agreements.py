from __future__ import annotations

"""fabric_api.extract.agreements - Agreement-centric extraction routines."""

import logging
from typing import Any

from ..client import ConnectWiseClient
from ..connectwise_models import Agreement
from ..core.api_utils import get_fields_for_api_call

logger = logging.getLogger(__name__)

def fetch_agreements_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch raw agreement data from ConnectWise API using schema-based field selection.
    Targets: /finance/agreements
    Validates against: Agreement model
    """
    logger.info("Fetching raw agreements using schema-based field selection")

    fields_str = fields_override if fields_override else get_fields_for_api_call(Agreement, max_depth=2)
    logger.debug(f"Using fields for agreements: {fields_str}")

    raw_agreements = client.paginate(
        endpoint="/finance/agreements",
        entity_name="agreements",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_agreements)} raw agreements")
    return raw_agreements


def fetch_agreement_by_id(
    client: ConnectWiseClient,
    agreement_id: int,
    fields_override: str | None = None,
) -> dict[str, Any]:
    """
    Fetch a single agreement by ID.
    Targets: /finance/agreements/{id}
    Validates against: Agreement model

    Args:
        client: ConnectWiseClient instance
        agreement_id: ID of the agreement to fetch
        fields_override: Optional field string to override default fields

    Returns:
        Agreement data as a dictionary
    """
    logger.info(f"Fetching agreement ID {agreement_id}")

    fields_str = fields_override if fields_override else get_fields_for_api_call(Agreement, max_depth=2)

    response = client.get(
        endpoint=f"/finance/agreements/{agreement_id}",
        params={"fields": fields_str} if fields_str else None
    )

    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to fetch agreement ID {agreement_id}: {response.status_code}")
        response.raise_for_status()
        return {}  # Will not reach here, but needed for type checking


def fetch_active_agreements(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    additional_conditions: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch only active agreements.

    Args:
        client: ConnectWiseClient instance
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        additional_conditions: Optional additional conditions to apply

    Returns:
        List of active agreements
    """
    # Create base condition for active agreements
    conditions = "activeFlag=true"

    # Add any additional conditions
    if additional_conditions:
        conditions = f"{conditions} AND {additional_conditions}"

    return fetch_agreements_raw(
        client=client,
        page_size=page_size,
        max_pages=max_pages,
        conditions=conditions
    )
