from __future__ import annotations

"""fabric_api.extract.invoices - Invoice-centric extraction routines."""

import logging
from typing import Any

from ..client import ConnectWiseClient
from ..connectwise_models import PostedInvoice, UnpostedInvoice
from ..core.api_utils import build_condition_string, get_fields_for_api_call

logger = logging.getLogger(__name__)


def fetch_posted_invoices_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch raw posted invoice data from ConnectWise API using schema-based field selection.
    Targets: /finance/invoices
    Validates against: PostedInvoice model
    """
    logger.info("Fetching raw posted invoices using schema-based field selection")

    fields_str = fields_override if fields_override else get_fields_for_api_call(PostedInvoice, max_depth=2)
    logger.debug(f"Using fields for posted invoices: {fields_str}")

    raw_posted_invoices = client.paginate(
        endpoint="/finance/invoices",
        entity_name="posted invoices",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_posted_invoices)} raw posted invoices")
    return raw_posted_invoices


def fetch_unposted_invoices_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch raw unposted invoice data from ConnectWise API using schema-based field selection.
    Targets: /finance/accounting/unpostedinvoices
    Validates against: UnpostedInvoice model
    """
    logger.info("Fetching raw unposted invoices using schema-based field selection")

    fields_str = fields_override if fields_override else get_fields_for_api_call(UnpostedInvoice, max_depth=2)
    logger.debug(f"Using fields for unposted invoices: {fields_str}")

    raw_unposted_invoices = client.paginate(
        endpoint="/finance/accounting/unpostedinvoices",
        entity_name="unposted invoices",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_unposted_invoices)} raw unposted invoices")
    return raw_unposted_invoices


def fetch_invoices_by_date_range(
    client: ConnectWiseClient,
    start_date: str,
    end_date: str,
    page_size: int = 100,
    max_pages: int | None = 50,
) -> dict[str, list[dict[str, Any]]]:
    """
    Fetch both posted and unposted invoices within a date range.

    Args:
        client: ConnectWiseClient instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch

    Returns:
        Dictionary with both types of invoices
    """
    # Build date range condition
    date_condition = build_condition_string(
        date_gte=start_date,
        date_lt=end_date
    )

    # Fetch posted invoices
    posted = fetch_posted_invoices_raw(
        client=client,
        conditions=date_condition,
        page_size=page_size,
        max_pages=max_pages
    )

    # Fetch unposted invoices
    unposted = fetch_unposted_invoices_raw(
        client=client,
        conditions=date_condition,
        page_size=page_size,
        max_pages=max_pages
    )

    return {
        "posted_invoices": posted,
        "unposted_invoices": unposted
    }
