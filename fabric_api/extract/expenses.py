from __future__ import annotations

"""fabric_api.extract.expenses - Expense entry extraction routines."""

import logging
from typing import Any

from ..client import ConnectWiseClient
from ..connectwise_models import ExpenseEntry
from ..core.api_utils import build_condition_string, get_fields_for_api_call

logger = logging.getLogger(__name__)

def fetch_expense_entries_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch raw expense entry data from ConnectWise API using schema-based field selection.
    Targets: /expense/entries
    Validates against: ExpenseEntry model
    """
    logger.info("Fetching raw expense entries using schema-based field selection")

    fields_str = fields_override if fields_override else get_fields_for_api_call(ExpenseEntry, max_depth=2)
    logger.debug(f"Using fields for expense entries: {fields_str}")

    raw_expense_entries = client.paginate(
        endpoint="/expense/entries",
        entity_name="expense entries",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_expense_entries)} raw expense entries")
    return raw_expense_entries


def fetch_expense_entries_by_date_range(
    client: ConnectWiseClient,
    start_date: str,
    end_date: str,
    page_size: int = 100,
    max_pages: int | None = 50,
) -> list[dict[str, Any]]:
    """
    Fetch expense entries within a date range.

    Args:
        client: ConnectWiseClient instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch

    Returns:
        List of expense entries
    """
    # Build date range condition
    condition = build_condition_string(
        date_gte=start_date,
        date_lt=end_date
    )

    return fetch_expense_entries_raw(
        client=client,
        conditions=condition,
        page_size=page_size,
        max_pages=max_pages
    )


def fetch_expense_entries_by_agreement(
    client: ConnectWiseClient,
    agreement_id: int,
    page_size: int = 100,
    max_pages: int | None = 50,
) -> list[dict[str, Any]]:
    """
    Fetch expense entries for a specific agreement.

    Args:
        client: ConnectWiseClient instance
        agreement_id: Agreement ID
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch

    Returns:
        List of expense entries
    """
    condition = f"agreement/id={agreement_id}"

    return fetch_expense_entries_raw(
        client=client,
        conditions=condition,
        page_size=page_size,
        max_pages=max_pages
    )
