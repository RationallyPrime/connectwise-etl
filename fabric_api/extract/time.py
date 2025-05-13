from __future__ import annotations

"""fabric_api.extract.time - Time entry extraction routines."""

from typing import Any, Dict, List, Optional
import logging

from ..client import ConnectWiseClient
from ..connectwise_models import TimeEntry
from ..api_utils import get_fields_for_api_call, build_condition_string

logger = logging.getLogger(__name__)

def fetch_time_entries_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int | None = 50,
    conditions: str | None = None,
    child_conditions: str | None = None,
    order_by: str | None = None,
    fields_override: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Fetch raw time entry data from ConnectWise API using schema-based field selection.
    Targets: /time/entries
    Validates against: TimeEntry model
    """
    logger.info("Fetching raw time entries using schema-based field selection")

    fields_str = fields_override if fields_override else get_fields_for_api_call(TimeEntry, max_depth=2)
    logger.debug(f"Using fields for time entries: {fields_str}")

    raw_time_entries = client.paginate(
        endpoint="/time/entries",
        entity_name="time entries",
        fields=fields_str,
        conditions=conditions,
        child_conditions=child_conditions,
        order_by=order_by,
        page_size=page_size,
        max_pages=max_pages,
    )

    logger.info(f"Successfully fetched {len(raw_time_entries)} raw time entries")
    return raw_time_entries


def fetch_time_entries_by_date_range(
    client: ConnectWiseClient,
    start_date: str,
    end_date: str,
    page_size: int = 100,
    max_pages: int | None = 50,
) -> List[Dict[str, Any]]:
    """
    Fetch time entries within a date range.
    
    Args:
        client: ConnectWiseClient instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        
    Returns:
        List of time entries
    """
    # Build date range condition
    condition = build_condition_string(
        date_gte=start_date,
        date_lt=end_date
    )
    
    return fetch_time_entries_raw(
        client=client,
        conditions=condition,
        page_size=page_size,
        max_pages=max_pages
    )


def fetch_billable_time_entries(
    client: ConnectWiseClient,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    page_size: int = 100,
    max_pages: int | None = 50,
) -> List[Dict[str, Any]]:
    """
    Fetch only billable time entries, optionally within a date range.
    
    Args:
        client: ConnectWiseClient instance
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
        page_size: Number of records per page
        max_pages: Maximum number of pages to fetch
        
    Returns:
        List of billable time entries
    """
    # Start with billable condition
    conditions = "billableOption=Billable"
    
    # Add date range if provided
    if start_date and end_date:
        date_condition = build_condition_string(
            date_gte=start_date,
            date_lt=end_date
        )
        conditions = f"{conditions} AND {date_condition}"
    
    return fetch_time_entries_raw(
        client=client,
        conditions=conditions,
        page_size=page_size,
        max_pages=max_pages
    )