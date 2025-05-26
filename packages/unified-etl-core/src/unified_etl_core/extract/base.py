from __future__ import annotations

"""
unified_etl.extract._common

Shared utilities for extraction modules
"""

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError
from unified_etl.api.connectwise_client import ConnectWiseClient

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


def validate_batch(
    data: list[dict[str, Any]], model_class: type[T]
) -> tuple[list[T], list[dict[str, Any]]]:
    """
    Validate a batch of raw data against a Pydantic model.

    Args:
        data: List of dictionaries with raw data
        model_class: Pydantic model class to validate against

    Returns:
        Tuple of (valid_models, validation_errors)
    """
    valid_models: list[T] = []
    validation_errors: list[dict[str, Any]] = []

    for i, item in enumerate(data):
        record_id = item.get("id", f"Unknown-{i}")
        try:
            model = model_class.model_validate(item)
            valid_models.append(model)
        except ValidationError as e:
            logger.warning(f"Validation failed for {model_class.__name__} ID {record_id}")
            validation_errors.append(
                {
                    "entity": model_class.__name__,
                    "raw_data_id": record_id,
                    "errors": e.errors(),
                    "raw_data": item,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

    logger.info(f"Validated {len(valid_models)} of {len(data)} {model_class.__name__} records")
    if validation_errors:
        logger.warning(
            f"Found {len(validation_errors)} validation errors in {model_class.__name__} data"
        )

    return valid_models, validation_errors


def fetch_parallel(
    client: ConnectWiseClient,
    endpoint_formatter: Callable[[Any], str],
    items: list[Any],
    max_workers: int = 5,
    **params: Any,
) -> list[dict[str, Any]]:
    """
    Fetch data from multiple endpoints in parallel using a thread pool.

    Args:
        client: ConnectWiseClient instance
        endpoint_formatter: Function that takes an item and returns an endpoint path
        items: List of items to fetch data for
        max_workers: Maximum number of parallel workers
        **params: Additional parameters to pass to the client.get method

    Returns:
        List of response data dictionaries
    """
    results = []

    def fetch_item(item):
        endpoint = endpoint_formatter(item)
        try:
            response = client.get(endpoint=endpoint, **params)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error fetching {endpoint}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Exception fetching {endpoint}: {e!s}")
            return None

    # Use ThreadPoolExecutor to fetch items in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(fetch_item, item): item for item in items}
        for future in as_completed(future_to_item):
            result = future.result()
            if result:
                results.append(result)

    return results


def format_date_filter(start_date: date, end_date: date) -> str:
    """
    Format a date range filter for ConnectWise API conditions.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (exclusive)

    Returns:
        Formatted condition string
    """
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    return f"date>=[{start_str}] AND date<[{end_str}]"
