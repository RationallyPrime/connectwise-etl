from __future__ import annotations

"""fabric_api.extract.agreements

Agreement‑level helpers.
"""

import logging
from typing import Any, List, Dict, Optional

from ..client import ConnectWiseClient
from ..models import ManageInvoiceError, ManageAgreement
from ..utils import get_parent_agreement_data
from ._common import safe_validate
from ..api_utils import get_fields_for_api_call
from .. import schemas

__all__ = [
    "get_agreement_with_relations",
    "get_all_agreements",
    "get_cached_agreement",
    "fetch_agreements_raw",
]

_LOGGER = logging.getLogger(__name__)

# Global cache for agreements to avoid redundant API calls
_agreement_cache = {}


def get_agreement_with_relations(
    client: ConnectWiseClient,
    agreement_id: int,
) -> tuple[dict[str, Any] | None, list[ManageInvoiceError]]:
    """Raw agreement JSON + error list so callers can decide what to do."""
    errors: list[ManageInvoiceError] = []

    try:
        # First, try the direct approach (if permissions available)
        _LOGGER.info(f"Attempting to fetch agreement {agreement_id} using direct endpoint")
        try:
            agr = client.get(f"/finance/agreements/{agreement_id}").json()
            _LOGGER.info(f"Successfully fetched agreement {agreement_id} using direct endpoint")
        except Exception as direct_exc:
            # If direct access fails, fetch all agreements and find the matching one
            _LOGGER.warning(
                f"Failed direct access for agreement {agreement_id}, fetching all agreements: {str(direct_exc)}"
            )

            # Fetch all agreements
            agr = get_cached_agreement(client, agreement_id)

            if not agr:
                _LOGGER.error(f"No agreement found with ID {agreement_id}")
                errors.append(
                    ManageInvoiceError(
                        invoice_number="",
                        error_table_id="Agreement",
                        table_name="Agreement",
                        error_message=f"No agreement found with ID {agreement_id}",
                    )
                )
                return None, errors

        # Fix the specific validation issues we're seeing in the logs
        if agr:
            # 1. Ensure agreement_id is set (required field)
            if "id" in agr and "agreement_id" not in agr:
                agr["agreement_id"] = agr["id"]

            # 2. Convert type from dict to string if needed
            if isinstance(agr.get("type"), dict) and "name" in agr["type"]:
                agr["type"] = agr["type"]["name"]

            # 3. Handle other nested objects that might be dicts
            for field in ["company", "workRole", "workType"]:
                if isinstance(agr.get(field), dict) and "name" in agr[field]:
                    agr[field] = agr[field]["name"]

        # Validate the fixed agreement data
        validated_agr = safe_validate(model_cls=ManageAgreement, raw=agr, errors=errors)

        if validated_agr:
            # Convert back to dictionary for consistency with existing code
            agr_dict = validated_agr.model_dump()

            # Enrich with parent + type if available
            parent_id, agr_type = get_parent_agreement_data(client, agreement_id)
            agr_dict["parentAgreementId"] = parent_id
            agr_dict["agreementType"] = agr_type

            return agr_dict, errors
        else:
            _LOGGER.error(f"Failed to validate agreement {agreement_id}")
            return None, errors

    except Exception as exc:
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


def get_all_agreements(
    client: ConnectWiseClient, max_pages: int = 50, clear_cache: bool = False
) -> dict[int, dict]:
    """Fetch all agreements at once and return them as a dictionary keyed by agreement_id.

    Args:
        client: The ConnectWise client
        max_pages: Maximum number of pages to fetch
        clear_cache: Whether to clear the existing cache

    Returns:
        Dictionary of agreements keyed by agreement ID
    """
    global _agreement_cache

    # Clear cache if requested
    if clear_cache:
        _agreement_cache.clear()

    # Return cache if it's not empty
    if _agreement_cache:
        _LOGGER.info(f"Using cached agreements ({len(_agreement_cache)} items)")
        return _agreement_cache

    _LOGGER.info("Fetching all agreements")
    agreements = {}

    try:
        # Fetch all agreements
        all_agreements = client.paginate(
            endpoint="/finance/agreements", entity_name="agreements", max_pages=max_pages
        )

        # Process each agreement
        for agreement in all_agreements:
            if "id" not in agreement:
                continue

            agreement_id = agreement["id"]

            # Fix the specific validation issues we're seeing in the logs
            # 1. Ensure agreement_id is set (required field)
            if "id" in agreement and "agreement_id" not in agreement:
                agreement["agreement_id"] = agreement["id"]

            # 2. Convert type from dict to string if needed
            if isinstance(agreement.get("type"), dict) and "name" in agreement["type"]:
                agreement["type"] = agreement["type"]["name"]

            # 3. Handle other nested objects that might be dicts
            for field in ["company", "workRole", "workType"]:
                if isinstance(agreement.get(field), dict) and "name" in agreement[field]:
                    agreement[field] = agreement[field]["name"]

            # Store in dictionary
            agreements[agreement_id] = agreement

        _LOGGER.info(f"Successfully fetched {len(agreements)} agreements")

        # Update cache
        _agreement_cache.update(agreements)

        return agreements

    except Exception as e:
        _LOGGER.error(f"Failed to fetch agreements: {str(e)}")
        return {}


def get_cached_agreement(
    client: ConnectWiseClient, agreement_id: int, refresh_cache: bool = False
) -> dict | None:
    """Get an agreement from the cache or fetch it if needed.

    Args:
        client: The ConnectWise client
        agreement_id: The agreement ID to fetch
        refresh_cache: Whether to refresh the cache

    Returns:
        The agreement data or None if not found
    """
    global _agreement_cache

    # Try to get from cache first
    if not refresh_cache and agreement_id in _agreement_cache:
        return _agreement_cache[agreement_id]

    # Make sure we have agreements loaded
    agreements = get_all_agreements(client, clear_cache=refresh_cache)

    # Return the specific agreement if found
    return agreements.get(agreement_id)


def fetch_agreements_raw(
    client: ConnectWiseClient,
    page_size: int = 100,
    max_pages: int = 50,
    conditions: Optional[str] = None,
    child_conditions: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch raw agreement data from ConnectWise API using schema-based field selection.

    Args:
        client: The ConnectWise client to use for the API call.
        page_size: Number of agreements to fetch per page.
        max_pages: Maximum number of pages to fetch.
        conditions: Optional filter conditions for the API call.
        child_conditions: Optional filter conditions for child objects.

    Returns:
        List of raw agreement dictionaries as returned from the API.
    """
    _LOGGER.info("Fetching raw agreements using schema-based field selection")

    # Use get_fields_for_api_call utility with schemas.Agreement model
    fields_str = get_fields_for_api_call(schemas.Agreement)
    _LOGGER.debug(f"Using fields: {fields_str}")

    # Prepare parameters for the API call
    params = {"pageSize": page_size, "fields": fields_str}

    # Add optional conditions if provided
    if conditions:
        params["conditions"] = conditions
    if child_conditions:
        params["childconditions"] = child_conditions

    # Call client.paginate with the fields and conditions
    raw_agreements = client.paginate(
        endpoint="/finance/agreements", entity_name="agreements", params=params, max_pages=max_pages
    )

    _LOGGER.info(f"Successfully fetched {len(raw_agreements)} raw agreements")
    return raw_agreements
