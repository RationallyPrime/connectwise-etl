from __future__ import annotations

"""fabric_api.extract

Extraction layer for fetching raw data from the ConnectWise API.
Provides a generic extraction function that works for all entity types.

Public API
----------
* extract_entity - Generic function to extract any entity type from ConnectWise API
  - Can return raw data (default) or validated models
  - Supports all standard query parameters (conditions, ordering, pagination)
  - Handles field selection automatically based on Pydantic models
"""

from .generic import extract_entity

__all__ = [
    "extract_entity",
]