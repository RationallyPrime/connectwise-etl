import re
import inspect
from typing import Type, Dict, Any, List, Optional, Set

from pydantic import BaseModel, Field


def get_fields_for_api_call(model_class: Type[BaseModel], max_depth: int = 1, include_all_nested: bool = False) -> str:
    """
    Generates a ConnectWise API 'fields' string from a Pydantic model.
    Handles basic nesting up to max_depth if ConnectWise API supports it
    (e.g., 'company/name').

    Args:
        model_class: The Pydantic model class to inspect
        max_depth: Maximum depth for nested fields (1 = top level + one level of nesting)
        include_all_nested: If True, includes all nested fields up to max_depth.
                           If False, includes only id and name for nested objects.

    Returns:
        Comma-separated list of fields formatted for the ConnectWise API
    """
    field_list = _get_field_list(model_class, max_depth, '', set(), include_all_nested)
    return ",".join(field_list)


def _get_field_list(
    model_class: Type[BaseModel],
    max_depth: int,
    parent_path: str,
    visited: Set[Type[BaseModel]],
    include_all_nested: bool
) -> List[str]:
    """
    Recursively extracts field names from a Pydantic model.

    Args:
        model_class: The Pydantic model class to inspect
        max_depth: Maximum recursion depth
        parent_path: Path to the current field
        visited: Set of already visited model classes to prevent infinite recursion
        include_all_nested: If True, includes all nested fields up to max_depth

    Returns:
        List of field names formatted for ConnectWise API
    """
    if max_depth <= 0 or model_class in visited:
        return []

    # Add model to visited set to prevent infinite recursion
    visited.add(model_class)

    # Get direct field names from the model
    field_list: List[str] = []

    # Get model fields
    model_fields = getattr(model_class, "model_fields", {})

    for field_name, field_info in model_fields.items():
        # Skip _info fields
        if field_name == "_info" or field_name == "field_info":
            continue

        # Check if the field should be aliased
        field_alias = None
        if hasattr(field_info, "alias") and field_info.alias:
            field_alias = field_info.alias

        # Convert field name to camelCase
        api_field_name = _to_camel_case(field_name)

        # Use alias if provided
        if field_alias:
            api_field_name = field_alias

        # Build the full path for this field
        full_path = f"{parent_path}/{api_field_name}" if parent_path else api_field_name
        field_list.append(full_path)

        # If this is a nested model and we haven't reached max depth
        # Check if the field's type annotation is a Pydantic model
        field_type = field_info.annotation
        origin = getattr(field_type, "__origin__", None)

        # Handle nested models
        if inspect.isclass(field_type) and issubclass(field_type, BaseModel) and max_depth > 1:
            # For nested objects, we usually want at least id and name
            if include_all_nested:
                # Include all nested fields
                nested_fields = _get_field_list(
                    field_type,
                    max_depth - 1,
                    full_path,
                    visited.copy(),
                    include_all_nested
                )
                field_list.extend(nested_fields)
            else:
                # Include only id and name for nested objects
                for subfield in ["id", "name", "identifier"]:
                    if subfield in getattr(field_type, "model_fields", {}):
                        field_list.append(f"{full_path}/{subfield}")

        # Handle list of models
        elif origin is list and hasattr(field_type, "__args__") and field_type.__args__:
            item_type = field_type.__args__[0]
            if inspect.isclass(item_type) and issubclass(item_type, BaseModel) and max_depth > 1:
                # We don't typically need to expand lists in the field selection
                # But we can mark it for potential array handling
                pass

    return field_list


def _to_camel_case(snake_str: str) -> str:
    """
    Convert a snake_case string to camelCase.

    Args:
        snake_str: The snake_case string to convert

    Returns:
        The string converted to camelCase
    """
    # Handle special cases (API specific mappings)
    special_cases = {
        "field_info": "_info",  # Special case for our field_info that maps to _info in the API
    }

    if snake_str in special_cases:
        return special_cases[snake_str]

    # Standard conversion
    if "_" not in snake_str:
        return snake_str

    # Convert snake_case to camelCase
    return re.sub(r'_([a-z])', lambda x: x.group(1).upper(), snake_str)
