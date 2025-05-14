import inspect
import re
from typing import Any

from pydantic import BaseModel


def get_fields_for_api_call(model_class: type[BaseModel], max_depth: int = 1, include_all_nested: bool = False) -> str:
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
    return ",".join(sorted(field_list))  # Sort for consistency and cache-friendliness


def _get_field_list(
    model_class: type[BaseModel],
    max_depth: int,
    parent_path: str,
    visited: set[type[BaseModel]],
    include_all_nested: bool
) -> list[str]:
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
    field_list: list[str] = []

    # Get model fields
    model_fields = getattr(model_class, "model_fields", {})

    for field_name, field_info in model_fields.items():
        # Skip excluded fields
        if field_name == "_info" or field_name == "field_info" or field_name.startswith("_"):
            continue

        # Check if the field should be aliased
        field_alias = None
        if hasattr(field_info, "alias") and field_info.alias:
            field_alias = field_info.alias

        # Convert field name to camelCase for the API
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
        getattr(field_type, "__origin__", None)

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


def build_condition_string(**conditions: Any) -> str:
    """
    Builds a condition string for ConnectWise API calls from keyword arguments.

    For example:
    build_condition_string(
        id=123,
        date_entered_gt="2023-01-01",
        status_id_in=[1, 2, 3]
    )

    Will produce:
    "id=123 AND dateEntered>[2023-01-01] AND status/id in (1,2,3)"

    Supports operators:
    - _eq: =
    - _gt: >
    - _gte: >=
    - _lt: <
    - _lte: <=
    - _ne: !=
    - _contains: contains
    - _like: like
    - _in: in
    - _not_in: not in

    Args:
        **conditions: Keyword arguments for conditions

    Returns:
        Formatted condition string for ConnectWise API
    """
    if not conditions:
        return ""

    condition_parts = []

    for key, value in conditions.items():
        # Skip None values
        if value is None:
            continue

        # Check for operators in the key
        operator_map = {
            "_eq": "=",
            "_gt": ">",
            "_gte": ">=",
            "_lt": "<",
            "_lte": "<=",
            "_ne": "!=",
            "_contains": "contains",
            "_like": "like",
            "_in": "in",
            "_not_in": "not in"
        }

        operator = "="  # Default operator
        field_name = key

        # Check if key contains an operator
        for op_suffix, op_symbol in operator_map.items():
            if key.endswith(op_suffix):
                operator = op_symbol
                field_name = key[:-len(op_suffix)]
                break

        # Convert field name to camelCase
        field_name = _to_camel_case(field_name)

        # Format value based on type
        if isinstance(value, str):
            # Add brackets for date values
            if (operator in [">", ">=", "<", "<="] and
                (re.match(r'^\d{4}-\d{2}-\d{2}', value) or
                 re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value))):
                formatted_value = f"[{value}]"
            else:
                formatted_value = f'"{value}"'
        elif isinstance(value, bool):
            formatted_value = str(value).lower()
        elif isinstance(value, list | tuple):
            # Format lists for IN operators
            if operator in ["in", "not in"]:
                values_str = ','.join(str(v) if isinstance(v, int | float) else f'"{v}"' for v in value)
                formatted_value = f"({values_str})"
            else:
                # Default list representation
                formatted_value = str(value)
        else:
            formatted_value = str(value)

        # Handle field paths (e.g., "company/id")
        if "/" in field_name:
            condition = f"{field_name} {operator} {formatted_value}"
        else:
            condition = f"{field_name}{operator}{formatted_value}"

        condition_parts.append(condition)

    return " AND ".join(condition_parts)
