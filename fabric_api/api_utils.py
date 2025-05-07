from pydantic import BaseModel
from typing import Type
import re


def get_fields_for_api_call(model_class: Type[BaseModel], max_depth: int = 1) -> str:
    """
    Generates a ConnectWise API 'fields' string from a Pydantic model.
    Handles basic nesting up to max_depth if ConnectWise API supports it
    (e.g., 'company/name'). For most CW fields, it will be top-level.
    We need to be careful about what CW actually supports for field selection.
    Often, it's just top-level fields.
    """
    # Implementation will inspect model_class.__fields__
    # For now, assume top-level fields are sufficient for ConnectWise 'fields' parameter.
    # ConnectWise typically doesn't support deep field selection like GraphQL.
    # We usually fetch IDs of related objects and then fetch those objects separately if needed,
    # or rely on relationship endpoints.
    # So, a simple list of top-level field names might be the most practical.
    # Get field names from the Pydantic model
    field_names = list(model_class.model_fields.keys())
    
    # Convert snake_case to camelCase for API compatibility
    camel_case_fields = []
    for field in field_names:
        # Skip _info or other special fields if needed
        if field == "_info":
            continue
            
        # Convert snake_case to camelCase
        if "_" in field:
            # Use regex to convert snake_case to camelCase
            # This converts catalog_item to catalogItem
            camel_field = re.sub(r'_([a-z])', lambda x: x.group(1).upper(), field)
            camel_case_fields.append(camel_field)
        else:
            camel_case_fields.append(field)
    
    return ",".join(camel_case_fields)
