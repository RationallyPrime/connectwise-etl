from pydantic import BaseModel
from typing import Type

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
    field_names = list(model_class.model_fields.keys())
    # We might need to exclude '_info' or other metadata fields if CW doesn't recognize them
    # in the 'fields' parameter, or if they are always returned.
    # Example: field_names = [f for f in field_names if f != '_info']
    return ",".join(field_names)