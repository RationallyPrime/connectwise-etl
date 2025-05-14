#!/usr/bin/env python
"""
Transformation utilities for ConnectWise data.
"""

from .flatten import flatten_all_nested_structures, verify_no_remaining_structs
from .struct_utils import convert_arrays_to_json, explode_array_columns, flatten_dataframe

__all__ = [
    "convert_arrays_to_json",
    "explode_array_columns",
    "flatten_all_nested_structures",
    "flatten_dataframe",
    "verify_no_remaining_structs"
]
