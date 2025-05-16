#!/usr/bin/env python
"""
Storage utilities optimized for Microsoft Fabric.
"""

from .fabric_delta import add_metadata_columns, dataframe_from_models, write_errors, write_to_delta

__all__ = ["add_metadata_columns", "dataframe_from_models", "write_errors", "write_to_delta"]
