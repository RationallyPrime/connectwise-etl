#!/usr/bin/env python
"""
ConnectWise PSA to Microsoft Fabric integration.
This package provides utilities for extracting data from ConnectWise PSA API
and loading it directly into Microsoft Fabric OneLake.
"""

# Import core components
from .client import ConnectWiseClient

# Import from core modules
from .core import (
    DELTA_WRITE_OPTIONS,
    ENTITY_CONFIG,
    build_condition_string,
    # Logging
    configure_logging,
    # Configuration
    get_entity_config,
    # API utilities
    get_fields_for_api_call,
    # Spark utilities
    get_spark_session,
    # Path utilities
    get_table_path,
    read_delta_table,
    table_exists,
)

# Import extract utilities
from .extract.generic import extract_entity

# Import the pipeline
from .pipeline import (
    process_bronze_to_silver,
    process_entity_to_bronze,
    run_daily_pipeline,
    run_full_pipeline,
)

# Import storage utilities
from .storage import dataframe_from_models, write_errors, write_to_delta

# Import transform utilities
from .transform import flatten_all_nested_structures, flatten_dataframe

__all__ = [
    "DELTA_WRITE_OPTIONS",
    "ENTITY_CONFIG",
    # Client
    "ConnectWiseClient",
    "build_condition_string",
    "configure_logging",
    "dataframe_from_models",
    # Extract
    "extract_entity",
    "flatten_all_nested_structures",
    # Transform
    "flatten_dataframe",
    "get_entity_config",
    "get_fields_for_api_call",
    "get_spark_session",
    # Core utilities
    "get_table_path",
    # Pipeline
    "process_entity_to_bronze",
    "process_bronze_to_silver",
    "run_full_pipeline",
    "run_daily_pipeline",
    "read_delta_table",
    "table_exists",
    # Storage
    "write_to_delta",
    "write_errors",
]
