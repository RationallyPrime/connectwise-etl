#!/usr/bin/env python
"""
Core utilities for Microsoft Fabric integration.
"""

from .api_utils import build_condition_string, get_fields_for_api_call
from .config import DELTA_WRITE_OPTIONS, ENTITY_CONFIG, get_entity_config
from .log_utils import (
    ETLLogger,
    api_call,
    configure_logging,
    critical,
    debug,
    error,
    etl_logger,
    etl_progress,
    info,
    validation,
    warning,
)
from .models import (
    ENTITY_TYPES,
    REFERENCE_MODELS,
    generate_models,
    load_schema,
)
from .path_utils import get_table_path
from .spark_utils import (
    create_empty_table_if_not_exists,
    get_spark_session,
    read_table_safely,
    table_exists,
)
from .utils import create_batch_identifier, get_first_day_next_month, get_nested_value

__all__ = [
    "DELTA_WRITE_OPTIONS",
    "ENTITY_CONFIG",
    "ENTITY_TYPES",
    "REFERENCE_MODELS",
    "ETLLogger",
    "api_call",
    "build_condition_string",
    "configure_logging",
    "create_batch_identifier",
    "create_empty_table_if_not_exists",
    "critical",
    "debug",
    "error",
    # Logging utilities
    "etl_logger",
    "etl_progress",
    # Model generator utilities
    "generate_models",
    # Configuration
    "get_entity_config",
    # API utilities
    "get_fields_for_api_call",
    "get_first_day_next_month",
    # General utilities
    "get_nested_value",
    # Spark utilities
    "get_spark_session",
    # Path utilities
    "get_table_path",
    "info",
    "load_schema",
    "read_table_safely",
    "table_exists",
    "validation",
    "warning"
]
