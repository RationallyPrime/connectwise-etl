"""
Unified ETL framework for JSON-based sources to Microsoft Fabric OneLake.

This package provides a configurable, extensible ETL framework built around:
- Automated model generation from JSON schemas
- Pydantic validation and SparkDantic integration
- Medallion architecture (Bronze → Silver → Gold)
- Microsoft Fabric OneLake Delta table operations

Supports data sources like ConnectWise PSA, Business Central, Jira, and any JSON-based APIs.
"""

__version__ = "0.1.0"

from unified_etl.main import run_etl

__all__ = ["run_etl"]
