"""Dynamic integration detection and loading."""

import importlib
import logging
from typing import Any


def detect_available_integrations() -> dict[str, Any]:
    """Dynamically detect which integration packages are available."""
    integrations = {}

    # Define known integrations
    known_integrations = {
        "connectwise": "unified_etl_connectwise",
        "businesscentral": "unified_etl_businesscentral",
        "jira": "unified_etl_jira",
    }

    for name, module_name in known_integrations.items():
        try:
            module = importlib.import_module(module_name)
            integrations[name] = {
                "module": module,
                "available": True,
                "extractor": getattr(module, "extractor", None),
                "models": getattr(module, "models", None),
            }
            logging.info(f"✅ Integration '{name}' detected and loaded")
        except ImportError:
            logging.info(f"⚠️ Integration '{name}' not available (package not installed)")
            integrations[name] = {"available": False}

    return integrations


def get_integration_models(integration_name: str):
    """Get models for a specific integration."""
    integrations = detect_available_integrations()

    if not integrations.get(integration_name, {}).get("available"):
        raise ImportError(f"Integration '{integration_name}' is not available")

    return integrations[integration_name]["models"]


def get_integration_extractor(integration_name: str):
    """Get extractor for a specific integration."""
    integrations = detect_available_integrations()

    if not integrations.get(integration_name, {}).get("available"):
        raise ImportError(f"Integration '{integration_name}' is not available")

    return integrations[integration_name]["extractor"]


def list_available_integrations() -> list[str]:
    """List all available integrations."""
    integrations = detect_available_integrations()
    return [name for name, info in integrations.items() if info.get("available", False)]
