"""ConnectWise API models generated from OpenAPI schema.

Compatible with Pydantic v2 and SparkDantic for Spark schema generation.
"""

from typing import Any, Dict, List, Optional
from pydantic import Field
from sparkdantic import SparkModel

# Base reference model
class ActivityReference(SparkModel):
    """Base reference model for ConnectWise entities."""
    id: int | None = None
    name: str | None = None
    _info: dict[str, str] | None = None

class AgreementReference(ActivityReference):
    """Reference model for Agreement entities."""
    type: str | None = None
    chargeFirmFlag: bool | None = None

class AgreementTypeReference(ActivityReference):
    """Reference model for AgreementType entities."""
    pass

class BatchReference(AgreementTypeReference):
    """Reference model for Batch entities."""
    pass

# Entity imports
from .agreement import Agreement
from .time_entry import TimeEntry

__all__ = [
    # Reference models
    "ActivityReference",
    "AgreementReference",
    "AgreementTypeReference",
    "BatchReference",
    # Entity models
    "Agreement",
    "TimeEntry",
]
