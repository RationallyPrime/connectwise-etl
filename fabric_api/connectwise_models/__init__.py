"""ConnectWise API models generated from OpenAPI schema.

Compatible with Pydantic v2 and SparkDantic for Spark schema generation.
"""

# Import all models from the single models.py file
from .models import (
    # Reference models
    ActivityReference,
    # Entity models
    Agreement,
    AgreementReference,
    AgreementTypeReference,
    BatchReference,
    ExpenseEntry,
    Invoice,
    PostedInvoice,
    ProductItem,
    TimeEntry,
    UnpostedInvoice,
)

__all__ = [
    # Reference models
    "ActivityReference",
    "AgreementReference",
    "AgreementTypeReference",
    "BatchReference",

    # Entity models
    "Agreement",
    "TimeEntry",
    "ExpenseEntry",
    "Invoice",
    "PostedInvoice",
    "UnpostedInvoice",
    "ProductItem",
]
