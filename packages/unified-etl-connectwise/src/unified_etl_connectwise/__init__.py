"""ConnectWise PSA adapter for Unified ETL Framework."""

from .client import ConnectWiseClient, ConnectWiseExtractor
from .models import models

__version__ = "1.0.0"

# Integration interface for core framework
try:
    extractor = ConnectWiseClient(config={})  # ConnectWiseExtractor is now an alias
except Exception:
    # Failed to initialize (likely missing credentials), set to None
    extractor = None

models = {
    "agreement": models.Agreement,
    "timeentry": models.TimeEntry,  # No underscore - matches table name
    "invoice": models.Invoice,
    "expenseentry": models.ExpenseEntry,  # No underscore - matches table name
    "productitem": models.ProductItem,  # No underscore - matches table name
    # "productrecurring": models.ProductRecurring,  # Commenting out - table doesn't exist in bronze
}

__all__ = [
    "ConnectWiseClient",
    "ConnectWiseExtractor",
    "extractor",
    "models",
]
