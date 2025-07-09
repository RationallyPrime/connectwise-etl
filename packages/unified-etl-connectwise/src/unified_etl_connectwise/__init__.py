"""ConnectWise PSA adapter for Unified ETL Framework."""

import logging

from .client import ConnectWiseClient, ConnectWiseExtractor
from .models import models as models_module
from .transforms import (
    create_expense_entry_fact,
    create_invoice_line_fact,
    create_time_entry_fact,
)

__version__ = "1.0.0"

logger = logging.getLogger(__name__)

# Integration interface for core framework
try:
    extractor = ConnectWiseClient(config={})  # ConnectWiseExtractor is now an alias
except Exception as e:
    # Failed to initialize (likely missing credentials), set to None
    logger.debug(f"ConnectWise client initialization skipped: {e}")
    extractor = None

models = {
    "agreement": models_module.Agreement,
    "timeentry": models_module.TimeEntry,  # No underscore - matches table name
    "invoice": models_module.Invoice,
    "expenseentry": models_module.ExpenseEntry,  # No underscore - matches table name
    "productitem": models_module.ProductItem,  # No underscore - matches table name
    # "productrecurring": models_module.ProductRecurring,  # Commenting out - table doesn't exist in bronze
}

__all__ = [
    "ConnectWiseClient",
    "ConnectWiseExtractor",
    "create_expense_entry_fact",
    "create_invoice_line_fact",
    "create_time_entry_fact",
    "extractor",
    "models",
]
