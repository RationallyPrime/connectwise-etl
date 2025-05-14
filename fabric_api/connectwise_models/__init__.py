"""ConnectWise API models generated from OpenAPI schema.

Compatible with Pydantic v2 and SparkDantic for Spark schema generation.
"""

from typing import Any, Dict, List, Optional

from pydantic import Field, RootModel
from sparkdantic import SparkModel

from .agreement import Agreement
from .expense_entry import ExpenseEntry
from .invoice import Invoice
from .posted_invoice import PostedInvoice
from .product_item import ProductItem
from .time_entry import TimeEntry
from .unposted_invoice import UnpostedInvoice

__all__ = [
    "Agreement",
    "ExpenseEntry",
    "Invoice",
    "PostedInvoice",
    "ProductItem",
    "TimeEntry",
    "UnpostedInvoice",
]
