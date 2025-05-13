#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ConnectWise API models generated directly from OpenAPI schema.
"""

import warnings

# Filter Pydantic deprecation warnings for Config options
warnings.filterwarnings("ignore", message="Valid config keys have changed in V2")

from typing import List, Any
from pydantic import RootModel
from sparkdantic import SparkModel

from .agreement import Agreement
from .time_entry import TimeEntry
from .expense_entry import ExpenseEntry
from .posted_invoice import PostedInvoice
from .unposted_invoice import UnpostedInvoice
from .product_item import ProductItem

__all__ = [
    "Agreement",
    "TimeEntry",
    "ExpenseEntry",
    "PostedInvoice",
    "UnpostedInvoice",
    "ProductItem",
]