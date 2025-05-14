#!/usr/bin/env python
"""
ConnectWise API models generated from actual API responses.
"""

from typing import List

from sparkdantic import SparkModel

# Import only the main models to avoid namespace collisions
from .agreement import Agreement
from .expense_entry import ExpenseEntry
from .invoice import PostedInvoice
from .product_item import ProductItem
from .time_entry import TimeEntry
from .unposted_invoice import UnpostedInvoice

__all__ = [
    "Agreement",
    # List container models
    "AgreementList",
    "ExpenseEntry",
    "ExpenseEntryList",
    "PostedInvoice",
    "PostedInvoiceList",
    "ProductItem",
    "ProductItemList",
    "TimeEntry",
    "TimeEntryList",
    "UnpostedInvoice",
    "UnpostedInvoiceList",
]

# For backward compatibility

# List container models for API responses
class AgreementList(SparkModel):
    """List of agreements response"""
    items: list[Agreement]

class PostedInvoiceList(SparkModel):
    """List of invoices response"""
    items: list[PostedInvoice]

class UnpostedInvoiceList(SparkModel):
    """List of unposted invoices response"""
    items: list[UnpostedInvoice]

class TimeEntryList(SparkModel):
    """List of time entries response"""
    items: list[TimeEntry]

class ExpenseEntryList(SparkModel):
    """List of expense entries response"""
    items: list[ExpenseEntry]

class ProductItemList(SparkModel):
    """List of product items response"""
    items: list[ProductItem]
