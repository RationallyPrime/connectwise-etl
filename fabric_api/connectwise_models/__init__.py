#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
    "ExpenseEntry",
    "PostedInvoice",
    "ProductItem",
    "TimeEntry",
    "UnpostedInvoice",
    # List container models
    "AgreementList",
    "PostedInvoiceList",
    "UnpostedInvoiceList",
    "TimeEntryList",
    "ExpenseEntryList",
    "ProductItemList",
]

# For backward compatibility

# List container models for API responses
class AgreementList(SparkModel):
    """List of agreements response"""
    items: List[Agreement]

class PostedInvoiceList(SparkModel):
    """List of invoices response"""
    items: List[PostedInvoice]

class UnpostedInvoiceList(SparkModel):
    """List of unposted invoices response"""
    items: List[UnpostedInvoice]

class TimeEntryList(SparkModel):
    """List of time entries response"""
    items: List[TimeEntry]

class ExpenseEntryList(SparkModel):
    """List of expense entries response"""
    items: List[ExpenseEntry]

class ProductItemList(SparkModel):
    """List of product items response"""
    items: List[ProductItem]
