from __future__ import annotations

"""fabric_api.models

Typed Pydantic domain models that mirror the legacy AL tables used in the
PTE Manage API extension for Business Central.  These models act as the
single‑source‑of‑truth for downstream extraction, transformation and loading
logic running inside Microsoft Fabric.

Coding conventions
------------------
* Pydantic v2 `BaseModel` – no dataclasses.
* Type‑hints use builtin collection generics (``list[int]``) – we never import
  ``List`` or ``Dict`` from *typing*.
* Optional fields are annotated with ``| None`` (PEP 604 union syntax).
* Relationships are maintained explicitly as attributes on the *parent* model
  (e.g. ``ManageInvoiceHeader.invoice_lines``) and as foreign‑keys on the
  *child* model (e.g. ``ManageInvoiceLine.invoice_number``).
* The models only describe *shape* – business rules live elsewhere.
"""

from datetime import date, datetime

from pydantic import BaseModel, Field

__all__ = [
    "ManageApiSetup",
    "ManageInvoiceHeader",
    "ManageInvoiceLine",
    "ManageTimeEntry",
    "ManageInvoiceExpense",
    "ManageProduct",
    "ManageInvoiceError",
    "ManageAgreement",
]


# ---------------------------------------------------------------------------
# Core configuration ---------------------------------------------------------
# ---------------------------------------------------------------------------


class ManageApiSetup(BaseModel):  # Table 62000
    """ConnectWise authentication & integration settings."""

    company: str = Field(default="", description="ConnectWise company identifier – e.g. 'thekking'.")
    client_id: str = Field(default="", description="ConnectWise *clientId* header value.")
    username: str | None = Field(default=None, description="Basic‑auth username.")
    password: str | None = Field(default=None, description="Basic‑auth password (stored securely).")
    public_key: str | None = Field(default=None, description="Public integration key (legacy auth).")
    private_key: str | None = Field(default=None, description="Private integration key (legacy auth).")
    base_url: str = Field(
        default="https://verk.thekking.is/v4_6_release/apis/3.0",
        description="Base REST URL – overridable per environment.",
    )
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Invoices -------------------------------------------------------------------
# ---------------------------------------------------------------------------


class ManageInvoiceLine(BaseModel):  # Table 62002
    """Line‑item details for a ConnectWise invoice."""

    invoice_number: str = Field(default="", description="Foreign‑key to *ManageInvoiceHeader.invoice_number*.")
    line_no: int = Field(default=..., description="Sequential line number within the invoice header.")

    memo: str | None = Field(default=None)
    quantity: float = 0.0
    line_amount: float = 0.0
    description: str | None = None

    cost: float | None = None
    price: float | None = None

    product_id: int | None = Field(default=None, description="ConnectWise product/id if this line represents a product.")
    item_identifier: str | None = None

    time_entry_id: int | None = Field(
        default=None, description="Link back to *ManageTimeEntry.time_entry_id* if relevant."
    )

    job_journal_line_no: int | None = None  # BC posting artefact
    document_date: date | None = None


class ManageInvoiceExpense(BaseModel):  # Table 62004
    """Expense line associated with an invoice."""

    invoice_number: str = Field(default="", description="Parent invoice header number.")
    line_no: int = Field(default=..., description="Sequential line number within the invoice header.")

    type: str | None = None  # Mileage, Meal, etc.
    quantity: float = 0.0
    amount: float = 0.0
    employee: str | None = None  # Employee identifier

    work_date: date | None = None

    agreement_id: int | None = None
    agreement_number: str | None = None
    parent_agreement_id: int | None = None
    agreement_type: str | None = None

    job_journal_line_no: int | None = None


class ManageProduct(BaseModel):  # Table 62005
    """Product billed on an invoice."""

    product_id: int = Field(default=..., description="ConnectWise product/id if this line represents a product.")
    invoice_number: str = Field(default="", description="Foreign key to *ManageInvoiceHeader.invoice_number*.")

    description: str | None = None
    discount_percentage: float = 0.0

    agreement_id: int | None = None
    agreement_number: str | None = None
    parent_agreement_id: int | None = None


class ManageInvoiceHeader(BaseModel):  # Table 62001
    """Top‑level invoice metadata."""

    billing_log_id: int = Field(default=0, description="Internal ConnectWise billingLogId.")
    invoice_number: str = Field(default="", description="Unique invoice number – primary key.")

    invoice_type: str = Field(default="", description="'Agreement' or 'Standard'.")
    customer_name: str | None = None
    social_security_no: str | None = None

    total_amount: float = 0.0
    total_amount_without_vat: float = 0.0
    vat_percentage: float = 0.0  # Already converted to percent (0‑100)

    site: str | None = None
    invoice_id: int | None = None  # ConnectWise *id* field
    project: str | None = None
    gl_entry_ids: str | None = None  # Comma‑separated list – mirrors AL design

    closed: bool = False
    sales_invoice_no: str | None = Field(default=None, description="Corresponding BC sales invoice.")
    invoice_date: date | None = None

    agreement_number: str | None = None

    # Runtime / calculated totals used in reporting
    job_journal_lines_amount: float = 0.0
    data_errors: int = 0
    total_amount_imported: float = 0.0
    posted_project_lines_amount: float = 0.0

    # ────────────────────────── Relationships ────────────────────────────
    invoice_lines: list[ManageInvoiceLine] = Field(default_factory=list, repr=False)
    time_entries: list[ManageTimeEntry] = Field(default_factory=list, repr=False)
    expenses: list[ManageInvoiceExpense] = Field(default_factory=list, repr=False)
    products: list[ManageProduct] = Field(default_factory=list, repr=False)
    errors: list[ManageInvoiceError] = Field(default_factory=list, repr=False)


# ---------------------------------------------------------------------------
# Time entries ---------------------------------------------------------------
# ---------------------------------------------------------------------------


class ManageTimeEntry(BaseModel):  # Table 62003
    """Employee work that may feed an invoice."""

    time_entry_id: int = Field(default=..., description="Primary key (ConnectWise id).")
    invoice_number: str | None = Field(
        default=None, description="Back‑reference to parent invoice header if billed."
    )

    employee: str | None = None  # Member.identifier

    agreement_id: int | None = None
    agreement_number: str | None = None
    parent_agreement_id: int | None = None
    agreement_type: str | None = None

    work_role_id: int | None = None
    work_type: str | None = None

    hourly_rate: float = 0.0
    global_hourly_rate: float = 0.0  # Fallback rate in AL implementation
    agreement_hours: float = 0.0
    agreement_amount: float = 0.0
    agreement_work_role_rate: float = 0.0

    ticket_id: int | None = None
    ticket_summary: str | None = None

    note: str | None = None
    site_name: str | None = None
    rate_type: str | None = None

    work_date: date | None = None


# ---------------------------------------------------------------------------
# Error tracking -------------------------------------------------------------
# ---------------------------------------------------------------------------


class ManageInvoiceError(BaseModel):  # Table 62006
    """Structured record of data issues detected during ETL."""

    invoice_number: str = Field(default="")
    error_table_id: str = Field(default=..., description="Identifier of the table where the error occurred.")

    missing_value: str | None = None
    error_message: str | None = None
    error_type: str | None = None  # e.g. MissingField, ValidationFailure, etc.
    table_name: str | None = None
    logged_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Agreements -----------------------------------------------------------------
# ---------------------------------------------------------------------------


class ManageAgreement(BaseModel):
    """ConnectWise agreement data."""

    agreement_id: int = Field(default=..., description="Primary key (ConnectWise id).")
    agreement_number: str | None = None
    name: str | None = None
    type: str | None = None
    status: str | None = None
    company_id: int | None = None
    company_name: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    parent_agreement_id: int | None = None
    agreement_type: str | None = None
    

# ---------------------------------------------------------------------------
# Pydantic forward‑reference resolution --------------------------------------
# ---------------------------------------------------------------------------

# After all classes are defined we can update forward refs so that Pydantic
# recognises type annotations like ``list[ManageInvoiceLine]`` when the target
# class appears *above*.

ManageInvoiceHeader.model_rebuild()
ManageTimeEntry.model_rebuild()
