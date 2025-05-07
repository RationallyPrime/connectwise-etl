from typing import List, Optional, Any, Dict, Union
from datetime import datetime
from pydantic import BaseModel, Field


class InfoObject(BaseModel):
    """Common _info object in ConnectWise responses"""
    additional_properties: Dict[str, str] = Field(default_factory=dict)


class IdentifierNameInfo(BaseModel):
    """Common pattern for resources with id, name, and _info"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class CompanyReference(BaseModel):
    """Company reference object"""
    id: Optional[int] = None
    identifier: Optional[str] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class LocationReference(BaseModel):
    """Location reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class DepartmentReference(BaseModel):
    """Department reference object"""
    id: Optional[int] = None
    identifier: Optional[str] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class SiteReference(BaseModel):
    """Site reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class CurrencyReference(BaseModel):
    """Currency reference object"""
    id: Optional[int] = None
    symbol: Optional[str] = None
    currency_code: Optional[str] = None
    decimal_separator: Optional[str] = None
    number_of_decimals: Optional[int] = None
    thousands_separator: Optional[str] = None
    negative_parentheses_flag: Optional[bool] = None
    display_symbol_flag: Optional[bool] = None
    currency_identifier: Optional[str] = None
    display_id_flag: Optional[bool] = None
    right_align: Optional[bool] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class TaxCodeReference(BaseModel):
    """Tax code reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class WorkTypeReference(BaseModel):
    """Work type reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    utilization_flag: Optional[bool] = None
    _info: Optional[InfoObject] = None


class WorkRoleReference(BaseModel):
    """Work role reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class AgreementReference(BaseModel):
    """Agreement reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    type: Optional[str] = None
    charge_firm_flag: Optional[bool] = None
    _info: Optional[InfoObject] = None


class InvoiceReference(BaseModel):
    """Invoice reference object"""
    id: Optional[int] = None
    identifier: Optional[str] = None
    billing_type: Optional[str] = None
    apply_to_type: Optional[str] = None
    invoice_date: Optional[str] = None
    charge_firm_flag: Optional[bool] = None
    _info: Optional[InfoObject] = None


class TicketReference(BaseModel):
    """Ticket reference object"""
    id: Optional[int] = None
    summary: Optional[str] = None
    _info: Optional[InfoObject] = None


class ProjectReference(BaseModel):
    """Project reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class PhaseReference(BaseModel):
    """Phase reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class SalesOrderReference(BaseModel):
    """Sales order reference object"""
    id: Optional[int] = None
    identifier: Optional[str] = None
    _info: Optional[InfoObject] = None


class MemberReference(BaseModel):
    """Member reference object"""
    id: Optional[int] = None
    identifier: Optional[str] = None
    name: Optional[str] = None
    daily_capacity: Optional[float] = None
    _info: Optional[InfoObject] = None


class ContactReference(BaseModel):
    """Contact reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class CustomField(BaseModel):
    """Custom field object"""
    id: Optional[int] = None
    caption: Optional[str] = None
    type: Optional[str] = None
    entry_method: Optional[str] = None
    number_of_decimals: Optional[int] = None
    value: Optional[Any] = None
    connectwise_id: Optional[str] = None


class BillingTermsReference(BaseModel):
    """Billing terms reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class StatusReference(BaseModel):
    """Status reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    is_closed: Optional[bool] = None
    _info: Optional[InfoObject] = None


class BillingCycleReference(BaseModel):
    """Billing cycle reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class InvoiceTemplateReference(BaseModel):
    """Invoice template reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class TypeReference(BaseModel):
    """Type reference object"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class AgreementTypeReference(BaseModel):
    """Agreement type reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class SLAReference(BaseModel):
    """SLA reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class ProductTypeReference(BaseModel):
    """Product type reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class UnitOfMeasureReference(BaseModel):
    """Unit of measure reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class VendorReference(BaseModel):
    """Vendor reference"""
    id: Optional[int] = None
    identifier: Optional[str] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class WarehouseReference(BaseModel):
    """Warehouse reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    locked_flag: Optional[bool] = None
    _info: Optional[InfoObject] = None


class WarehouseBinReference(BaseModel):
    """Warehouse bin reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class PaymentMethodReference(BaseModel):
    """Payment method reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class ClassificationReference(BaseModel):
    """Classification reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class ExpenseTax(BaseModel):
    """Expense tax object"""
    id: Optional[int] = None
    amount: Optional[float] = None
    type: Optional[TypeReference] = None


class RecurringInfo(BaseModel):
    """Recurring information for products"""
    recurring_revenue: Optional[float] = None
    recurring_cost: Optional[float] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    bill_cycle_id: Optional[int] = None
    bill_cycle: Optional[BillingCycleReference] = None
    cycles: Optional[int] = None
    cycle_type: Optional[str] = None
    agreement_type: Optional[AgreementTypeReference] = None


class InvoiceGroupingReference(BaseModel):
    """Invoice grouping reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    show_price_flag: Optional[bool] = None
    show_sub_items_flag: Optional[bool] = None
    group_parent_child_additions_flag: Optional[bool] = None
    _info: Optional[InfoObject] = None


class EntityTypeReference(BaseModel):
    """Entity type reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


class ForecastStatusReference(BaseModel):
    """Forecast status reference"""
    id: Optional[int] = None
    name: Optional[str] = None
    _info: Optional[InfoObject] = None


# Main API Response Models

class UnpostedInvoice(BaseModel):
    """Model for unposted invoice response"""
    id: Optional[int] = None
    billing_log_id: Optional[int] = None
    location_id: Optional[int] = None
    location: Optional[LocationReference] = None
    department_id: Optional[int] = None
    department: Optional[DepartmentReference] = None
    company: Optional[CompanyReference] = None
    account_number: Optional[str] = None
    bill_to_company: Optional[CompanyReference] = None
    bill_to_site: Optional[SiteReference] = None
    ship_to_company: Optional[CompanyReference] = None
    ship_to_site: Optional[SiteReference] = None
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_type: Optional[str] = None
    description: Optional[str] = None
    billing_terms: Optional[BillingTermsReference] = None
    due_days: Optional[str] = None
    due_date: Optional[str] = None
    currency: Optional[CurrencyReference] = None
    sub_total: Optional[float] = None
    total: Optional[float] = None
    has_time: Optional[bool] = None
    has_expenses: Optional[bool] = None
    has_products: Optional[bool] = None
    invoice_taxable_flag: Optional[bool] = None
    tax_code: Optional[TaxCodeReference] = None
    avalara_tax_flag: Optional[bool] = None
    item_taxable_flag: Optional[bool] = None
    sales_tax_amount: Optional[float] = None
    state_tax_flag: Optional[bool] = None
    state_tax_xref: Optional[str] = None
    state_tax_amount: Optional[float] = None
    county_tax_flag: Optional[bool] = None
    county_tax_xref: Optional[str] = None
    county_tax_amount: Optional[float] = None
    city_tax_flag: Optional[bool] = None
    city_tax_xref: Optional[str] = None
    city_tax_amount: Optional[float] = None
    country_tax_flag: Optional[bool] = None
    country_tax_xref: Optional[str] = None
    country_tax_amount: Optional[float] = None
    composite_tax_flag: Optional[bool] = None
    composite_tax_xref: Optional[str] = None
    composite_tax_amount: Optional[float] = None
    level_six_tax_flag: Optional[bool] = None
    level_six_tax_xref: Optional[str] = None
    level_six_tax_amount: Optional[float] = None
    created_by: Optional[str] = None
    date_closed: Optional[str] = None
    _info: Optional[InfoObject] = None


class Invoice(BaseModel):
    """Model for invoice response"""
    id: Optional[int] = None
    invoice_number: Optional[str] = None
    type: str
    status: Optional[StatusReference] = None
    company: CompanyReference
    bill_to_company: Optional[CompanyReference] = None
    ship_to_company: Optional[CompanyReference] = None
    account_number: Optional[str] = None
    apply_to_type: Optional[str] = None
    apply_to_id: Optional[int] = None
    attention: Optional[str] = None
    ship_to_attention: Optional[str] = None
    billing_site: Optional[SiteReference] = None
    billing_site_address_line1: Optional[str] = None
    billing_site_address_line2: Optional[str] = None
    billing_site_city: Optional[str] = None
    billing_site_state: Optional[str] = None
    billing_site_zip: Optional[str] = None
    billing_site_country: Optional[str] = None
    shipping_site: Optional[SiteReference] = None
    shipping_site_address_line1: Optional[str] = None
    shipping_site_address_line2: Optional[str] = None
    shipping_site_city: Optional[str] = None
    shipping_site_state: Optional[str] = None
    shipping_site_zip: Optional[str] = None
    shipping_site_country: Optional[str] = None
    billing_terms: Optional[BillingTermsReference] = None
    reference: Optional[str] = None
    customer_po: Optional[str] = None
    template_setup_id: Optional[int] = None
    invoice_template: Optional[InvoiceTemplateReference] = None
    email_template_id: Optional[int] = None
    add_to_batch_email_list: Optional[bool] = None
    date: Optional[datetime] = None
    restrict_downpayment_flag: Optional[bool] = None
    location_id: Optional[int] = None
    location: Optional[LocationReference] = None
    department_id: Optional[int] = None
    department: Optional[DepartmentReference] = None
    territory_id: Optional[int] = None
    territory: Optional[IdentifierNameInfo] = None
    top_comment: Optional[str] = None
    bottom_comment: Optional[str] = None
    taxable_flag: Optional[bool] = None
    tax_code: Optional[TaxCodeReference] = None
    internal_notes: Optional[str] = None
    downpayment_previously_taxed_flag: Optional[bool] = None
    service_total: Optional[float] = None
    override_down_payment_amount_flag: Optional[bool] = None
    currency: Optional[CurrencyReference] = None
    due_date: Optional[datetime] = None
    expense_total: Optional[float] = None
    product_total: Optional[float] = None
    previous_progress_applied: Optional[float] = None
    service_adjustment_amount: Optional[float] = None
    agreement_amount: Optional[float] = None
    downpayment_applied: Optional[float] = None
    subtotal: Optional[float] = None
    total: Optional[float] = None
    remaining_downpayment: Optional[float] = None
    sales_tax: Optional[float] = None
    adjustment_reason: Optional[str] = None
    adjusted_by: Optional[str] = None
    closed_by: Optional[str] = None
    payments: Optional[float] = None
    credits: Optional[float] = None
    balance: Optional[float] = None
    special_invoice_flag: Optional[bool] = None
    billing_setup_reference: Optional[IdentifierNameInfo] = None
    ticket: Optional[TicketReference] = None
    project: Optional[ProjectReference] = None
    phase: Optional[PhaseReference] = None
    sales_order: Optional[SalesOrderReference] = None
    agreement: Optional[AgreementReference] = None
    gl_batch: Optional[IdentifierNameInfo] = None
    unbatched_batch: Optional[IdentifierNameInfo] = None
    _info: Optional[InfoObject] = None
    custom_fields: Optional[List[CustomField]] = None


class Agreement(BaseModel):
    """Model for agreement response"""
    id: Optional[int] = None
    name: str
    type: AgreementTypeReference
    company: CompanyReference
    contact: ContactReference
    site: Optional[SiteReference] = None
    sub_contract_company: Optional[CompanyReference] = None
    sub_contract_contact: Optional[ContactReference] = None
    parent_agreement: Optional[AgreementReference] = None
    customer_po: Optional[str] = None
    location: Optional[LocationReference] = None
    department: Optional[DepartmentReference] = None
    restrict_location_flag: Optional[bool] = None
    restrict_department_flag: Optional[bool] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    no_ending_date_flag: Optional[bool] = None
    opportunity: Optional[IdentifierNameInfo] = None
    cancelled_flag: Optional[bool] = None
    date_cancelled: Optional[datetime] = None
    reason_cancelled: Optional[str] = None
    sla: Optional[SLAReference] = None
    work_order: Optional[str] = None
    internal_notes: Optional[str] = None
    application_units: Optional[str] = None
    application_limit: Optional[float] = None
    application_cycle: Optional[str] = None
    application_unlimited_flag: Optional[bool] = None
    one_time_flag: Optional[bool] = None
    cover_agreement_time: Optional[bool] = None
    cover_agreement_product: Optional[bool] = None
    cover_agreement_expense: Optional[bool] = None
    cover_sales_tax: Optional[bool] = None
    carry_over_unused: Optional[bool] = None
    allow_overruns: Optional[bool] = None
    expired_days: Optional[int] = None
    limit: Optional[int] = None
    expire_when_zero: Optional[bool] = None
    charge_to_firm: Optional[bool] = None
    employee_comp_rate: Optional[str] = None
    employee_comp_not_exceed: Optional[str] = None
    comp_hourly_rate: Optional[float] = None
    comp_limit_amount: Optional[float] = None
    billing_cycle: Optional[BillingCycleReference] = None
    bill_one_time_flag: Optional[bool] = None
    billing_terms: Optional[BillingTermsReference] = None
    invoicing_cycle: Optional[str] = None
    bill_to_company: Optional[CompanyReference] = None
    bill_to_contact: Optional[ContactReference] = None
    bill_to_site: Optional[SiteReference] = None
    bill_amount: Optional[float] = None
    taxable: Optional[bool] = None
    prorate_first_bill: Optional[float] = None
    bill_start_date: Optional[datetime] = None
    tax_code: Optional[TaxCodeReference] = None
    restrict_down_payment: Optional[bool] = None
    prorate_flag: Optional[bool] = None
    invoice_prorated_additions_flag: Optional[bool] = None
    invoice_description: Optional[str] = None
    top_comment: Optional[bool] = None
    bottom_comment: Optional[bool] = None
    work_role: Optional[WorkRoleReference] = None
    work_type: Optional[WorkTypeReference] = None
    project_type: Optional[IdentifierNameInfo] = None
    invoice_template: Optional[InvoiceTemplateReference] = None
    bill_time: Optional[str] = None
    bill_expenses: Optional[str] = None
    bill_products: Optional[str] = None
    billable_time_invoice: Optional[bool] = None
    billable_expense_invoice: Optional[bool] = None
    billable_product_invoice: Optional[bool] = None
    currency: Optional[CurrencyReference] = None
    period_type: Optional[str] = None
    auto_invoice_flag: Optional[bool] = None
    next_invoice_date: Optional[str] = None
    company_location: Optional[LocationReference] = None
    ship_to_company: Optional[CompanyReference] = None
    ship_to_contact: Optional[ContactReference] = None
    ship_to_site: Optional[SiteReference] = None
    agreement_status: Optional[str] = None
    _info: Optional[InfoObject] = None
    custom_fields: Optional[List[CustomField]] = None


class TimeEntry(BaseModel):
    """Model for time entry response"""
    id: Optional[int] = None
    company: Optional[CompanyReference] = None
    company_type: Optional[str] = None
    charge_to_id: Optional[int] = None
    charge_to_type: Optional[str] = None
    member: Optional[MemberReference] = None
    location_id: Optional[int] = None
    business_unit_id: Optional[int] = None
    business_group_desc: Optional[str] = None
    location: Optional[LocationReference] = None
    department: Optional[DepartmentReference] = None
    work_type: Optional[WorkTypeReference] = None
    work_role: Optional[WorkRoleReference] = None
    agreement: Optional[AgreementReference] = None
    agreement_type: Optional[str] = None
    activity: Optional[IdentifierNameInfo] = None
    opportunity_recid: Optional[int] = None
    project_activity: Optional[str] = None
    territory: Optional[str] = None
    time_start: datetime
    time_end: Optional[datetime] = None
    hours_deduct: Optional[float] = None
    actual_hours: Optional[float] = None
    billable_option: Optional[str] = None
    notes: Optional[str] = None
    internal_notes: Optional[str] = None
    add_to_detail_description_flag: Optional[bool] = None
    add_to_internal_analysis_flag: Optional[bool] = None
    add_to_resolution_flag: Optional[bool] = None
    email_resource_flag: Optional[bool] = None
    email_contact_flag: Optional[bool] = None
    email_cc_flag: Optional[bool] = None
    email_cc: Optional[str] = None
    hours_billed: Optional[float] = None
    invoice_hours: Optional[float] = None
    hourly_cost: Optional[str] = None
    entered_by: Optional[str] = None
    date_entered: Optional[datetime] = None
    invoice: Optional[InvoiceReference] = None
    mobile_guid: Optional[str] = None
    hourly_rate: Optional[float] = None
    overage_rate: Optional[float] = None
    agreement_hours: Optional[float] = None
    agreement_amount: Optional[float] = None
    agreement_adjustment: Optional[float] = None
    adjustment: Optional[float] = None
    invoice_ready: Optional[int] = None
    time_sheet: Optional[IdentifierNameInfo] = None
    status: Optional[str] = None
    ticket: Optional[TicketReference] = None
    project: Optional[ProjectReference] = None
    phase: Optional[PhaseReference] = None
    ticket_board: Optional[str] = None
    ticket_status: Optional[str] = None
    ticket_type: Optional[str] = None
    ticket_sub_type: Optional[str] = None
    invoice_flag: Optional[bool] = None
    extended_invoice_amount: Optional[float] = None
    location_name: Optional[str] = None
    tax_code: Optional[TaxCodeReference] = None
    _info: Optional[InfoObject] = None
    custom_fields: Optional[List[CustomField]] = None


class ExpenseEntry(BaseModel):
    """Model for expense entry response"""
    id: Optional[int] = None
    expense_report: Optional[IdentifierNameInfo] = None
    company: Optional[CompanyReference] = None
    charge_to_id: Optional[int] = None
    charge_to_type: Optional[str] = None
    type: TypeReference
    member: Optional[MemberReference] = None
    payment_method: Optional[PaymentMethodReference] = None
    classification: Optional[ClassificationReference] = None
    amount: float
    billable_option: Optional[str] = None
    date: datetime
    location_id: Optional[int] = None
    business_unit_id: Optional[int] = None
    notes: Optional[str] = None
    agreement: Optional[AgreementReference] = None
    invoice_amount: Optional[float] = None
    mobile_guid: Optional[str] = None
    taxes: Optional[List[ExpenseTax]] = None
    invoice: Optional[InvoiceReference] = None
    currency: Optional[CurrencyReference] = None
    status: Optional[str] = None
    bill_amount: Optional[float] = None
    agreement_amount: Optional[float] = None
    odometer_start: Optional[float] = None
    odometer_end: Optional[float] = None
    ticket: Optional[TicketReference] = None
    project: Optional[ProjectReference] = None
    phase: Optional[PhaseReference] = None
    _info: Optional[InfoObject] = None
    custom_fields: Optional[List[CustomField]] = None


class ProductItem(BaseModel):
    """Model for product item response"""
    id: Optional[int] = None
    catalog_item: IdentifierNameInfo
    description: Optional[str] = None
    sequence_number: Optional[float] = None
    quantity: Optional[float] = None
    unit_of_measure: Optional[UnitOfMeasureReference] = None
    price: Optional[float] = None
    cost: Optional[float] = None
    ext_price: Optional[float] = None
    ext_cost: Optional[float] = None
    discount: Optional[float] = None
    margin: Optional[float] = None
    agreement_amount: Optional[float] = None
    price_method: Optional[str] = None
    billable_option: str
    agreement: Optional[AgreementReference] = None
    location_id: Optional[int] = None
    location: Optional[LocationReference] = None
    business_unit_id: Optional[int] = None
    business_unit: Optional[IdentifierNameInfo] = None
    vendor: Optional[VendorReference] = None
    vendor_sku: Optional[str] = None
    taxable_flag: Optional[bool] = None
    dropship_flag: Optional[bool] = None
    special_order_flag: Optional[bool] = None
    phase_product_flag: Optional[bool] = None
    cancelled_flag: Optional[bool] = None
    quantity_cancelled: Optional[float] = None
    cancelled_reason: Optional[str] = None
    customer_description: Optional[str] = None
    internal_notes: Optional[str] = None
    product_supplied_flag: Optional[bool] = None
    sub_contractor_ship_to_id: Optional[int] = None
    sub_contractor_amount_limit: Optional[float] = None
    recurring: Optional[RecurringInfo] = None
    sla: Optional[SLAReference] = None
    entity_type: Optional[EntityTypeReference] = None
    ticket: Optional[TicketReference] = None
    project: Optional[ProjectReference] = None
    phase: Optional[PhaseReference] = None
    sales_order: Optional[SalesOrderReference] = None
    opportunity: Optional[IdentifierNameInfo] = None
    invoice: Optional[InvoiceReference] = None
    warehouse_id: Optional[int] = None
    warehouse_id_object: Optional[WarehouseReference] = None
    warehouse_bin_id: Optional[int] = None
    warehouse_bin_id_object: Optional[WarehouseBinReference] = None
    calculated_price_flag: Optional[bool] = None
    calculated_cost_flag: Optional[bool] = None
    forecast_detail_id: Optional[int] = None
    cancelled_by: Optional[int] = None
    cancelled_date: Optional[datetime] = None
    warehouse: Optional[str] = None
    warehouse_bin: Optional[str] = None
    purchase_date: Optional[datetime] = None
    tax_code: Optional[TaxCodeReference] = None
    integration_x_ref: Optional[str] = None
    list_price: Optional[float] = None
    serial_number_ids: Optional[List[int]] = None
    serial_numbers: Optional[List[str]] = None
    company: Optional[CompanyReference] = None
    forecast_status: Optional[ForecastStatusReference] = None
    product_class: Optional[str] = None
    need_to_purchase_flag: Optional[bool] = None
    need_to_order_quantity: Optional[int] = None
    minimum_stock_flag: Optional[bool] = None
    ship_set: Optional[str] = None
    calculated_price: Optional[float] = None
    calculated_cost: Optional[float] = None
    invoice_grouping: Optional[InvoiceGroupingReference] = None
    po_approved_flag: Optional[bool] = None
    uom: Optional[str] = None
    add_components_flag: Optional[bool] = None
    ignore_pricing_schedules_flag: Optional[bool] = None
    asio_subscriptions_id: Optional[str] = None
    _info: Optional[InfoObject] = None
    bypass_forecast_update: Optional[bool] = None
    custom_fields: Optional[List[CustomField]] = None


# Lists for API responses
class UnpostedInvoiceList(BaseModel):
    """List of unposted invoices response"""
    items: List[UnpostedInvoice]


class InvoiceList(BaseModel):
    """List of invoices response"""
    items: List[Invoice]


class AgreementList(BaseModel):
    """List of agreements response"""
    items: List[Agreement]


class TimeEntryList(BaseModel):
    """List of time entries response"""
    items: List[TimeEntry]


class ExpenseEntryList(BaseModel):
    """List of expense entries response"""
    items: List[ExpenseEntry]


class ProductItemList(BaseModel):
    """List of product items response"""
    items: List[ProductItem]