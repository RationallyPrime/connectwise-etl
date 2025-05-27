"""
CDM models generated from CDM JSON schema files.

Compatible with Pydantic v2 and SparkDantic for Spark schema generation.
"""

from __future__ import annotations

from pydantic import ConfigDict, Field
from sparkdantic import SparkModel


class JobLedgerEntry(SparkModel):
    """
    Represents the table Job Ledger Entry
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    EntryNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryNo-1'}, title='Entry No.'
    )
    JobNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobNo-2'},
        max_length=20,
        title='Job No.',
    )
    PostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDate-3'},
        max_length=4,
        title='Posting Date',
    )
    DocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentNo-4'},
        max_length=20,
        title='Document No.',
    )
    Type: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Type-5'}, title='Type'
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-7'}, max_length=20, title='No.'
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-8'},
        max_length=100,
        title='Description',
    )
    Quantity: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Quantity-9'},
        multiple_of=1e-05,
        title='Quantity',
    )
    DirectUnitCostLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DirectUnitCostLCY-11'},
        multiple_of=1e-05,
        title='Direct Unit Cost (LCY)',
    )
    UnitCostLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitCostLCY-12'},
        multiple_of=1e-05,
        title='Unit Cost (LCY)',
    )
    TotalCostLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TotalCostLCY-13'},
        multiple_of=1e-05,
        title='Total Cost (LCY)',
    )
    UnitPriceLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitPriceLCY-14'},
        multiple_of=1e-05,
        title='Unit Price (LCY)',
    )
    TotalPriceLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TotalPriceLCY-15'},
        multiple_of=1e-05,
        title='Total Price (LCY)',
    )
    ResourceGroupNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ResourceGroupNo-16'},
        max_length=20,
        title='Resource Group No.',
    )
    UnitofMeasureCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitofMeasureCode-17'},
        max_length=10,
        title='Unit of Measure Code',
    )
    LocationCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LocationCode-20'},
        max_length=10,
        title='Location Code',
    )
    JobPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobPostingGroup-29'},
        max_length=20,
        title='Job Posting Group',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-30'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-31'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    WorkTypeCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WorkTypeCode-32'},
        max_length=10,
        title='Work Type Code',
    )
    CustomerPriceGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerPriceGroup-33'},
        max_length=10,
        title='Customer Price Group',
    )
    UserID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UserID-37'},
        max_length=50,
        title='User ID',
    )
    SourceCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SourceCode-38'},
        max_length=10,
        title='Source Code',
    )
    ShptMethodCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShptMethodCode-40'},
        max_length=10,
        title='Shpt. Method Code',
    )
    AmttoPosttoGL: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AmttoPosttoGL-60'},
        multiple_of=1e-05,
        title='Amt. to Post to G/L',
    )
    AmtPostedtoGL: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AmtPostedtoGL-61'},
        multiple_of=1e-05,
        title='Amt. Posted to G/L',
    )
    EntryType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryType-64'}, title='Entry Type'
    )
    JournalBatchName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JournalBatchName-75'},
        max_length=10,
        title='Journal Batch Name',
    )
    ReasonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReasonCode-76'},
        max_length=10,
        title='Reason Code',
    )
    TransactionType: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionType-77'},
        max_length=10,
        title='Transaction Type',
    )
    TransportMethod: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransportMethod-78'},
        max_length=10,
        title='Transport Method',
    )
    CountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CountryRegionCode-79'},
        max_length=10,
        title='Country/Region Code',
    )
    GenBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenBusPostingGroup-80'},
        max_length=20,
        title='Gen. Bus. Posting Group',
    )
    GenProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenProdPostingGroup-81'},
        max_length=20,
        title='Gen. Prod. Posting Group',
    )
    EntryExitPoint: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'EntryExitPoint-82'},
        max_length=10,
        title='Entry/Exit Point',
    )
    DocumentDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentDate-83'},
        max_length=4,
        title='Document Date',
    )
    ExternalDocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExternalDocumentNo-84'},
        max_length=35,
        title='External Document No.',
    )
    Area: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Area-85'}, max_length=10, title='Area'
    )
    TransactionSpecification: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionSpecification-86'},
        max_length=10,
        title='Transaction Specification',
    )
    NoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NoSeries-87'},
        max_length=20,
        title='No. Series',
    )
    AdditionalCurrencyTotalCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AdditionalCurrencyTotalCost-88'},
        multiple_of=1e-05,
        title='Additional-Currency Total Cost',
    )
    AddCurrencyTotalPrice: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AddCurrencyTotalPrice-89'},
        multiple_of=1e-05,
        title='Add.-Currency Total Price',
    )
    AddCurrencyLineAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AddCurrencyLineAmount-94'},
        multiple_of=1e-05,
        title='Add.-Currency Line Amount',
    )
    DimensionSetID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionSetID-480'},
        title='Dimension Set ID',
    )
    JobTaskNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobTaskNo-1000'},
        max_length=20,
        title='Job Task No.',
    )
    LineAmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineAmountLCY-1001'},
        multiple_of=1e-05,
        title='Line Amount (LCY)',
    )
    UnitCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitCost-1002'},
        multiple_of=1e-05,
        title='Unit Cost',
    )
    TotalCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TotalCost-1003'},
        multiple_of=1e-05,
        title='Total Cost',
    )
    UnitPrice: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitPrice-1004'},
        multiple_of=1e-05,
        title='Unit Price',
    )
    TotalPrice: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TotalPrice-1005'},
        multiple_of=1e-05,
        title='Total Price',
    )
    LineAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineAmount-1006'},
        multiple_of=1e-05,
        title='Line Amount',
    )
    LineDiscountAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineDiscountAmount-1007'},
        multiple_of=1e-05,
        title='Line Discount Amount',
    )
    LineDiscountAmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineDiscountAmountLCY-1008'},
        multiple_of=1e-05,
        title='Line Discount Amount (LCY)',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-1009'},
        max_length=10,
        title='Currency Code',
    )
    CurrencyFactor: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyFactor-1010'},
        multiple_of=1e-05,
        title='Currency Factor',
    )
    Description2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description2-1016'},
        max_length=50,
        title='Description 2',
    )
    LedgerEntryType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LedgerEntryType-1017'},
        title='Ledger Entry Type',
    )
    LedgerEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LedgerEntryNo-1018'},
        title='Ledger Entry No.',
    )
    SerialNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SerialNo-1019'},
        max_length=50,
        title='Serial No.',
    )
    LotNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LotNo-1020'},
        max_length=50,
        title='Lot No.',
    )
    LineDiscount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineDiscount-1021'},
        multiple_of=1e-05,
        title='Line Discount %',
    )
    LineType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'LineType-1022'}, title='Line Type'
    )
    OriginalUnitCostLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalUnitCostLCY-1023'},
        multiple_of=1e-05,
        title='Original Unit Cost (LCY)',
    )
    OriginalTotalCostLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalTotalCostLCY-1024'},
        multiple_of=1e-05,
        title='Original Total Cost (LCY)',
    )
    OriginalUnitCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalUnitCost-1025'},
        multiple_of=1e-05,
        title='Original Unit Cost',
    )
    OriginalTotalCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalTotalCost-1026'},
        multiple_of=1e-05,
        title='Original Total Cost',
    )
    OriginalTotalCostACY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalTotalCostACY-1027'},
        multiple_of=1e-05,
        title='Original Total Cost (ACY)',
    )
    Adjusted: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Adjusted-1028'}, title='Adjusted'
    )
    DateTimeAdjusted: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DateTimeAdjusted-1029'},
        max_length=8,
        title='DateTime Adjusted',
    )
    VariantCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VariantCode-5402'},
        max_length=10,
        title='Variant Code',
    )
    BinCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BinCode-5403'},
        max_length=20,
        title='Bin Code',
    )
    QtyperUnitofMeasure: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'QtyperUnitofMeasure-5404'},
        multiple_of=1e-05,
        title='Qty. per Unit of Measure',
    )
    QuantityBase: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'QuantityBase-5405'},
        multiple_of=1e-05,
        title='Quantity (Base)',
    )
    ServiceOrderNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ServiceOrderNo-5900'},
        max_length=20,
        title='Service Order No.',
    )
    PostedServiceShipmentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostedServiceShipmentNo-5901'},
        max_length=20,
        title='Posted Service Shipment No.',
    )
    PackageNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PackageNo-6515'},
        max_length=50,
        title='Package No.',
    )
    PTEOldPhaseCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEOldPhaseCode-65000'},
        max_length=10,
        title='PTE Old Phase Code',
    )
    PTEJobAnalyzesType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEJobAnalyzesType-65001'},
        title='PTE Job Analyzes Type',
    )
    PTECustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTECustomerNo-65002'},
        max_length=20,
        title='PTE Customer No.',
    )
    PTECustomerName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTECustomerName-65003'},
        max_length=50,
        title='PTE Customer Name',
    )
    PTEEmployeeDivision: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEEmployeeDivision-65004'},
        max_length=20,
        title='PTE Employee Division',
    )
    PTEJiraWorklogid: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEJiraWorklogid-65005'},
        title='PTE Jira Worklog_id',
    )
    PTEDescription3: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEDescription3-65020'},
        max_length=80,
        title='PTE Description3',
    )
    WJOBReviewedby: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBReviewedby-10007200'},
        max_length=50,
        title='WJOB Reviewed by',
    )
    WJOBWork: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'WJOBWork-10007201'}, title='WJOB Work'
    )
    WJOBJobType: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBJobType-10007202'},
        max_length=15,
        title='WJOB Job Type',
    )
    WJOBManuallyModified: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBManuallyModified-10007203'},
        title='WJOB Manually Modified',
    )
    WJOBReviewed: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBReviewed-10007204'},
        title='WJOB Reviewed',
    )
    WJOBRegbyResource: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBRegbyResource-10007206'},
        max_length=20,
        title='WJOB Reg. by Resource',
    )
    WJOBResourceName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBResourceName-10007208'},
        max_length=100,
        title='WJOB Resource Name',
    )
    WJobExternalDocument2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobExternalDocument2-10007210'},
        max_length=30,
        title='WJob External Document 2',
    )
    WJobPAProformaNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPAProformaNo-10007255'},
        max_length=20,
        title='WJobPA Proforma No.',
    )
    WJobJRRequestNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobJRRequestNo-10007270'},
        max_length=20,
        title='WJobJR Request No.',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class Item(SparkModel):
    """
    Represents the table Item
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-1'}, max_length=20, title='No.'
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-3'},
        max_length=100,
        title='Description',
    )
    Description2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description2-5'},
        max_length=50,
        title='Description 2',
    )
    BaseUnitofMeasure: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BaseUnitofMeasure-8'},
        max_length=10,
        title='Base Unit of Measure',
    )
    Type: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Type-10'}, title='Type'
    )
    InventoryPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InventoryPostingGroup-11'},
        max_length=20,
        title='Inventory Posting Group',
    )
    ItemDiscGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ItemDiscGroup-14'},
        max_length=20,
        title='Item Disc. Group',
    )
    UnitPrice: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitPrice-18'},
        multiple_of=1e-05,
        title='Unit Price',
    )
    UnitCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitCost-22'},
        multiple_of=1e-05,
        title='Unit Cost',
    )
    VendorNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorNo-31'},
        max_length=20,
        title='Vendor No.',
    )
    VendorItemNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorItemNo-32'},
        max_length=50,
        title='Vendor Item No.',
    )
    GrossWeight: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GrossWeight-41'},
        multiple_of=1e-05,
        title='Gross Weight',
    )
    NetWeight: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NetWeight-42'},
        multiple_of=1e-05,
        title='Net Weight',
    )
    UnitVolume: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitVolume-44'},
        multiple_of=1e-05,
        title='Unit Volume',
    )
    Blocked: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Blocked-54'}, title='Blocked'
    )
    GenProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenProdPostingGroup-91'},
        max_length=20,
        title='Gen. Prod. Posting Group',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-105'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-106'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    AssemblyPolicy: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AssemblyPolicy-910'},
        title='Assembly Policy',
    )
    ItemCategoryCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ItemCategoryCode-5702'},
        max_length=20,
        title='Item Category Code',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class CustLedgerEntry(SparkModel):
    """
    Represents the table Cust. Ledger Entry
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    EntryNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryNo-1'}, title='Entry No.'
    )
    CustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerNo-3'},
        max_length=20,
        title='Customer No.',
    )
    PostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDate-4'},
        max_length=4,
        title='Posting Date',
    )
    DocumentType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DocumentType-5'}, title='Document Type'
    )
    DocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentNo-6'},
        max_length=20,
        title='Document No.',
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-7'},
        max_length=100,
        title='Description',
    )
    CustomerName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerName-8'},
        max_length=100,
        title='Customer Name',
    )
    YourReference: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'YourReference-10'},
        max_length=35,
        title='Your Reference',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-11'},
        max_length=10,
        title='Currency Code',
    )
    SalesLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SalesLCY-18'},
        multiple_of=1e-05,
        title='Sales (LCY)',
    )
    ProfitLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ProfitLCY-19'},
        multiple_of=1e-05,
        title='Profit (LCY)',
    )
    InvDiscountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvDiscountLCY-20'},
        multiple_of=1e-05,
        title='Inv. Discount (LCY)',
    )
    SelltoCustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCustomerNo-21'},
        max_length=20,
        title='Sell-to Customer No.',
    )
    CustomerPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerPostingGroup-22'},
        max_length=20,
        title='Customer Posting Group',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-23'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-24'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    SalespersonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SalespersonCode-25'},
        max_length=20,
        title='Salesperson Code',
    )
    UserID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UserID-27'},
        max_length=50,
        title='User ID',
    )
    SourceCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SourceCode-28'},
        max_length=10,
        title='Source Code',
    )
    OnHold: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OnHold-33'},
        max_length=3,
        title='On Hold',
    )
    AppliestoDocType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoDocType-34'},
        title='Applies-to Doc. Type',
    )
    AppliestoDocNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoDocNo-35'},
        max_length=20,
        title='Applies-to Doc. No.',
    )
    Open: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Open-36'}, title='Open'
    )
    DueDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DueDate-37'},
        max_length=4,
        title='Due Date',
    )
    PmtDiscountDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtDiscountDate-38'},
        max_length=4,
        title='Pmt. Discount Date',
    )
    OriginalPmtDiscPossible: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalPmtDiscPossible-39'},
        multiple_of=1e-05,
        title='Original Pmt. Disc. Possible',
    )
    PmtDiscGivenLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtDiscGivenLCY-40'},
        multiple_of=1e-05,
        title='Pmt. Disc. Given (LCY)',
    )
    OrigPmtDiscPossibleLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OrigPmtDiscPossibleLCY-42'},
        multiple_of=1e-05,
        title='Orig. Pmt. Disc. Possible(LCY)',
    )
    Positive: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Positive-43'}, title='Positive'
    )
    ClosedbyEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyEntryNo-44'},
        title='Closed by Entry No.',
    )
    ClosedatDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedatDate-45'},
        max_length=4,
        title='Closed at Date',
    )
    ClosedbyAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyAmount-46'},
        multiple_of=1e-05,
        title='Closed by Amount',
    )
    AppliestoID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoID-47'},
        max_length=50,
        title='Applies-to ID',
    )
    JournalTemplName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JournalTemplName-48'},
        max_length=10,
        title='Journal Templ. Name',
    )
    JournalBatchName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JournalBatchName-49'},
        max_length=10,
        title='Journal Batch Name',
    )
    ReasonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReasonCode-50'},
        max_length=10,
        title='Reason Code',
    )
    BalAccountType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BalAccountType-51'},
        title='Bal. Account Type',
    )
    BalAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BalAccountNo-52'},
        max_length=20,
        title='Bal. Account No.',
    )
    TransactionNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionNo-53'},
        title='Transaction No.',
    )
    ClosedbyAmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyAmountLCY-54'},
        multiple_of=1e-05,
        title='Closed by Amount (LCY)',
    )
    DocumentDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentDate-62'},
        max_length=4,
        title='Document Date',
    )
    ExternalDocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExternalDocumentNo-63'},
        max_length=35,
        title='External Document No.',
    )
    CalculateInterest: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CalculateInterest-64'},
        title='Calculate Interest',
    )
    ClosingInterestCalculated: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosingInterestCalculated-65'},
        title='Closing Interest Calculated',
    )
    NoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NoSeries-66'},
        max_length=20,
        title='No. Series',
    )
    ClosedbyCurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyCurrencyCode-67'},
        max_length=10,
        title='Closed by Currency Code',
    )
    ClosedbyCurrencyAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyCurrencyAmount-68'},
        multiple_of=1e-05,
        title='Closed by Currency Amount',
    )
    AdjustedCurrencyFactor: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AdjustedCurrencyFactor-73'},
        multiple_of=1e-05,
        title='Adjusted Currency Factor',
    )
    OriginalCurrencyFactor: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalCurrencyFactor-74'},
        multiple_of=1e-05,
        title='Original Currency Factor',
    )
    RemainingPmtDiscPossible: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'RemainingPmtDiscPossible-77'},
        multiple_of=1e-05,
        title='Remaining Pmt. Disc. Possible',
    )
    PmtDiscToleranceDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtDiscToleranceDate-78'},
        max_length=4,
        title='Pmt. Disc. Tolerance Date',
    )
    MaxPaymentTolerance: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'MaxPaymentTolerance-79'},
        multiple_of=1e-05,
        title='Max. Payment Tolerance',
    )
    LastIssuedReminderLevel: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastIssuedReminderLevel-80'},
        title='Last Issued Reminder Level',
    )
    AcceptedPaymentTolerance: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AcceptedPaymentTolerance-81'},
        multiple_of=1e-05,
        title='Accepted Payment Tolerance',
    )
    AcceptedPmtDiscTolerance: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AcceptedPmtDiscTolerance-82'},
        title='Accepted Pmt. Disc. Tolerance',
    )
    PmtToleranceLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtToleranceLCY-83'},
        multiple_of=1e-05,
        title='Pmt. Tolerance (LCY)',
    )
    AmounttoApply: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AmounttoApply-84'},
        multiple_of=1e-05,
        title='Amount to Apply',
    )
    ICPartnerCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ICPartnerCode-85'},
        max_length=20,
        title='IC Partner Code',
    )
    ApplyingEntry: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ApplyingEntry-86'},
        title='Applying Entry',
    )
    Reversed: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Reversed-87'}, title='Reversed'
    )
    ReversedbyEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReversedbyEntryNo-88'},
        title='Reversed by Entry No.',
    )
    ReversedEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReversedEntryNo-89'},
        title='Reversed Entry No.',
    )
    Prepayment: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Prepayment-90'}, title='Prepayment'
    )
    PaymentReference: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentReference-171'},
        max_length=50,
        title='Payment Reference',
    )
    PaymentMethodCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentMethodCode-172'},
        max_length=10,
        title='Payment Method Code',
    )
    AppliestoExtDocNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoExtDocNo-173'},
        max_length=35,
        title='Applies-to Ext. Doc. No.',
    )
    RecipientBankAccount: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'RecipientBankAccount-288'},
        max_length=20,
        title='Recipient Bank Account',
    )
    MessagetoRecipient: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'MessagetoRecipient-289'},
        max_length=140,
        title='Message to Recipient',
    )
    ExportedtoPaymentFile: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExportedtoPaymentFile-290'},
        title='Exported to Payment File',
    )
    DimensionSetID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionSetID-480'},
        title='Dimension Set ID',
    )
    DirectDebitMandateID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DirectDebitMandateID-1200'},
        max_length=35,
        title='Direct Debit Mandate ID',
    )
    PaymentPrediction: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentPrediction-1300'},
        title='Payment Prediction',
    )
    PredictionConfidence: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PredictionConfidence-1302'},
        multiple_of=1e-05,
        title='Prediction Confidence %',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class VendorLedgerEntry(SparkModel):
    """
    Represents the table Vendor Ledger Entry
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    EntryNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryNo-1'}, title='Entry No.'
    )
    VendorNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorNo-3'},
        max_length=20,
        title='Vendor No.',
    )
    PostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDate-4'},
        max_length=4,
        title='Posting Date',
    )
    DocumentType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DocumentType-5'}, title='Document Type'
    )
    DocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentNo-6'},
        max_length=20,
        title='Document No.',
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-7'},
        max_length=100,
        title='Description',
    )
    VendorName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorName-8'},
        max_length=100,
        title='Vendor Name',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-11'},
        max_length=10,
        title='Currency Code',
    )
    PurchaseLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PurchaseLCY-18'},
        multiple_of=1e-05,
        title='Purchase (LCY)',
    )
    InvDiscountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvDiscountLCY-20'},
        multiple_of=1e-05,
        title='Inv. Discount (LCY)',
    )
    BuyfromVendorNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BuyfromVendorNo-21'},
        max_length=20,
        title='Buy-from Vendor No.',
    )
    VendorPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorPostingGroup-22'},
        max_length=20,
        title='Vendor Posting Group',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-23'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-24'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    PurchaserCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PurchaserCode-25'},
        max_length=20,
        title='Purchaser Code',
    )
    UserID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UserID-27'},
        max_length=50,
        title='User ID',
    )
    SourceCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SourceCode-28'},
        max_length=10,
        title='Source Code',
    )
    OnHold: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OnHold-33'},
        max_length=3,
        title='On Hold',
    )
    AppliestoDocType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoDocType-34'},
        title='Applies-to Doc. Type',
    )
    AppliestoDocNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoDocNo-35'},
        max_length=20,
        title='Applies-to Doc. No.',
    )
    Open: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Open-36'}, title='Open'
    )
    DueDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DueDate-37'},
        max_length=4,
        title='Due Date',
    )
    PmtDiscountDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtDiscountDate-38'},
        max_length=4,
        title='Pmt. Discount Date',
    )
    OriginalPmtDiscPossible: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalPmtDiscPossible-39'},
        multiple_of=1e-05,
        title='Original Pmt. Disc. Possible',
    )
    PmtDiscRcdLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtDiscRcdLCY-40'},
        multiple_of=1e-05,
        title='Pmt. Disc. Rcd.(LCY)',
    )
    OrigPmtDiscPossibleLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OrigPmtDiscPossibleLCY-42'},
        multiple_of=1e-05,
        title='Orig. Pmt. Disc. Possible(LCY)',
    )
    Positive: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Positive-43'}, title='Positive'
    )
    ClosedbyEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyEntryNo-44'},
        title='Closed by Entry No.',
    )
    ClosedatDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedatDate-45'},
        max_length=4,
        title='Closed at Date',
    )
    ClosedbyAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyAmount-46'},
        multiple_of=1e-05,
        title='Closed by Amount',
    )
    AppliestoID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoID-47'},
        max_length=50,
        title='Applies-to ID',
    )
    JournalTemplName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JournalTemplName-48'},
        max_length=10,
        title='Journal Templ. Name',
    )
    JournalBatchName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JournalBatchName-49'},
        max_length=10,
        title='Journal Batch Name',
    )
    ReasonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReasonCode-50'},
        max_length=10,
        title='Reason Code',
    )
    BalAccountType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BalAccountType-51'},
        title='Bal. Account Type',
    )
    BalAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BalAccountNo-52'},
        max_length=20,
        title='Bal. Account No.',
    )
    TransactionNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionNo-53'},
        title='Transaction No.',
    )
    ClosedbyAmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyAmountLCY-54'},
        multiple_of=1e-05,
        title='Closed by Amount (LCY)',
    )
    DocumentDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentDate-62'},
        max_length=4,
        title='Document Date',
    )
    ExternalDocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExternalDocumentNo-63'},
        max_length=35,
        title='External Document No.',
    )
    NoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NoSeries-64'},
        max_length=20,
        title='No. Series',
    )
    ClosedbyCurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyCurrencyCode-65'},
        max_length=10,
        title='Closed by Currency Code',
    )
    ClosedbyCurrencyAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ClosedbyCurrencyAmount-66'},
        multiple_of=1e-05,
        title='Closed by Currency Amount',
    )
    AdjustedCurrencyFactor: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AdjustedCurrencyFactor-73'},
        multiple_of=1e-05,
        title='Adjusted Currency Factor',
    )
    OriginalCurrencyFactor: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OriginalCurrencyFactor-74'},
        multiple_of=1e-05,
        title='Original Currency Factor',
    )
    RemainingPmtDiscPossible: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'RemainingPmtDiscPossible-77'},
        multiple_of=1e-05,
        title='Remaining Pmt. Disc. Possible',
    )
    PmtDiscToleranceDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtDiscToleranceDate-78'},
        max_length=4,
        title='Pmt. Disc. Tolerance Date',
    )
    MaxPaymentTolerance: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'MaxPaymentTolerance-79'},
        multiple_of=1e-05,
        title='Max. Payment Tolerance',
    )
    AcceptedPaymentTolerance: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AcceptedPaymentTolerance-81'},
        multiple_of=1e-05,
        title='Accepted Payment Tolerance',
    )
    AcceptedPmtDiscTolerance: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AcceptedPmtDiscTolerance-82'},
        title='Accepted Pmt. Disc. Tolerance',
    )
    PmtToleranceLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtToleranceLCY-83'},
        multiple_of=1e-05,
        title='Pmt. Tolerance (LCY)',
    )
    AmounttoApply: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AmounttoApply-84'},
        multiple_of=1e-05,
        title='Amount to Apply',
    )
    ICPartnerCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ICPartnerCode-85'},
        max_length=20,
        title='IC Partner Code',
    )
    ApplyingEntry: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ApplyingEntry-86'},
        title='Applying Entry',
    )
    Reversed: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Reversed-87'}, title='Reversed'
    )
    ReversedbyEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReversedbyEntryNo-88'},
        title='Reversed by Entry No.',
    )
    ReversedEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReversedEntryNo-89'},
        title='Reversed Entry No.',
    )
    Prepayment: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Prepayment-90'}, title='Prepayment'
    )
    CreditorNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreditorNo-170'},
        max_length=20,
        title='Creditor No.',
    )
    PaymentReference: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentReference-171'},
        max_length=50,
        title='Payment Reference',
    )
    PaymentMethodCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentMethodCode-172'},
        max_length=10,
        title='Payment Method Code',
    )
    AppliestoExtDocNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoExtDocNo-173'},
        max_length=35,
        title='Applies-to Ext. Doc. No.',
    )
    InvoiceReceivedDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvoiceReceivedDate-175'},
        max_length=4,
        title='Invoice Received Date',
    )
    RecipientBankAccount: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'RecipientBankAccount-288'},
        max_length=20,
        title='Recipient Bank Account',
    )
    MessagetoRecipient: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'MessagetoRecipient-289'},
        max_length=140,
        title='Message to Recipient',
    )
    ExportedtoPaymentFile: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExportedtoPaymentFile-290'},
        title='Exported to Payment File',
    )
    DimensionSetID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionSetID-480'},
        title='Dimension Set ID',
    )
    RemittoCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'RemittoCode-1000'},
        max_length=20,
        title='Remit-to Code',
    )
    PTEGreia: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'PTEGreia-65000'}, title='PTE Greiða'
    )
    PTEtgjaldaflokkur: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEtgjaldaflokkur-65001'},
        max_length=10,
        title='PTE Útgjaldaflokkur',
    )
    PTEFastanmer: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEFastanmer-65002'},
        max_length=7,
        title='PTE Fastanúmer',
    )
    PTEContractor: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEContractor-65003'},
        title='PTE Contractor',
    )
    PTEGrseill: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEGrseill-65004'},
        title='PTE Gíróseðill',
    )
    PTEUppskrifaaf: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEUppskrifaaf-65005'},
        max_length=50,
        title='PTE Uppáskrifað af',
    )
    PTEsamykktur: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEsamykktur-65006'},
        title='PTE Ósamþykktur',
    )
    PTEMttkudagsetning: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEMttkudagsetning-65007'},
        max_length=4,
        title='PTE Móttökudagsetning',
    )
    PTEBankatexti: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEBankatexti-65008'},
        max_length=250,
        title='PTE Bankatexti',
    )
    PTEGreisluform: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEGreisluform-65009'},
        title='PTE Greiðsluform',
    )
    PTEbkaur: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'PTEbkaur-65010'}, title='PTE Óbókaður'
    )
    CTRContractor: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CTRContractor-10027575'},
        title='CTR Contractor',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class Currency(SparkModel):
    """
    Represents the table Currency
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    Code: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Code-1'}, max_length=10, title='Code'
    )
    ISOCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ISOCode-4'},
        max_length=3,
        title='ISO Code',
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-15'},
        max_length=30,
        title='Description',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class DimensionValue(SparkModel):
    """
    Represents the table Dimension Value
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    DimensionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionCode-1'},
        max_length=20,
        title='Dimension Code',
    )
    Code: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Code-2'}, max_length=20, title='Code'
    )
    Name: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name-3'}, max_length=50, title='Name'
    )
    DimensionValueType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionValueType-4'},
        title='Dimension Value Type',
    )
    Totaling: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Totaling-5'},
        max_length=250,
        title='Totaling',
    )
    Blocked: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Blocked-6'}, title='Blocked'
    )
    ConsolidationCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ConsolidationCode-7'},
        max_length=20,
        title='Consolidation Code',
    )
    Indentation: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Indentation-8'}, title='Indentation'
    )
    GlobalDimensionNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimensionNo-9'},
        title='Global Dimension No.',
    )
    MaptoICDimensionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'MaptoICDimensionCode-10'},
        max_length=20,
        title='Map-to IC Dimension Code',
    )
    MaptoICDimensionValueCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'MaptoICDimensionValueCode-11'},
        max_length=20,
        title='Map-to IC Dimension Value Code',
    )
    DimensionValueID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionValueID-12'},
        title='Dimension Value ID',
    )
    DimensionId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionId-8002'},
        max_length=16,
        title='Dimension Id',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class SalesInvoiceLine(SparkModel):
    """
    Represents the table Sales Invoice Line
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    SelltoCustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCustomerNo-2'},
        max_length=20,
        title='Sell-to Customer No.',
    )
    DocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentNo-3'},
        max_length=20,
        title='Document No.',
    )
    LineNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'LineNo-4'}, title='Line No.'
    )
    Type: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Type-5'}, title='Type'
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-6'}, max_length=20, title='No.'
    )
    LocationCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LocationCode-7'},
        max_length=10,
        title='Location Code',
    )
    PostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingGroup-8'},
        max_length=20,
        title='Posting Group',
    )
    ShipmentDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShipmentDate-10'},
        max_length=4,
        title='Shipment Date',
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-11'},
        max_length=100,
        title='Description',
    )
    Description2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description2-12'},
        max_length=50,
        title='Description 2',
    )
    UnitofMeasure: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitofMeasure-13'},
        max_length=50,
        title='Unit of Measure',
    )
    Quantity: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Quantity-15'},
        multiple_of=1e-05,
        title='Quantity',
    )
    UnitPrice: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitPrice-22'},
        multiple_of=1e-05,
        title='Unit Price',
    )
    UnitCostLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitCostLCY-23'},
        multiple_of=1e-05,
        title='Unit Cost (LCY)',
    )
    VAT: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VAT-25'},
        multiple_of=1e-05,
        title='VAT %',
    )
    LineDiscount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineDiscount-27'},
        multiple_of=1e-05,
        title='Line Discount %',
    )
    LineDiscountAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineDiscountAmount-28'},
        multiple_of=1e-05,
        title='Line Discount Amount',
    )
    Amount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Amount-29'},
        multiple_of=1e-05,
        title='Amount',
    )
    AmountIncludingVAT: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AmountIncludingVAT-30'},
        multiple_of=1e-05,
        title='Amount Including VAT',
    )
    AllowInvoiceDisc: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AllowInvoiceDisc-32'},
        title='Allow Invoice Disc.',
    )
    GrossWeight: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GrossWeight-34'},
        multiple_of=1e-05,
        title='Gross Weight',
    )
    NetWeight: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NetWeight-35'},
        multiple_of=1e-05,
        title='Net Weight',
    )
    UnitsperParcel: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitsperParcel-36'},
        multiple_of=1e-05,
        title='Units per Parcel',
    )
    UnitVolume: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitVolume-37'},
        multiple_of=1e-05,
        title='Unit Volume',
    )
    AppltoItemEntry: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppltoItemEntry-38'},
        title='Appl.-to Item Entry',
    )
    ShortcutDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension1Code-40'},
        max_length=20,
        title='Shortcut Dimension 1 Code',
    )
    ShortcutDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension2Code-41'},
        max_length=20,
        title='Shortcut Dimension 2 Code',
    )
    CustomerPriceGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerPriceGroup-42'},
        max_length=10,
        title='Customer Price Group',
    )
    JobNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobNo-45'},
        max_length=20,
        title='Job No.',
    )
    WorkTypeCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WorkTypeCode-52'},
        max_length=10,
        title='Work Type Code',
    )
    ShipmentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShipmentNo-63'},
        max_length=20,
        title='Shipment No.',
    )
    ShipmentLineNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShipmentLineNo-64'},
        title='Shipment Line No.',
    )
    OrderNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OrderNo-65'},
        max_length=20,
        title='Order No.',
    )
    OrderLineNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'OrderLineNo-66'}, title='Order Line No.'
    )
    BilltoCustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCustomerNo-68'},
        max_length=20,
        title='Bill-to Customer No.',
    )
    InvDiscountAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvDiscountAmount-69'},
        multiple_of=1e-05,
        title='Inv. Discount Amount',
    )
    DropShipment: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DropShipment-73'}, title='Drop Shipment'
    )
    GenBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenBusPostingGroup-74'},
        max_length=20,
        title='Gen. Bus. Posting Group',
    )
    GenProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenProdPostingGroup-75'},
        max_length=20,
        title='Gen. Prod. Posting Group',
    )
    VATCalculationType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATCalculationType-77'},
        title='VAT Calculation Type',
    )
    TransactionType: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionType-78'},
        max_length=10,
        title='Transaction Type',
    )
    TransportMethod: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransportMethod-79'},
        max_length=10,
        title='Transport Method',
    )
    AttachedtoLineNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AttachedtoLineNo-80'},
        title='Attached to Line No.',
    )
    ExitPoint: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExitPoint-81'},
        max_length=10,
        title='Exit Point',
    )
    Area: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Area-82'}, max_length=10, title='Area'
    )
    TransactionSpecification: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionSpecification-83'},
        max_length=10,
        title='Transaction Specification',
    )
    TaxCategory: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxCategory-84'},
        max_length=10,
        title='Tax Category',
    )
    TaxAreaCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxAreaCode-85'},
        max_length=20,
        title='Tax Area Code',
    )
    TaxLiable: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'TaxLiable-86'}, title='Tax Liable'
    )
    TaxGroupCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxGroupCode-87'},
        max_length=20,
        title='Tax Group Code',
    )
    VATClauseCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATClauseCode-88'},
        max_length=20,
        title='VAT Clause Code',
    )
    VATBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATBusPostingGroup-89'},
        max_length=20,
        title='VAT Bus. Posting Group',
    )
    VATProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATProdPostingGroup-90'},
        max_length=20,
        title='VAT Prod. Posting Group',
    )
    BlanketOrderNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BlanketOrderNo-97'},
        max_length=20,
        title='Blanket Order No.',
    )
    BlanketOrderLineNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BlanketOrderLineNo-98'},
        title='Blanket Order Line No.',
    )
    VATBaseAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATBaseAmount-99'},
        multiple_of=1e-05,
        title='VAT Base Amount',
    )
    UnitCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitCost-100'},
        multiple_of=1e-05,
        title='Unit Cost',
    )
    SystemCreatedEntry: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedEntry-101'},
        title='System-Created Entry',
    )
    LineAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineAmount-103'},
        multiple_of=1e-05,
        title='Line Amount',
    )
    VATDifference: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATDifference-104'},
        multiple_of=1e-05,
        title='VAT Difference',
    )
    VATIdentifier: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATIdentifier-106'},
        max_length=20,
        title='VAT Identifier',
    )
    ICPartnerRefType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ICPartnerRefType-107'},
        title='IC Partner Ref. Type',
    )
    ICPartnerReference: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ICPartnerReference-108'},
        max_length=20,
        title='IC Partner Reference',
    )
    PrepaymentLine: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PrepaymentLine-123'},
        title='Prepayment Line',
    )
    ICPartnerCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ICPartnerCode-130'},
        max_length=20,
        title='IC Partner Code',
    )
    PostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDate-131'},
        max_length=4,
        title='Posting Date',
    )
    ICItemReferenceNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ICItemReferenceNo-138'},
        max_length=50,
        title='IC Item Reference No.',
    )
    PmtDiscountAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtDiscountAmount-145'},
        multiple_of=1e-05,
        title='Pmt. Discount Amount',
    )
    LineDiscountCalculation: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LineDiscountCalculation-180'},
        title='Line Discount Calculation',
    )
    DimensionSetID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionSetID-480'},
        title='Dimension Set ID',
    )
    JobTaskNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobTaskNo-1001'},
        max_length=20,
        title='Job Task No.',
    )
    JobContractEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobContractEntryNo-1002'},
        title='Job Contract Entry No.',
    )
    DeferralCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DeferralCode-1700'},
        max_length=10,
        title='Deferral Code',
    )
    AllocationAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AllocationAccountNo-2678'},
        max_length=20,
        title='Allocation Account No.',
    )
    VariantCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VariantCode-5402'},
        max_length=10,
        title='Variant Code',
    )
    BinCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BinCode-5403'},
        max_length=20,
        title='Bin Code',
    )
    QtyperUnitofMeasure: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'QtyperUnitofMeasure-5404'},
        multiple_of=1e-05,
        title='Qty. per Unit of Measure',
    )
    UnitofMeasureCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitofMeasureCode-5407'},
        max_length=10,
        title='Unit of Measure Code',
    )
    QuantityBase: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'QuantityBase-5415'},
        multiple_of=1e-05,
        title='Quantity (Base)',
    )
    FAPostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'FAPostingDate-5600'},
        max_length=4,
        title='FA Posting Date',
    )
    DepreciationBookCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DepreciationBookCode-5602'},
        max_length=10,
        title='Depreciation Book Code',
    )
    DepruntilFAPostingDate: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DepruntilFAPostingDate-5605'},
        title='Depr. until FA Posting Date',
    )
    DuplicateinDepreciationBook: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DuplicateinDepreciationBook-5612'},
        max_length=10,
        title='Duplicate in Depreciation Book',
    )
    UseDuplicationList: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UseDuplicationList-5613'},
        title='Use Duplication List',
    )
    ResponsibilityCenter: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ResponsibilityCenter-5700'},
        max_length=10,
        title='Responsibility Center',
    )
    ItemCategoryCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ItemCategoryCode-5709'},
        max_length=20,
        title='Item Category Code',
    )
    Nonstock: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Nonstock-5710'}, title='Nonstock'
    )
    PurchasingCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PurchasingCode-5711'},
        max_length=10,
        title='Purchasing Code',
    )
    ItemReferenceNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ItemReferenceNo-5725'},
        max_length=50,
        title='Item Reference No.',
    )
    ItemReferenceUnitofMeasure: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ItemReferenceUnitofMeasure-5726'},
        max_length=10,
        title='Item Reference Unit of Measure',
    )
    ItemReferenceType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ItemReferenceType-5727'},
        title='Item Reference Type',
    )
    ItemReferenceTypeNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ItemReferenceTypeNo-5728'},
        max_length=30,
        title='Item Reference Type No.',
    )
    ApplfromItemEntry: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ApplfromItemEntry-5811'},
        title='Appl.-from Item Entry',
    )
    ReturnReasonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReturnReasonCode-6608'},
        max_length=10,
        title='Return Reason Code',
    )
    PriceCalculationMethod: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PriceCalculationMethod-7000'},
        title='Price Calculation Method',
    )
    AllowLineDisc: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AllowLineDisc-7001'},
        title='Allow Line Disc.',
    )
    CustomerDiscGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerDiscGroup-7002'},
        max_length=20,
        title='Customer Disc. Group',
    )
    Pricedescription: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Pricedescription-7004'},
        max_length=80,
        title='Price description',
    )
    ShpfyOrderLineId: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShpfyOrderLineId-30100'},
        title='Shpfy Order Line Id',
    )
    ShpfyOrderNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShpfyOrderNo-30101'},
        max_length=50,
        title='Shpfy Order No.',
    )
    PTEDescription3: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEDescription3-65008'},
        max_length=80,
        title='PTE Description3',
    )
    AMSAgreementNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSAgreementNo-10007050'},
        max_length=20,
        title='AMSAgreement No.',
    )
    AMSIndex: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSIndex-10007051'},
        max_length=10,
        title='AMSIndex',
    )
    AMSIndexBaseRate: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSIndexBaseRate-10007052'},
        multiple_of=1e-05,
        title='AMSIndex Base Rate',
    )
    AMSIndexRate: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSIndexRate-10007053'},
        multiple_of=1e-05,
        title='AMSIndex Rate',
    )
    AMSAgreementLineType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSAgreementLineType-10007054'},
        title='AMSAgreement Line Type',
    )
    AMSAgreementLineNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSAgreementLineNo-10007055'},
        max_length=20,
        title='AMSAgreement Line No.',
    )
    AMSAgreementLineLineNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSAgreementLineLineNo-10007059'},
        title='AMSAgreement Line Line No.',
    )
    AMSInvoicePrintingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSInvoicePrintingGroup-10007060'},
        max_length=20,
        title='AMSInvoice Printing Group',
    )
    WJOBPrintOrder: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBPrintOrder-10007200'},
        title='WJOB Print Order',
    )
    WJOBCostResource: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBCostResource-10007201'},
        title='WJOB Cost Resource',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class Vendor(SparkModel):
    """
    Represents the table Vendor
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-1'}, max_length=20, title='No.'
    )
    Name: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name-2'}, max_length=100, title='Name'
    )
    SearchName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SearchName-3'},
        max_length=100,
        title='Search Name',
    )
    Name2: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name2-4'}, max_length=50, title='Name 2'
    )
    Address: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Address-5'},
        max_length=100,
        title='Address',
    )
    Address2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Address2-6'},
        max_length=50,
        title='Address 2',
    )
    City: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'City-7'}, max_length=30, title='City'
    )
    Contact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Contact-8'},
        max_length=100,
        title='Contact',
    )
    PhoneNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PhoneNo-9'},
        max_length=30,
        title='Phone No.',
    )
    TelexNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TelexNo-10'},
        max_length=20,
        title='Telex No.',
    )
    OurAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OurAccountNo-14'},
        max_length=20,
        title='Our Account No.',
    )
    TerritoryCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TerritoryCode-15'},
        max_length=10,
        title='Territory Code',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-16'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-17'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    BudgetedAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BudgetedAmount-19'},
        multiple_of=1e-05,
        title='Budgeted Amount',
    )
    VendorPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorPostingGroup-21'},
        max_length=20,
        title='Vendor Posting Group',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-22'},
        max_length=10,
        title='Currency Code',
    )
    LanguageCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LanguageCode-24'},
        max_length=10,
        title='Language Code',
    )
    RegistrationNumber: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'RegistrationNumber-25'},
        max_length=50,
        title='Registration Number',
    )
    StatisticsGroup: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'StatisticsGroup-26'},
        title='Statistics Group',
    )
    PaymentTermsCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentTermsCode-27'},
        max_length=10,
        title='Payment Terms Code',
    )
    FinChargeTermsCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'FinChargeTermsCode-28'},
        max_length=10,
        title='Fin. Charge Terms Code',
    )
    PurchaserCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PurchaserCode-29'},
        max_length=20,
        title='Purchaser Code',
    )
    ShipmentMethodCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShipmentMethodCode-30'},
        max_length=10,
        title='Shipment Method Code',
    )
    ShippingAgentCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShippingAgentCode-31'},
        max_length=10,
        title='Shipping Agent Code',
    )
    InvoiceDiscCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvoiceDiscCode-33'},
        max_length=20,
        title='Invoice Disc. Code',
    )
    CountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CountryRegionCode-35'},
        max_length=10,
        title='Country/Region Code',
    )
    Blocked: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Blocked-39'}, title='Blocked'
    )
    PaytoVendorNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaytoVendorNo-45'},
        max_length=20,
        title='Pay-to Vendor No.',
    )
    Priority: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Priority-46'}, title='Priority'
    )
    PaymentMethodCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentMethodCode-47'},
        max_length=10,
        title='Payment Method Code',
    )
    FormatRegion: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'FormatRegion-48'},
        max_length=80,
        title='Format Region',
    )
    LastModifiedDateTime: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastModifiedDateTime-53'},
        max_length=8,
        title='Last Modified Date Time',
    )
    LastDateModified: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastDateModified-54'},
        max_length=4,
        title='Last Date Modified',
    )
    ApplicationMethod: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ApplicationMethod-80'},
        title='Application Method',
    )
    PricesIncludingVAT: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PricesIncludingVAT-82'},
        title='Prices Including VAT',
    )
    FaxNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'FaxNo-84'},
        max_length=30,
        title='Fax No.',
    )
    TelexAnswerBack: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TelexAnswerBack-85'},
        max_length=20,
        title='Telex Answer Back',
    )
    VATRegistrationNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATRegistrationNo-86'},
        max_length=20,
        title='VAT Registration No.',
    )
    GenBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenBusPostingGroup-88'},
        max_length=20,
        title='Gen. Bus. Posting Group',
    )
    GLN: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'GLN-90'}, max_length=13, title='GLN'
    )
    PostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostCode-91'},
        max_length=20,
        title='Post Code',
    )
    County: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'County-92'},
        max_length=30,
        title='County',
    )
    EORINumber: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'EORINumber-93'},
        max_length=40,
        title='EORI Number',
    )
    EMail: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'EMail-102'},
        max_length=80,
        title='E-Mail',
    )
    HomePage: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'HomePage-103'},
        max_length=80,
        title='Home Page',
    )
    NoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NoSeries-107'},
        max_length=20,
        title='No. Series',
    )
    TaxAreaCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxAreaCode-108'},
        max_length=20,
        title='Tax Area Code',
    )
    TaxLiable: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'TaxLiable-109'}, title='Tax Liable'
    )
    VATBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATBusPostingGroup-110'},
        max_length=20,
        title='VAT Bus. Posting Group',
    )
    BlockPaymentTolerance: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BlockPaymentTolerance-116'},
        title='Block Payment Tolerance',
    )
    ICPartnerCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ICPartnerCode-119'},
        max_length=20,
        title='IC Partner Code',
    )
    Prepayment: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Prepayment-124'},
        multiple_of=1e-05,
        title='Prepayment %',
    )
    PartnerType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'PartnerType-132'}, title='Partner Type'
    )
    IntrastatPartnerType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'IntrastatPartnerType-133'},
        title='Intrastat Partner Type',
    )
    ExcludefromPmtPractices: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExcludefromPmtPractices-134'},
        title='Exclude from Pmt. Practices',
    )
    CompanySizeCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CompanySizeCode-135'},
        max_length=20,
        title='Company Size Code',
    )
    PrivacyBlocked: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PrivacyBlocked-150'},
        title='Privacy Blocked',
    )
    DisableSearchbyName: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DisableSearchbyName-160'},
        title='Disable Search by Name',
    )
    CreditorNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreditorNo-170'},
        max_length=20,
        title='Creditor No.',
    )
    AllowMultiplePostingGroups: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AllowMultiplePostingGroups-175'},
        title='Allow Multiple Posting Groups',
    )
    PreferredBankAccountCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PreferredBankAccountCode-288'},
        max_length=20,
        title='Preferred Bank Account Code',
    )
    CoupledtoCRM: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CoupledtoCRM-720'},
        title='Coupled to CRM',
    )
    CashFlowPaymentTermsCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CashFlowPaymentTermsCode-840'},
        max_length=10,
        title='Cash Flow Payment Terms Code',
    )
    PrimaryContactNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PrimaryContactNo-5049'},
        max_length=20,
        title='Primary Contact No.',
    )
    MobilePhoneNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'MobilePhoneNo-5061'},
        max_length=30,
        title='Mobile Phone No.',
    )
    ResponsibilityCenter: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ResponsibilityCenter-5700'},
        max_length=10,
        title='Responsibility Center',
    )
    LocationCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LocationCode-5701'},
        max_length=10,
        title='Location Code',
    )
    LeadTimeCalculation: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LeadTimeCalculation-5790'},
        max_length=32,
        title='Lead Time Calculation',
    )
    PriceCalculationMethod: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PriceCalculationMethod-7000'},
        title='Price Calculation Method',
    )
    BaseCalendarCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BaseCalendarCode-7600'},
        max_length=10,
        title='Base Calendar Code',
    )
    DocumentSendingProfile: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentSendingProfile-7601'},
        max_length=20,
        title='Document Sending Profile',
    )
    ValidateEUVatRegNo: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ValidateEUVatRegNo-7602'},
        title='Validate EU Vat Reg. No.',
    )
    CurrencyId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyId-8001'},
        max_length=16,
        title='Currency Id',
    )
    PaymentTermsId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentTermsId-8002'},
        max_length=16,
        title='Payment Terms Id',
    )
    PaymentMethodId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentMethodId-8003'},
        max_length=16,
        title='Payment Method Id',
    )
    OverReceiptCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OverReceiptCode-8510'},
        max_length=20,
        title='Over-Receipt Code',
    )
    PTEMicrosoftafslttur: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEMicrosoftafslttur-65000'},
        multiple_of=1e-05,
        title='PTE Microsoft afsláttur',
    )
    PTEGjaldkeri: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEGjaldkeri-65001'},
        max_length=30,
        title='PTE Gjaldkeri',
    )
    PTESmigjaldkera: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTESmigjaldkera-65002'},
        max_length=30,
        title='PTE Sími gjaldkera',
    )
    PTEHeitikeju: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEHeitikeju-65003'},
        max_length=10,
        title='PTE Heiti keðju',
    )
    PTEAfmrkuntgjaldaflokks: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEAfmrkuntgjaldaflokks-65004'},
        max_length=10,
        title='PTE Afmörkun útgjaldaflokks',
    )
    PTEEkkiuppskrifa: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEEkkiuppskrifa-65005'},
        title='PTE Ekki uppáskrifað',
    )
    PTEPrentalaunamia: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEPrentalaunamia-65006'},
        title='PTE Prenta launamiða',
    )
    PTEjafnaargreislur: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEjafnaargreislur-65007'},
        multiple_of=1e-05,
        title='PTE Ójafnaðar greiðslur',
    )
    PTEEkkigreislutillgu: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEEkkigreislutillgu-65008'},
        title='PTE Ekki í greiðslutillögu',
    )
    PTEContractor: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEContractor-65009'},
        title='PTE Contractor',
    )
    PTESleppagreislutillgu: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTESleppagreislutillgu-65010'},
        title='PTE Sleppa í greiðslutillögu',
    )
    CTRContractor: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CTRContractor-10027575'},
        title='CTR Contractor',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class Customer(SparkModel):
    """
    Represents the table Customer
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-1'}, max_length=20, title='No.'
    )
    Name: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name-2'}, max_length=100, title='Name'
    )
    Address: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Address-5'},
        max_length=100,
        title='Address',
    )
    Address2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Address2-6'},
        max_length=50,
        title='Address 2',
    )
    City: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'City-7'}, max_length=30, title='City'
    )
    Contact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Contact-8'},
        max_length=100,
        title='Contact',
    )
    PhoneNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PhoneNo-9'},
        max_length=30,
        title='Phone No.',
    )
    TelexNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TelexNo-10'},
        max_length=20,
        title='Telex No.',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-16'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-17'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    ChainName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ChainName-18'},
        max_length=10,
        title='Chain Name',
    )
    BudgetedAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BudgetedAmount-19'},
        multiple_of=1e-05,
        title='Budgeted Amount',
    )
    CreditLimitLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreditLimitLCY-20'},
        multiple_of=1e-05,
        title='Credit Limit (LCY)',
    )
    CustomerPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerPostingGroup-21'},
        max_length=20,
        title='Customer Posting Group',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-22'},
        max_length=10,
        title='Currency Code',
    )
    CustomerPriceGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerPriceGroup-23'},
        max_length=10,
        title='Customer Price Group',
    )
    SalespersonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SalespersonCode-29'},
        max_length=20,
        title='Salesperson Code',
    )
    CustomerDiscGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerDiscGroup-34'},
        max_length=20,
        title='Customer Disc. Group',
    )
    CountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CountryRegionCode-35'},
        max_length=10,
        title='Country/Region Code',
    )
    Blocked: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Blocked-39'}, title='Blocked'
    )
    GenBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenBusPostingGroup-88'},
        max_length=20,
        title='Gen. Bus. Posting Group',
    )
    PostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostCode-91'},
        max_length=20,
        title='Post Code',
    )
    County: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'County-92'},
        max_length=30,
        title='County',
    )
    PTEIndustryCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEIndustryCode-65001'},
        max_length=20,
        title='PTE Industry Code',
    )
    PTEGroupCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEGroupCode-65006'},
        max_length=12,
        title='PTE Group Code',
    )
    PTEGroupingCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEGroupingCode-65007'},
        max_length=10,
        title='PTE Grouping Code',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class Job(SparkModel):
    """
    Represents the table Job
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-1'}, max_length=20, title='No.'
    )
    SearchDescription: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SearchDescription-2'},
        max_length=100,
        title='Search Description',
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-3'},
        max_length=100,
        title='Description',
    )
    Description2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description2-4'},
        max_length=50,
        title='Description 2',
    )
    BilltoCustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCustomerNo-5'},
        max_length=20,
        title='Bill-to Customer No.',
    )
    CreationDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreationDate-12'},
        max_length=4,
        title='Creation Date',
    )
    StartingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'StartingDate-13'},
        max_length=4,
        title='Starting Date',
    )
    EndingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'EndingDate-14'},
        max_length=4,
        title='Ending Date',
    )
    Status: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Status-19'}, title='Status'
    )
    PersonResponsible: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PersonResponsible-20'},
        max_length=20,
        title='Person Responsible',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-21'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-22'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    JobPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobPostingGroup-23'},
        max_length=20,
        title='Job Posting Group',
    )
    Blocked: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Blocked-24'}, title='Blocked'
    )
    LastDateModified: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastDateModified-29'},
        max_length=4,
        title='Last Date Modified',
    )
    CustomerDiscGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerDiscGroup-31'},
        max_length=20,
        title='Customer Disc. Group',
    )
    CustomerPriceGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerPriceGroup-32'},
        max_length=10,
        title='Customer Price Group',
    )
    LanguageCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LanguageCode-41'},
        max_length=10,
        title='Language Code',
    )
    BilltoName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoName-58'},
        max_length=100,
        title='Bill-to Name',
    )
    BilltoAddress: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoAddress-59'},
        max_length=100,
        title='Bill-to Address',
    )
    BilltoAddress2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoAddress2-60'},
        max_length=50,
        title='Bill-to Address 2',
    )
    BilltoCity: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCity-61'},
        max_length=30,
        title='Bill-to City',
    )
    BilltoCounty: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCounty-63'},
        max_length=30,
        title='Bill-to County',
    )
    BilltoPostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoPostCode-64'},
        max_length=20,
        title='Bill-to Post Code',
    )
    NoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NoSeries-66'},
        max_length=20,
        title='No. Series',
    )
    BilltoCountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCountryRegionCode-67'},
        max_length=10,
        title='Bill-to Country/Region Code',
    )
    BilltoName2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoName2-68'},
        max_length=50,
        title='Bill-to Name 2',
    )
    Reserve: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Reserve-117'}, title='Reserve'
    )
    WIPMethod: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WIPMethod-1000'},
        max_length=20,
        title='WIP Method',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-1001'},
        max_length=10,
        title='Currency Code',
    )
    BilltoContactNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoContactNo-1002'},
        max_length=20,
        title='Bill-to Contact No.',
    )
    BilltoContact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoContact-1003'},
        max_length=100,
        title='Bill-to Contact',
    )
    WIPPostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WIPPostingDate-1008'},
        max_length=4,
        title='WIP Posting Date',
    )
    InvoiceCurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvoiceCurrencyCode-1011'},
        max_length=10,
        title='Invoice Currency Code',
    )
    ExchCalculationCost: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExchCalculationCost-1012'},
        title='Exch. Calculation (Cost)',
    )
    ExchCalculationPrice: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExchCalculationPrice-1013'},
        title='Exch. Calculation (Price)',
    )
    AllowScheduleContractLines: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AllowScheduleContractLines-1014'},
        title='Allow Schedule/Contract Lines',
    )
    Complete: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Complete-1015'}, title='Complete'
    )
    ApplyUsageLink: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ApplyUsageLink-1025'},
        title='Apply Usage Link',
    )
    WIPPostingMethod: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WIPPostingMethod-1027'},
        title='WIP Posting Method',
    )
    OverBudget: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'OverBudget-1035'}, title='Over Budget'
    )
    ProjectManager: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ProjectManager-1036'},
        max_length=50,
        title='Project Manager',
    )
    SelltoCustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCustomerNo-2000'},
        max_length=20,
        title='Sell-to Customer No.',
    )
    SelltoCustomerName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCustomerName-2001'},
        max_length=100,
        title='Sell-to Customer Name',
    )
    SelltoCustomerName2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCustomerName2-2002'},
        max_length=50,
        title='Sell-to Customer Name 2',
    )
    SelltoAddress: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoAddress-2003'},
        max_length=100,
        title='Sell-to Address',
    )
    SelltoAddress2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoAddress2-2004'},
        max_length=50,
        title='Sell-to Address 2',
    )
    SelltoCity: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCity-2005'},
        max_length=30,
        title='Sell-to City',
    )
    SelltoContact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoContact-2006'},
        max_length=100,
        title='Sell-to Contact',
    )
    SelltoPostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoPostCode-2007'},
        max_length=20,
        title='Sell-to Post Code',
    )
    SelltoCounty: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCounty-2008'},
        max_length=30,
        title='Sell-to County',
    )
    SelltoCountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCountryRegionCode-2009'},
        max_length=10,
        title='Sell-to Country/Region Code',
    )
    SelltoPhoneNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoPhoneNo-2010'},
        max_length=30,
        title='Sell-to Phone No.',
    )
    SelltoEMail: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoEMail-2011'},
        max_length=80,
        title='Sell-to E-Mail',
    )
    SelltoContactNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoContactNo-2012'},
        max_length=20,
        title='Sell-to Contact No.',
    )
    ShiptoCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCode-3000'},
        max_length=10,
        title='Ship-to Code',
    )
    ShiptoName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoName-3001'},
        max_length=100,
        title='Ship-to Name',
    )
    ShiptoName2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoName2-3002'},
        max_length=50,
        title='Ship-to Name 2',
    )
    ShiptoAddress: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoAddress-3003'},
        max_length=100,
        title='Ship-to Address',
    )
    ShiptoAddress2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoAddress2-3004'},
        max_length=50,
        title='Ship-to Address 2',
    )
    ShiptoCity: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCity-3005'},
        max_length=30,
        title='Ship-to City',
    )
    ShiptoContact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoContact-3006'},
        max_length=100,
        title='Ship-to Contact',
    )
    ShiptoPostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoPostCode-3007'},
        max_length=20,
        title='Ship-to Post Code',
    )
    ShiptoCounty: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCounty-3008'},
        max_length=30,
        title='Ship-to County',
    )
    ShiptoCountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCountryRegionCode-3009'},
        max_length=10,
        title='Ship-to Country/Region Code',
    )
    ExternalDocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExternalDocumentNo-4000'},
        max_length=35,
        title='External Document No.',
    )
    PaymentMethodCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentMethodCode-4001'},
        max_length=10,
        title='Payment Method Code',
    )
    PaymentTermsCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentTermsCode-4002'},
        max_length=10,
        title='Payment Terms Code',
    )
    YourReference: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'YourReference-4003'},
        max_length=35,
        title='Your Reference',
    )
    PriceCalculationMethod: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PriceCalculationMethod-7000'},
        title='Price Calculation Method',
    )
    CostCalculationMethod: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CostCalculationMethod-7001'},
        title='Cost Calculation Method',
    )
    PTEGroupCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEGroupCode-65000'},
        max_length=12,
        title='PTE Group Code',
    )
    PTEForbidtoReopenJobReq: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEForbidtoReopenJobReq-65001'},
        title='PTE Forbid to Reopen Job Req.',
    )
    PTETempoID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTETempoID-65002'},
        max_length=30,
        title='PTE Tempo ID',
    )
    PTEJiraPrimaryProjectId: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEJiraPrimaryProjectId-65003'},
        title='PTE Jira Primary Project Id',
    )
    PTETransactionReportssur: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTETransactionReportssur-65004'},
        title='PTE Transaction Report Össur',
    )
    PTECategory: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTECategory-65005'},
        max_length=10,
        title='PTE Category',
    )
    PTEJobRequestRequired: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEJobRequestRequired-65006'},
        title='PTE Job Request Required',
    )
    WJOBNamedEmployees: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBNamedEmployees-10007200'},
        title='WJOBNamedEmployees',
    )
    WJOBRSMReport: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBRSMReport-10007203'},
        title='WJOB RSM Report',
    )
    WJOBRSMReportTable: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBRSMReportTable-10007204'},
        title='WJOB RSM Report Table',
    )
    WJOBTemplate: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBTemplate-10007208'},
        title='WJOB Template',
    )
    WJOBType: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBType-10007209'},
        max_length=15,
        title='WJOB Type',
    )
    WJOBWorkTypeCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBWorkTypeCode-10007211'},
        max_length=10,
        title='WJOB Work Type Code',
    )
    WJOBRSMCombine: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBRSMCombine-10007212'},
        title='WJOB RSM Combine',
    )
    WJOBScheduleisalsoContract: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBScheduleisalsoContract-10007218'},
        title='WJOBSchedule is also Contract',
    )
    WJOBCreateSchedulewhenPoste: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBCreateSchedulewhenPoste-10007219'},
        title='WJOBCreate Schedule when Poste',
    )
    WJOBMaxHoursWarning: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBMaxHoursWarning-10007220'},
        title='WJOB Max. Hours Warning',
    )
    WJOBSupervisor: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBSupervisor-10007221'},
        max_length=20,
        title='WJOB Supervisor',
    )
    WJOBVATBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBVATBusPostingGroup-10007222'},
        max_length=10,
        title='WJOB VAT Bus.Posting Group',
    )
    WJOBJournalsReviewedDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBJournalsReviewedDate-10007223'},
        max_length=4,
        title='WJOB Journals Reviewed Date',
    )
    WJOBCreatedby: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBCreatedby-10007225'},
        max_length=50,
        title='WJOB Created by',
    )
    WJOBLastModifiedby: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBLastModifiedby-10007226'},
        max_length=50,
        title='WJOB Last Modified by',
    )
    WJOBShiptoCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBShiptoCode-10007249'},
        max_length=10,
        title='WJOB Ship-to Code',
    )
    WJobPAInvoiceMaxhours: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPAInvoiceMaxhours-10007251'},
        multiple_of=1e-05,
        title='WJobPA Invoice Max. (hours)',
    )
    WJobPAInvoiceMaxAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPAInvoiceMaxAmount-10007252'},
        multiple_of=1e-05,
        title='WJobPA Invoice Max. Amount',
    )
    WJobPADiscountLimitAmt: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPADiscountLimitAmt-10007253'},
        multiple_of=1e-05,
        title='WJobPA Discount Limit (Amt)',
    )
    WJobPAExcLimitDiscount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPAExcLimitDiscount-10007254'},
        multiple_of=1e-05,
        title='WJobPA Exc. Limit Discount %',
    )
    WJobPAProformaperTask: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPAProformaperTask-10007256'},
        title='WJobPA Proforma per Task',
    )
    WJobPAIndexCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPAIndexCode-10007258'},
        max_length=10,
        title='WJobPA Index Code',
    )
    WJobPAContractRefIndex: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPAContractRefIndex-10007259'},
        multiple_of=1e-05,
        title='WJobPA Contract Ref. Index',
    )
    WJobJRJobRequestsRequired: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobJRJobRequestsRequired-10007270'},
        title='WJobJR Job Requests Required',
    )
    WJOBShowonTransactionReport: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBShowonTransactionReport-10007550'},
        title='WJOBShow on Transaction Report',
    )
    WJOBTransactionGrouping: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJOBTransactionGrouping-10007551'},
        title='WJOB Transaction Grouping',
    )
    WJobPABilltoJobNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'WJobPABilltoJobNo-10027451'},
        max_length=20,
        title='WJobPA Bill-to Job No.',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class SalesInvoiceHeader(SparkModel):
    """
    Represents the table Sales Invoice Header
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    SelltoCustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCustomerNo-2'},
        max_length=20,
        title='Sell-to Customer No.',
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-3'}, max_length=20, title='No.'
    )
    BilltoCustomerNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCustomerNo-4'},
        max_length=20,
        title='Bill-to Customer No.',
    )
    BilltoName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoName-5'},
        max_length=100,
        title='Bill-to Name',
    )
    BilltoName2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoName2-6'},
        max_length=50,
        title='Bill-to Name 2',
    )
    BilltoAddress: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoAddress-7'},
        max_length=100,
        title='Bill-to Address',
    )
    BilltoAddress2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoAddress2-8'},
        max_length=50,
        title='Bill-to Address 2',
    )
    BilltoCity: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCity-9'},
        max_length=30,
        title='Bill-to City',
    )
    BilltoContact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoContact-10'},
        max_length=100,
        title='Bill-to Contact',
    )
    YourReference: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'YourReference-11'},
        max_length=35,
        title='Your Reference',
    )
    ShiptoCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCode-12'},
        max_length=10,
        title='Ship-to Code',
    )
    ShiptoName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoName-13'},
        max_length=100,
        title='Ship-to Name',
    )
    ShiptoName2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoName2-14'},
        max_length=50,
        title='Ship-to Name 2',
    )
    ShiptoAddress: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoAddress-15'},
        max_length=100,
        title='Ship-to Address',
    )
    ShiptoAddress2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoAddress2-16'},
        max_length=50,
        title='Ship-to Address 2',
    )
    ShiptoCity: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCity-17'},
        max_length=30,
        title='Ship-to City',
    )
    ShiptoContact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoContact-18'},
        max_length=100,
        title='Ship-to Contact',
    )
    OrderDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OrderDate-19'},
        max_length=4,
        title='Order Date',
    )
    PostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDate-20'},
        max_length=4,
        title='Posting Date',
    )
    ShipmentDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShipmentDate-21'},
        max_length=4,
        title='Shipment Date',
    )
    PostingDescription: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDescription-22'},
        max_length=100,
        title='Posting Description',
    )
    PaymentTermsCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentTermsCode-23'},
        max_length=10,
        title='Payment Terms Code',
    )
    DueDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DueDate-24'},
        max_length=4,
        title='Due Date',
    )
    PaymentDiscount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentDiscount-25'},
        multiple_of=1e-05,
        title='Payment Discount %',
    )
    PmtDiscountDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PmtDiscountDate-26'},
        max_length=4,
        title='Pmt. Discount Date',
    )
    ShipmentMethodCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShipmentMethodCode-27'},
        max_length=10,
        title='Shipment Method Code',
    )
    LocationCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LocationCode-28'},
        max_length=10,
        title='Location Code',
    )
    ShortcutDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension1Code-29'},
        max_length=20,
        title='Shortcut Dimension 1 Code',
    )
    ShortcutDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension2Code-30'},
        max_length=20,
        title='Shortcut Dimension 2 Code',
    )
    CustomerPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerPostingGroup-31'},
        max_length=20,
        title='Customer Posting Group',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-32'},
        max_length=10,
        title='Currency Code',
    )
    CurrencyFactor: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyFactor-33'},
        multiple_of=1e-05,
        title='Currency Factor',
    )
    CustomerPriceGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerPriceGroup-34'},
        max_length=10,
        title='Customer Price Group',
    )
    PricesIncludingVAT: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PricesIncludingVAT-35'},
        title='Prices Including VAT',
    )
    InvoiceDiscCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvoiceDiscCode-37'},
        max_length=20,
        title='Invoice Disc. Code',
    )
    CustomerDiscGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomerDiscGroup-40'},
        max_length=20,
        title='Customer Disc. Group',
    )
    LanguageCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LanguageCode-41'},
        max_length=10,
        title='Language Code',
    )
    FormatRegion: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'FormatRegion-42'},
        max_length=80,
        title='Format Region',
    )
    SalespersonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SalespersonCode-43'},
        max_length=20,
        title='Salesperson Code',
    )
    OrderNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OrderNo-44'},
        max_length=20,
        title='Order No.',
    )
    NoPrinted: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'NoPrinted-47'}, title='No. Printed'
    )
    OnHold: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OnHold-51'},
        max_length=3,
        title='On Hold',
    )
    AppliestoDocType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoDocType-52'},
        title='Applies-to Doc. Type',
    )
    AppliestoDocNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AppliestoDocNo-53'},
        max_length=20,
        title='Applies-to Doc. No.',
    )
    BalAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BalAccountNo-55'},
        max_length=20,
        title='Bal. Account No.',
    )
    VATRegistrationNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATRegistrationNo-70'},
        max_length=20,
        title='VAT Registration No.',
    )
    ReasonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReasonCode-73'},
        max_length=10,
        title='Reason Code',
    )
    GenBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenBusPostingGroup-74'},
        max_length=20,
        title='Gen. Bus. Posting Group',
    )
    EU3PartyTrade: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'EU3PartyTrade-75'},
        title='EU 3-Party Trade',
    )
    TransactionType: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionType-76'},
        max_length=10,
        title='Transaction Type',
    )
    TransportMethod: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransportMethod-77'},
        max_length=10,
        title='Transport Method',
    )
    VATCountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATCountryRegionCode-78'},
        max_length=10,
        title='VAT Country/Region Code',
    )
    SelltoCustomerName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCustomerName-79'},
        max_length=100,
        title='Sell-to Customer Name',
    )
    SelltoCustomerName2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCustomerName2-80'},
        max_length=50,
        title='Sell-to Customer Name 2',
    )
    SelltoAddress: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoAddress-81'},
        max_length=100,
        title='Sell-to Address',
    )
    SelltoAddress2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoAddress2-82'},
        max_length=50,
        title='Sell-to Address 2',
    )
    SelltoCity: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCity-83'},
        max_length=30,
        title='Sell-to City',
    )
    SelltoContact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoContact-84'},
        max_length=100,
        title='Sell-to Contact',
    )
    BilltoPostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoPostCode-85'},
        max_length=20,
        title='Bill-to Post Code',
    )
    BilltoCounty: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCounty-86'},
        max_length=30,
        title='Bill-to County',
    )
    BilltoCountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoCountryRegionCode-87'},
        max_length=10,
        title='Bill-to Country/Region Code',
    )
    SelltoPostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoPostCode-88'},
        max_length=20,
        title='Sell-to Post Code',
    )
    SelltoCounty: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCounty-89'},
        max_length=30,
        title='Sell-to County',
    )
    SelltoCountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoCountryRegionCode-90'},
        max_length=10,
        title='Sell-to Country/Region Code',
    )
    ShiptoPostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoPostCode-91'},
        max_length=20,
        title='Ship-to Post Code',
    )
    ShiptoCounty: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCounty-92'},
        max_length=30,
        title='Ship-to County',
    )
    ShiptoCountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCountryRegionCode-93'},
        max_length=10,
        title='Ship-to Country/Region Code',
    )
    BalAccountType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BalAccountType-94'},
        title='Bal. Account Type',
    )
    ExitPoint: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExitPoint-97'},
        max_length=10,
        title='Exit Point',
    )
    Correction: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Correction-98'}, title='Correction'
    )
    DocumentDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentDate-99'},
        max_length=4,
        title='Document Date',
    )
    ExternalDocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExternalDocumentNo-100'},
        max_length=35,
        title='External Document No.',
    )
    Area: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Area-101'}, max_length=10, title='Area'
    )
    TransactionSpecification: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionSpecification-102'},
        max_length=10,
        title='Transaction Specification',
    )
    PaymentMethodCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentMethodCode-104'},
        max_length=10,
        title='Payment Method Code',
    )
    ShippingAgentCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShippingAgentCode-105'},
        max_length=10,
        title='Shipping Agent Code',
    )
    PackageTrackingNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PackageTrackingNo-106'},
        max_length=30,
        title='Package Tracking No.',
    )
    PreAssignedNoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PreAssignedNoSeries-107'},
        max_length=20,
        title='Pre-Assigned No. Series',
    )
    NoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NoSeries-108'},
        max_length=20,
        title='No. Series',
    )
    OrderNoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OrderNoSeries-110'},
        max_length=20,
        title='Order No. Series',
    )
    PreAssignedNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PreAssignedNo-111'},
        max_length=20,
        title='Pre-Assigned No.',
    )
    UserID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UserID-112'},
        max_length=50,
        title='User ID',
    )
    SourceCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SourceCode-113'},
        max_length=10,
        title='Source Code',
    )
    TaxAreaCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxAreaCode-114'},
        max_length=20,
        title='Tax Area Code',
    )
    TaxLiable: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'TaxLiable-115'}, title='Tax Liable'
    )
    VATBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATBusPostingGroup-116'},
        max_length=20,
        title='VAT Bus. Posting Group',
    )
    VATBaseDiscount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATBaseDiscount-119'},
        multiple_of=1e-05,
        title='VAT Base Discount %',
    )
    InvoiceDiscountCalculation: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvoiceDiscountCalculation-121'},
        title='Invoice Discount Calculation',
    )
    InvoiceDiscountValue: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InvoiceDiscountValue-122'},
        multiple_of=1e-05,
        title='Invoice Discount Value',
    )
    PrepaymentNoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PrepaymentNoSeries-131'},
        max_length=20,
        title='Prepayment No. Series',
    )
    PrepaymentInvoice: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PrepaymentInvoice-136'},
        title='Prepayment Invoice',
    )
    PrepaymentOrderNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PrepaymentOrderNo-137'},
        max_length=20,
        title='Prepayment Order No.',
    )
    QuoteNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'QuoteNo-151'},
        max_length=20,
        title='Quote No.',
    )
    CompanyBankAccountCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CompanyBankAccountCode-163'},
        max_length=20,
        title='Company Bank Account Code',
    )
    SelltoPhoneNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoPhoneNo-171'},
        max_length=30,
        title='Sell-to Phone No.',
    )
    SelltoEMail: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoEMail-172'},
        max_length=80,
        title='Sell-to E-Mail',
    )
    VATReportingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATReportingDate-179'},
        max_length=4,
        title='VAT Reporting Date',
    )
    PaymentReference: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentReference-180'},
        max_length=50,
        title='Payment Reference',
    )
    DimensionSetID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionSetID-480'},
        title='Dimension Set ID',
    )
    PaymentServiceSetID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentServiceSetID-600'},
        title='Payment Service Set ID',
    )
    DocumentExchangeIdentifier: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentExchangeIdentifier-710'},
        max_length=50,
        title='Document Exchange Identifier',
    )
    DocumentExchangeStatus: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentExchangeStatus-711'},
        title='Document Exchange Status',
    )
    DocExchOriginalIdentifier: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocExchOriginalIdentifier-712'},
        max_length=50,
        title='Doc. Exch. Original Identifier',
    )
    CoupledtoCRM: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CoupledtoCRM-720'},
        title='Coupled to CRM',
    )
    DirectDebitMandateID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DirectDebitMandateID-1200'},
        max_length=35,
        title='Direct Debit Mandate ID',
    )
    CustLedgerEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustLedgerEntryNo-1304'},
        title='Cust. Ledger Entry No.',
    )
    CampaignNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CampaignNo-5050'},
        max_length=20,
        title='Campaign No.',
    )
    SelltoContactNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SelltoContactNo-5052'},
        max_length=20,
        title='Sell-to Contact No.',
    )
    BilltoContactNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BilltoContactNo-5053'},
        max_length=20,
        title='Bill-to Contact No.',
    )
    OpportunityNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OpportunityNo-5055'},
        max_length=20,
        title='Opportunity No.',
    )
    ResponsibilityCenter: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ResponsibilityCenter-5700'},
        max_length=10,
        title='Responsibility Center',
    )
    PriceCalculationMethod: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PriceCalculationMethod-7000'},
        title='Price Calculation Method',
    )
    AllowLineDisc: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AllowLineDisc-7001'},
        title='Allow Line Disc.',
    )
    GetShipmentUsed: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GetShipmentUsed-7200'},
        title='Get Shipment Used',
    )
    DraftInvoiceSystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DraftInvoiceSystemId-8001'},
        max_length=16,
        title='Draft Invoice SystemId',
    )
    ShpfyOrderId: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShpfyOrderId-30100'},
        title='Shpfy Order Id',
    )
    ShpfyOrderNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShpfyOrderNo-30101'},
        max_length=50,
        title='Shpfy Order No.',
    )
    PTENmerverkbeini: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTENmerverkbeini-65100'},
        max_length=10,
        title='PTE Númer verkbeiðni',
    )
    PTEDocumentSendingProfile: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEDocumentSendingProfile-65101'},
        max_length=20,
        title='PTE Document Sending Profile',
    )
    PTEOCRband: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEOCRband-65102'},
        max_length=80,
        title='PTE OCR band',
    )
    PTEBankAccountCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEBankAccountCode-65103'},
        max_length=20,
        title='PTE Bank Account Code',
    )
    PTEUseSaleslines: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTEUseSaleslines-65104'},
        title='PTE Use Saleslines',
    )
    AMSAgreementNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSAgreementNo-10007050'},
        max_length=20,
        title='AMSAgreement No.',
    )
    AMSNextPeriodDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSNextPeriodDate-10007052'},
        max_length=4,
        title='AMSNext Period Date',
    )
    AMSPeriod: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSPeriod-10007053'},
        max_length=4,
        title='AMSPeriod',
    )
    AMSAgreementType: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSAgreementType-10007055'},
        max_length=20,
        title='AMSAgreement Type',
    )
    AMSAgreementPeriodGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSAgreementPeriodGroup-10007056'},
        max_length=20,
        title='AMSAgreement Period Group',
    )
    AMSPeriodEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSPeriodEntryNo-10007059'},
        title='AMSPeriod Entry No.',
    )
    AMSPeriodInvoicingType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSPeriodInvoicingType-10007065'},
        title='AMSPeriod Invoicing Type',
    )
    AMSNotOverageRelated: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSNotOverageRelated-10007066'},
        title='AMSNot Overage Related',
    )
    AMSBillingRunBatchNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AMSBillingRunBatchNo-10007067'},
        title='AMSBilling Run Batch No.',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class GLEntry(SparkModel):
    """
    Represents the table G/L Entry
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    EntryNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryNo-1'}, title='Entry No.'
    )
    GLAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GLAccountNo-3'},
        max_length=20,
        title='G/L Account No.',
    )
    PostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDate-4'},
        max_length=4,
        title='Posting Date',
    )
    DocumentType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DocumentType-5'}, title='Document Type'
    )
    DocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentNo-6'},
        max_length=20,
        title='Document No.',
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-7'},
        max_length=100,
        title='Description',
    )
    BalAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BalAccountNo-10'},
        max_length=20,
        title='Bal. Account No.',
    )
    Amount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Amount-17'},
        multiple_of=1e-05,
        title='Amount',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-23'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-24'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    UserID: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UserID-27'},
        max_length=50,
        title='User ID',
    )
    SourceCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SourceCode-28'},
        max_length=10,
        title='Source Code',
    )
    SystemCreatedEntry: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedEntry-29'},
        title='System-Created Entry',
    )
    PriorYearEntry: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PriorYearEntry-30'},
        title='Prior-Year Entry',
    )
    JobNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobNo-41'},
        max_length=20,
        title='Job No.',
    )
    Quantity: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Quantity-42'},
        multiple_of=1e-05,
        title='Quantity',
    )
    VATAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATAmount-43'},
        multiple_of=1e-05,
        title='VAT Amount',
    )
    BusinessUnitCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BusinessUnitCode-45'},
        max_length=20,
        title='Business Unit Code',
    )
    JournalBatchName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JournalBatchName-46'},
        max_length=10,
        title='Journal Batch Name',
    )
    ReasonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReasonCode-47'},
        max_length=10,
        title='Reason Code',
    )
    GenPostingType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenPostingType-48'},
        title='Gen. Posting Type',
    )
    GenBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenBusPostingGroup-49'},
        max_length=20,
        title='Gen. Bus. Posting Group',
    )
    GenProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenProdPostingGroup-50'},
        max_length=20,
        title='Gen. Prod. Posting Group',
    )
    BalAccountType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BalAccountType-51'},
        title='Bal. Account Type',
    )
    TransactionNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionNo-52'},
        title='Transaction No.',
    )
    DebitAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DebitAmount-53'},
        multiple_of=1e-05,
        title='Debit Amount',
    )
    CreditAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreditAmount-54'},
        multiple_of=1e-05,
        title='Credit Amount',
    )
    DocumentDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentDate-55'},
        max_length=4,
        title='Document Date',
    )
    ExternalDocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExternalDocumentNo-56'},
        max_length=35,
        title='External Document No.',
    )
    SourceType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'SourceType-57'}, title='Source Type'
    )
    SourceNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SourceNo-58'},
        max_length=20,
        title='Source No.',
    )
    NoSeries: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NoSeries-59'},
        max_length=20,
        title='No. Series',
    )
    TaxAreaCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxAreaCode-60'},
        max_length=20,
        title='Tax Area Code',
    )
    TaxLiable: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'TaxLiable-61'}, title='Tax Liable'
    )
    TaxGroupCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxGroupCode-62'},
        max_length=20,
        title='Tax Group Code',
    )
    UseTax: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'UseTax-63'}, title='Use Tax'
    )
    VATBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATBusPostingGroup-64'},
        max_length=20,
        title='VAT Bus. Posting Group',
    )
    VATProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATProdPostingGroup-65'},
        max_length=20,
        title='VAT Prod. Posting Group',
    )
    AdditionalCurrencyAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AdditionalCurrencyAmount-68'},
        multiple_of=1e-05,
        title='Additional-Currency Amount',
    )
    AddCurrencyDebitAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AddCurrencyDebitAmount-69'},
        multiple_of=1e-05,
        title='Add.-Currency Debit Amount',
    )
    AddCurrencyCreditAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AddCurrencyCreditAmount-70'},
        multiple_of=1e-05,
        title='Add.-Currency Credit Amount',
    )
    CloseIncomeStatementDimID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CloseIncomeStatementDimID-71'},
        title='Close Income Statement Dim. ID',
    )
    ICPartnerCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ICPartnerCode-72'},
        max_length=20,
        title='IC Partner Code',
    )
    Reversed: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Reversed-73'}, title='Reversed'
    )
    ReversedbyEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReversedbyEntryNo-74'},
        title='Reversed by Entry No.',
    )
    ReversedEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReversedEntryNo-75'},
        title='Reversed Entry No.',
    )
    JournalTemplName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JournalTemplName-78'},
        max_length=10,
        title='Journal Templ. Name',
    )
    VATReportingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATReportingDate-79'},
        max_length=4,
        title='VAT Reporting Date',
    )
    DimensionSetID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionSetID-480'},
        title='Dimension Set ID',
    )
    LastDimCorrectionEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastDimCorrectionEntryNo-495'},
        title='Last Dim. Correction Entry No.',
    )
    LastDimCorrectionNode: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastDimCorrectionNode-496'},
        title='Last Dim. Correction Node',
    )
    DimensionChangesCount: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionChangesCount-497'},
        title='Dimension Changes Count',
    )
    AllocationAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AllocationAccountNo-2678'},
        max_length=20,
        title='Allocation Account No.',
    )
    ProdOrderNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ProdOrderNo-5400'},
        max_length=20,
        title='Prod. Order No.',
    )
    FAEntryType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'FAEntryType-5600'},
        title='FA Entry Type',
    )
    FAEntryNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'FAEntryNo-5601'}, title='FA Entry No.'
    )
    Comment: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Comment-5618'},
        max_length=250,
        title='Comment',
    )
    NonDeductibleVATAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NonDeductibleVATAmount-6200'},
        multiple_of=1e-05,
        title='Non-Deductible VAT Amount',
    )
    NonDeductibleVATAmountACY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NonDeductibleVATAmountACY-6201'},
        multiple_of=1e-05,
        title='Non-Deductible VAT Amount ACY',
    )
    LastModifiedDateTime: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastModifiedDateTime-8005'},
        max_length=8,
        title='Last Modified DateTime',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class Resource(SparkModel):
    """
    Represents the table Resource
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-1'}, max_length=20, title='No.'
    )
    Type: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Type-2'}, title='Type'
    )
    Name: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name-3'}, max_length=100, title='Name'
    )
    Name2: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name2-5'}, max_length=50, title='Name 2'
    )
    Address: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Address-6'},
        max_length=100,
        title='Address',
    )
    Address2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Address2-7'},
        max_length=50,
        title='Address 2',
    )
    City: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'City-8'}, max_length=30, title='City'
    )
    SocialSecurityNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SocialSecurityNo-9'},
        max_length=30,
        title='Social Security No.',
    )
    JobTitle: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JobTitle-10'},
        max_length=30,
        title='Job Title',
    )
    Education: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Education-11'},
        max_length=30,
        title='Education',
    )
    EmploymentDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'EmploymentDate-13'},
        max_length=4,
        title='Employment Date',
    )
    ResourceGroupNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ResourceGroupNo-14'},
        max_length=20,
        title='Resource Group No.',
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-16'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-17'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    BaseUnitofMeasure: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BaseUnitofMeasure-18'},
        max_length=10,
        title='Base Unit of Measure',
    )
    DirectUnitCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DirectUnitCost-19'},
        multiple_of=1e-05,
        title='Direct Unit Cost',
    )
    UnitCost: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitCost-21'},
        multiple_of=1e-05,
        title='Unit Cost',
    )
    UnitPrice: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UnitPrice-24'},
        multiple_of=1e-05,
        title='Unit Price',
    )
    VendorNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorNo-25'},
        max_length=20,
        title='Vendor No.',
    )
    Blocked: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Blocked-38'}, title='Blocked'
    )
    GenProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenProdPostingGroup-51'},
        max_length=20,
        title='Gen. Prod. Posting Group',
    )
    PostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostCode-53'},
        max_length=20,
        title='Post Code',
    )
    County: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'County-54'},
        max_length=30,
        title='County',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class CompanyInformation(SparkModel):
    """
    Represents the table Company Information
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    PrimaryKey: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PrimaryKey-1'},
        max_length=10,
        title='Primary Key',
    )
    Name: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name-2'}, max_length=100, title='Name'
    )
    Name2: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name2-3'}, max_length=50, title='Name 2'
    )
    Address: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Address-4'},
        max_length=100,
        title='Address',
    )
    Address2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Address2-5'},
        max_length=50,
        title='Address 2',
    )
    City: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'City-6'}, max_length=30, title='City'
    )
    PhoneNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PhoneNo-7'},
        max_length=30,
        title='Phone No.',
    )
    PhoneNo2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PhoneNo2-8'},
        max_length=30,
        title='Phone No. 2',
    )
    TelexNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TelexNo-9'},
        max_length=30,
        title='Telex No.',
    )
    FaxNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'FaxNo-10'},
        max_length=30,
        title='Fax No.',
    )
    GiroNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GiroNo-11'},
        max_length=20,
        title='Giro No.',
    )
    BankName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BankName-12'},
        max_length=100,
        title='Bank Name',
    )
    BankBranchNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BankBranchNo-13'},
        max_length=20,
        title='Bank Branch No.',
    )
    BankAccountNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BankAccountNo-14'},
        max_length=30,
        title='Bank Account No.',
    )
    PaymentRoutingNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PaymentRoutingNo-15'},
        max_length=20,
        title='Payment Routing No.',
    )
    CustomsPermitNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomsPermitNo-17'},
        max_length=10,
        title='Customs Permit No.',
    )
    CustomsPermitDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomsPermitDate-18'},
        max_length=4,
        title='Customs Permit Date',
    )
    VATRegistrationNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATRegistrationNo-19'},
        max_length=20,
        title='VAT Registration No.',
    )
    RegistrationNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'RegistrationNo-20'},
        max_length=20,
        title='Registration No.',
    )
    TelexAnswerBack: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TelexAnswerBack-21'},
        max_length=20,
        title='Telex Answer Back',
    )
    ShiptoName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoName-22'},
        max_length=100,
        title='Ship-to Name',
    )
    ShiptoName2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoName2-23'},
        max_length=50,
        title='Ship-to Name 2',
    )
    ShiptoAddress: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoAddress-24'},
        max_length=100,
        title='Ship-to Address',
    )
    ShiptoAddress2: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoAddress2-25'},
        max_length=50,
        title='Ship-to Address 2',
    )
    ShiptoCity: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCity-26'},
        max_length=30,
        title='Ship-to City',
    )
    ShiptoContact: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoContact-27'},
        max_length=100,
        title='Ship-to Contact',
    )
    LocationCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LocationCode-28'},
        max_length=10,
        title='Location Code',
    )
    PostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostCode-30'},
        max_length=20,
        title='Post Code',
    )
    County: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'County-31'},
        max_length=30,
        title='County',
    )
    ShiptoPostCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoPostCode-32'},
        max_length=20,
        title='Ship-to Post Code',
    )
    ShiptoCounty: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCounty-33'},
        max_length=30,
        title='Ship-to County',
    )
    EMail: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'EMail-34'},
        max_length=80,
        title='E-Mail',
    )
    HomePage: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'HomePage-35'},
        max_length=80,
        title='Home Page',
    )
    CountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CountryRegionCode-36'},
        max_length=10,
        title='Country/Region Code',
    )
    ShiptoCountryRegionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShiptoCountryRegionCode-37'},
        max_length=10,
        title='Ship-to Country/Region Code',
    )
    IBAN: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'IBAN-38'}, max_length=50, title='IBAN'
    )
    SWIFTCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SWIFTCode-39'},
        max_length=20,
        title='SWIFT Code',
    )
    IndustrialClassification: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'IndustrialClassification-40'},
        max_length=30,
        title='Industrial Classification',
    )
    SystemIndicator: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemIndicator-46'},
        title='System Indicator',
    )
    CustomSystemIndicatorText: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustomSystemIndicatorText-47'},
        max_length=250,
        title='Custom System Indicator Text',
    )
    SystemIndicatorStyle: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemIndicatorStyle-48'},
        title='System Indicator Style',
    )
    AllowBlankPaymentInfo: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AllowBlankPaymentInfo-50'},
        title='Allow Blank Payment Info.',
    )
    ContactPerson: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ContactPerson-51'},
        max_length=50,
        title='Contact Person',
    )
    GLN: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'GLN-90'}, max_length=13, title='GLN'
    )
    EORINumber: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'EORINumber-92'},
        max_length=40,
        title='EORI Number',
    )
    UseGLNinElectronicDocument: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'UseGLNinElectronicDocument-95'},
        title='Use GLN in Electronic Document',
    )
    PictureLastModDateTime: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PictureLastModDateTime-96'},
        max_length=8,
        title='Picture - Last Mod. Date Time',
    )
    LastModifiedDateTime: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastModifiedDateTime-98'},
        max_length=8,
        title='Last Modified Date Time',
    )
    CreatedDateTime: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreatedDateTime-99'},
        max_length=8,
        title='Created DateTime',
    )
    DemoCompany: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DemoCompany-100'}, title='Demo Company'
    )
    AlternativeLanguageCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AlternativeLanguageCode-200'},
        max_length=10,
        title='Alternative Language Code',
    )
    BrandColorValue: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BrandColorValue-300'},
        max_length=10,
        title='Brand Color Value',
    )
    BrandColorCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BrandColorCode-301'},
        max_length=20,
        title='Brand Color Code',
    )
    ResponsibilityCenter: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ResponsibilityCenter-5700'},
        max_length=10,
        title='Responsibility Center',
    )
    CheckAvailPeriodCalc: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CheckAvailPeriodCalc-5791'},
        max_length=32,
        title='Check-Avail. Period Calc.',
    )
    CheckAvailTimeBucket: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CheckAvailTimeBucket-5792'},
        title='Check-Avail. Time Bucket',
    )
    BaseCalendarCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'BaseCalendarCode-7600'},
        max_length=10,
        title='Base Calendar Code',
    )
    CalConvergenceTimeFrame: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CalConvergenceTimeFrame-7601'},
        max_length=32,
        title='Cal. Convergence Time Frame',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class AccountingPeriod(SparkModel):
    """
    Represents the table Accounting Period
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    StartingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'StartingDate-1'},
        max_length=4,
        title='Starting Date',
    )
    Name: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name-2'}, max_length=10, title='Name'
    )
    NewFiscalYear: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NewFiscalYear-3'},
        title='New Fiscal Year',
    )
    Closed: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Closed-4'}, title='Closed'
    )
    DateLocked: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DateLocked-5'}, title='Date Locked'
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class GLAccount(SparkModel):
    """
    Represents the table G/L Account
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    No: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No-1'}, max_length=20, title='No.'
    )
    Name: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name-2'}, max_length=100, title='Name'
    )
    SearchName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SearchName-3'},
        max_length=100,
        title='Search Name',
    )
    AccountType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'AccountType-4'}, title='Account Type'
    )
    GlobalDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension1Code-6'},
        max_length=20,
        title='Global Dimension 1 Code',
    )
    GlobalDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimension2Code-7'},
        max_length=20,
        title='Global Dimension 2 Code',
    )
    AccountCategory: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AccountCategory-8'},
        title='Account Category',
    )
    IncomeBalance: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'IncomeBalance-9'},
        title='Income/Balance',
    )
    DebitCredit: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DebitCredit-10'}, title='Debit/Credit'
    )
    No2: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'No2-11'}, max_length=20, title='No. 2'
    )
    Blocked: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Blocked-13'}, title='Blocked'
    )
    DirectPosting: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DirectPosting-14'},
        title='Direct Posting',
    )
    ReconciliationAccount: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReconciliationAccount-16'},
        title='Reconciliation Account',
    )
    NewPage: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'NewPage-17'}, title='New Page'
    )
    NoofBlankLines: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'NoofBlankLines-18'},
        title='No. of Blank Lines',
    )
    Indentation: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Indentation-19'}, title='Indentation'
    )
    LastModifiedDateTime: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastModifiedDateTime-25'},
        max_length=8,
        title='Last Modified Date Time',
    )
    LastDateModified: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LastDateModified-26'},
        max_length=4,
        title='Last Date Modified',
    )
    Totaling: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Totaling-34'},
        max_length=250,
        title='Totaling',
    )
    ConsolTranslationMethod: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ConsolTranslationMethod-39'},
        title='Consol. Translation Method',
    )
    ConsolDebitAcc: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ConsolDebitAcc-40'},
        max_length=20,
        title='Consol. Debit Acc.',
    )
    ConsolCreditAcc: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ConsolCreditAcc-41'},
        max_length=20,
        title='Consol. Credit Acc.',
    )
    GenPostingType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenPostingType-43'},
        title='Gen. Posting Type',
    )
    GenBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenBusPostingGroup-44'},
        max_length=20,
        title='Gen. Bus. Posting Group',
    )
    GenProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GenProdPostingGroup-45'},
        max_length=20,
        title='Gen. Prod. Posting Group',
    )
    AutomaticExtTexts: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AutomaticExtTexts-49'},
        title='Automatic Ext. Texts',
    )
    TaxAreaCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxAreaCode-54'},
        max_length=20,
        title='Tax Area Code',
    )
    TaxLiable: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'TaxLiable-55'}, title='Tax Liable'
    )
    TaxGroupCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TaxGroupCode-56'},
        max_length=20,
        title='Tax Group Code',
    )
    VATBusPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATBusPostingGroup-57'},
        max_length=20,
        title='VAT Bus. Posting Group',
    )
    VATProdPostingGroup: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VATProdPostingGroup-58'},
        max_length=20,
        title='VAT Prod. Posting Group',
    )
    ExchangeRateAdjustment: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ExchangeRateAdjustment-63'},
        title='Exchange Rate Adjustment',
    )
    DefaultICPartnerGLAccNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DefaultICPartnerGLAccNo-66'},
        max_length=20,
        title='Default IC Partner G/L Acc. No',
    )
    OmitDefaultDescrinJnl: bool | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'OmitDefaultDescrinJnl-70'},
        title='Omit Default Descr. in Jnl.',
    )
    AccountSubcategoryEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AccountSubcategoryEntryNo-80'},
        title='Account Subcategory Entry No.',
    )
    CostTypeNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CostTypeNo-1100'},
        max_length=20,
        title='Cost Type No.',
    )
    DefaultDeferralTemplateCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DefaultDeferralTemplateCode-1700'},
        max_length=10,
        title='Default Deferral Template Code',
    )
    APIAccountType: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'APIAccountType-9000'},
        title='API Account Type',
    )
    IRSNumber: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'IRSNumber-10900'},
        max_length=10,
        title='IRS Number',
    )
    IRSNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'IRSNo-14602'},
        max_length=10,
        title='IRS No.',
    )
    ReviewPolicy: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReviewPolicy-22200'},
        title='Review Policy',
    )
    PTENewNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PTENewNo-65000'},
        max_length=20,
        title='PTE New No.',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class DetailedVendorLedgEntry(SparkModel):
    """
    Represents the table Detailed Vendor Ledg. Entry
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    EntryNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryNo-1'}, title='Entry No.'
    )
    VendorLedgerEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorLedgerEntryNo-2'},
        title='Vendor Ledger Entry No.',
    )
    EntryType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryType-3'}, title='Entry Type'
    )
    PostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDate-4'},
        max_length=4,
        title='Posting Date',
    )
    DocumentType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DocumentType-5'}, title='Document Type'
    )
    DocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentNo-6'},
        max_length=20,
        title='Document No.',
    )
    Amount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Amount-7'},
        multiple_of=1e-05,
        title='Amount',
    )
    AmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AmountLCY-8'},
        multiple_of=1e-05,
        title='Amount (LCY)',
    )
    VendorNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'VendorNo-9'},
        max_length=20,
        title='Vendor No.',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-10'},
        max_length=10,
        title='Currency Code',
    )
    SourceCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SourceCode-12'},
        max_length=10,
        title='Source Code',
    )
    TransactionNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'TransactionNo-13'},
        title='Transaction No.',
    )
    JournalBatchName: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'JournalBatchName-14'},
        max_length=10,
        title='Journal Batch Name',
    )
    ReasonCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ReasonCode-15'},
        max_length=10,
        title='Reason Code',
    )
    DebitAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DebitAmount-16'},
        multiple_of=1e-05,
        title='Debit Amount',
    )
    CreditAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreditAmount-17'},
        multiple_of=1e-05,
        title='Credit Amount',
    )
    DebitAmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DebitAmountLCY-18'},
        multiple_of=1e-05,
        title='Debit Amount (LCY)',
    )
    CreditAmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreditAmountLCY-19'},
        multiple_of=1e-05,
        title='Credit Amount (LCY)',
    )
    InitialEntryDueDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'InitialEntryDueDate-20'},
        max_length=4,
        title='Initial Entry Due Date',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class Dimension(SparkModel):
    """
    Represents the table Dimension
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    Code: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Code-1'}, max_length=20, title='Code'
    )
    Name: str | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Name-2'}, max_length=30, title='Name'
    )
    CodeCaption: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CodeCaption-3'},
        max_length=80,
        title='Code Caption',
    )
    FilterCaption: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'FilterCaption-4'},
        max_length=80,
        title='Filter Caption',
    )
    Description: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Description-5'},
        max_length=100,
        title='Description',
    )
    Blocked: bool | None = Field(
        None, json_schema_extra={'x-cdm-name': 'Blocked-6'}, title='Blocked'
    )
    ConsolidationCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ConsolidationCode-7'},
        max_length=20,
        title='Consolidation Code',
    )
    MaptoICDimensionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'MaptoICDimensionCode-8'},
        max_length=20,
        title='Map-to IC Dimension Code',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class DimensionSetEntry(SparkModel):
    """
    Represents the table Dimension Set Entry
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    DimensionSetID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionSetID-1'},
        title='Dimension Set ID',
    )
    DimensionCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionCode-2'},
        max_length=20,
        title='Dimension Code',
    )
    DimensionValueCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionValueCode-3'},
        max_length=20,
        title='Dimension Value Code',
    )
    DimensionValueID: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DimensionValueID-4'},
        title='Dimension Value ID',
    )
    GlobalDimensionNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'GlobalDimensionNo-8'},
        title='Global Dimension No.',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class GeneralLedgerSetup(SparkModel):
    """
    Represents the table General Ledger Setup
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    AdditionalReportingCurrency: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AdditionalReportingCurrency-68'},
        max_length=10,
        title='Additional Reporting Currency',
    )
    LCYCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'LCYCode-71'},
        max_length=10,
        title='LCY Code',
    )
    ShortcutDimension1Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension1Code-81'},
        max_length=20,
        title='Shortcut Dimension 1 Code',
    )
    ShortcutDimension2Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension2Code-82'},
        max_length=20,
        title='Shortcut Dimension 2 Code',
    )
    ShortcutDimension3Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension3Code-83'},
        max_length=20,
        title='Shortcut Dimension 3 Code',
    )
    ShortcutDimension4Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension4Code-84'},
        max_length=20,
        title='Shortcut Dimension 4 Code',
    )
    ShortcutDimension5Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension5Code-85'},
        max_length=20,
        title='Shortcut Dimension 5 Code',
    )
    ShortcutDimension6Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension6Code-86'},
        max_length=20,
        title='Shortcut Dimension 6 Code',
    )
    ShortcutDimension7Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension7Code-87'},
        max_length=20,
        title='Shortcut Dimension 7 Code',
    )
    ShortcutDimension8Code: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'ShortcutDimension8Code-88'},
        max_length=20,
        title='Shortcut Dimension 8 Code',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


class DetailedCustLedgEntry(SparkModel):
    """
    Represents the table Detailed Cust. Ledg. Entry
    """

    model_config = ConfigDict(
        extra='forbid',
    )
    EntryNo: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryNo-1'}, title='Entry No.'
    )
    CustLedgerEntryNo: int | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CustLedgerEntryNo-2'},
        title='Cust. Ledger Entry No.',
    )
    EntryType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'EntryType-3'}, title='Entry Type'
    )
    PostingDate: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'PostingDate-4'},
        max_length=4,
        title='Posting Date',
    )
    DocumentType: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'DocumentType-5'}, title='Document Type'
    )
    DocumentNo: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DocumentNo-6'},
        max_length=20,
        title='Document No.',
    )
    Amount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'Amount-7'},
        multiple_of=1e-05,
        title='Amount',
    )
    AmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'AmountLCY-8'},
        multiple_of=1e-05,
        title='Amount (LCY)',
    )
    CurrencyCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CurrencyCode-10'},
        max_length=10,
        title='Currency Code',
    )
    SourceCode: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SourceCode-12'},
        max_length=10,
        title='Source Code',
    )
    DebitAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DebitAmount-16'},
        multiple_of=1e-05,
        title='Debit Amount',
    )
    CreditAmount: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreditAmount-17'},
        multiple_of=1e-05,
        title='Credit Amount',
    )
    DebitAmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'DebitAmountLCY-18'},
        multiple_of=1e-05,
        title='Debit Amount (LCY)',
    )
    CreditAmountLCY: float | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'CreditAmountLCY-19'},
        multiple_of=1e-05,
        title='Credit Amount (LCY)',
    )
    timestamp: int | None = Field(
        None, json_schema_extra={'x-cdm-name': 'timestamp-0'}, title='timestamp'
    )
    SystemId: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'systemId-2000000000'},
        max_length=16,
        title='$systemId',
    )
    SystemCreatedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedAt-2000000001'},
        max_length=8,
        title='SystemCreatedAt',
    )
    SystemCreatedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemCreatedBy-2000000002'},
        max_length=16,
        title='SystemCreatedBy',
    )
    SystemModifiedAt: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedAt-2000000003'},
        max_length=8,
        title='SystemModifiedAt',
    )
    SystemModifiedBy: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': 'SystemModifiedBy-2000000004'},
        max_length=16,
        title='SystemModifiedBy',
    )
    Company: str | None = Field(
        None,
        json_schema_extra={'x-cdm-name': '$Company'},
        max_length=16,
        title='$Company',
    )


