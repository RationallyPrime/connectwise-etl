"""
CDM models generated from CDM JSON schema files.

Compatible with Pydantic v2 and SparkDantic for Spark schema generation.
"""

from __future__ import annotations

import datetime

from pydantic import Field
from sparkdantic import SparkModel


class AccountingPeriod(SparkModel):
    """Represents the table Accounting Period

    Display Name: Accounting Period
    Entity Name: AccountingPeriod-50
    """

    StartingDate: datetime.date | None = Field(
        default=None, alias="StartingDate-1", description="Starting Date. Max length: 4"
    )
    Name: str | None = Field(default=None, alias="Name-2", description="Name. Max length: 10")
    NewFiscalYear: bool | None = Field(
        default=None, alias="NewFiscalYear-3", description="New Fiscal Year. Max length: 4"
    )
    Closed: bool | None = Field(default=None, alias="Closed-4", description="Closed. Max length: 4")
    DateLocked: bool | None = Field(
        default=None, alias="DateLocked-5", description="Date Locked. Max length: 4"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class CompanyInformation(SparkModel):
    """Represents the table Company Information

    Display Name: Company Information
    Entity Name: CompanyInformation-79
    """

    PrimaryKey: str | None = Field(
        default=None, alias="PrimaryKey-1", description="Primary Key. Max length: 10"
    )
    Name: str | None = Field(default=None, alias="Name-2", description="Name. Max length: 100")
    Name2: str | None = Field(default=None, alias="Name2-3", description="Name 2. Max length: 50")
    Address: str | None = Field(
        default=None, alias="Address-4", description="Address. Max length: 100"
    )
    Address2: str | None = Field(
        default=None, alias="Address2-5", description="Address 2. Max length: 50"
    )
    City: str | None = Field(default=None, alias="City-6", description="City. Max length: 30")
    PhoneNo: str | None = Field(
        default=None, alias="PhoneNo-7", description="Phone No.. Max length: 30"
    )
    PhoneNo2: str | None = Field(
        default=None, alias="PhoneNo2-8", description="Phone No. 2. Max length: 30"
    )
    TelexNo: str | None = Field(
        default=None, alias="TelexNo-9", description="Telex No.. Max length: 30"
    )
    FaxNo: str | None = Field(default=None, alias="FaxNo-10", description="Fax No.. Max length: 30")
    GiroNo: str | None = Field(
        default=None, alias="GiroNo-11", description="Giro No.. Max length: 20"
    )
    BankName: str | None = Field(
        default=None, alias="BankName-12", description="Bank Name. Max length: 100"
    )
    BankBranchNo: str | None = Field(
        default=None, alias="BankBranchNo-13", description="Bank Branch No.. Max length: 20"
    )
    BankAccountNo: str | None = Field(
        default=None, alias="BankAccountNo-14", description="Bank Account No.. Max length: 30"
    )
    PaymentRoutingNo: str | None = Field(
        default=None, alias="PaymentRoutingNo-15", description="Payment Routing No.. Max length: 20"
    )
    CustomsPermitNo: str | None = Field(
        default=None, alias="CustomsPermitNo-17", description="Customs Permit No.. Max length: 10"
    )
    CustomsPermitDate: datetime.date | None = Field(
        default=None, alias="CustomsPermitDate-18", description="Customs Permit Date. Max length: 4"
    )
    VATRegistrationNo: str | None = Field(
        default=None,
        alias="VATRegistrationNo-19",
        description="VAT Registration No.. Max length: 20",
    )
    RegistrationNo: str | None = Field(
        default=None, alias="RegistrationNo-20", description="Registration No.. Max length: 20"
    )
    TelexAnswerBack: str | None = Field(
        default=None, alias="TelexAnswerBack-21", description="Telex Answer Back. Max length: 20"
    )
    ShiptoName: str | None = Field(
        default=None, alias="ShiptoName-22", description="Ship-to Name. Max length: 100"
    )
    ShiptoName2: str | None = Field(
        default=None, alias="ShiptoName2-23", description="Ship-to Name 2. Max length: 50"
    )
    ShiptoAddress: str | None = Field(
        default=None, alias="ShiptoAddress-24", description="Ship-to Address. Max length: 100"
    )
    ShiptoAddress2: str | None = Field(
        default=None, alias="ShiptoAddress2-25", description="Ship-to Address 2. Max length: 50"
    )
    ShiptoCity: str | None = Field(
        default=None, alias="ShiptoCity-26", description="Ship-to City. Max length: 30"
    )
    ShiptoContact: str | None = Field(
        default=None, alias="ShiptoContact-27", description="Ship-to Contact. Max length: 100"
    )
    LocationCode: str | None = Field(
        default=None, alias="LocationCode-28", description="Location Code. Max length: 10"
    )
    PostCode: str | None = Field(
        default=None, alias="PostCode-30", description="Post Code. Max length: 20"
    )
    County: str | None = Field(
        default=None, alias="County-31", description="County. Max length: 30"
    )
    ShiptoPostCode: str | None = Field(
        default=None, alias="ShiptoPostCode-32", description="Ship-to Post Code. Max length: 20"
    )
    ShiptoCounty: str | None = Field(
        default=None, alias="ShiptoCounty-33", description="Ship-to County. Max length: 30"
    )
    EMail: str | None = Field(default=None, alias="EMail-34", description="E-Mail. Max length: 80")
    HomePage: str | None = Field(
        default=None, alias="HomePage-35", description="Home Page. Max length: 80"
    )
    CountryRegionCode: str | None = Field(
        default=None,
        alias="CountryRegionCode-36",
        description="Country/Region Code. Max length: 10",
    )
    ShiptoCountryRegionCode: str | None = Field(
        default=None,
        alias="ShiptoCountryRegionCode-37",
        description="Ship-to Country/Region Code. Max length: 10",
    )
    IBAN: str | None = Field(default=None, alias="IBAN-38", description="IBAN. Max length: 50")
    SWIFTCode: str | None = Field(
        default=None, alias="SWIFTCode-39", description="SWIFT Code. Max length: 20"
    )
    IndustrialClassification: str | None = Field(
        default=None,
        alias="IndustrialClassification-40",
        description="Industrial Classification. Max length: 30",
    )
    SystemIndicator: int | None = Field(
        default=None, alias="SystemIndicator-46", description="System Indicator. Max length: 4"
    )
    CustomSystemIndicatorText: str | None = Field(
        default=None,
        alias="CustomSystemIndicatorText-47",
        description="Custom System Indicator Text. Max length: 250",
    )
    SystemIndicatorStyle: int | None = Field(
        default=None,
        alias="SystemIndicatorStyle-48",
        description="System Indicator Style. Max length: 4",
    )
    AllowBlankPaymentInfo: bool | None = Field(
        default=None,
        alias="AllowBlankPaymentInfo-50",
        description="Allow Blank Payment Info.. Max length: 4",
    )
    ContactPerson: str | None = Field(
        default=None, alias="ContactPerson-51", description="Contact Person. Max length: 50"
    )
    GLN: str | None = Field(default=None, alias="GLN-90", description="GLN. Max length: 13")
    EORINumber: str | None = Field(
        default=None, alias="EORINumber-92", description="EORI Number. Max length: 40"
    )
    UseGLNinElectronicDocument: bool | None = Field(
        default=None,
        alias="UseGLNinElectronicDocument-95",
        description="Use GLN in Electronic Document. Max length: 4",
    )
    PictureLastModDateTime: datetime.datetime | None = Field(
        default=None,
        alias="PictureLastModDateTime-96",
        description="Picture - Last Mod. Date Time. Max length: 8",
    )
    LastModifiedDateTime: datetime.datetime | None = Field(
        default=None,
        alias="LastModifiedDateTime-98",
        description="Last Modified Date Time. Max length: 8",
    )
    CreatedDateTime: datetime.datetime | None = Field(
        default=None, alias="CreatedDateTime-99", description="Created DateTime. Max length: 8"
    )
    DemoCompany: bool | None = Field(
        default=None, alias="DemoCompany-100", description="Demo Company. Max length: 4"
    )
    AlternativeLanguageCode: str | None = Field(
        default=None,
        alias="AlternativeLanguageCode-200",
        description="Alternative Language Code. Max length: 10",
    )
    BrandColorValue: str | None = Field(
        default=None, alias="BrandColorValue-300", description="Brand Color Value. Max length: 10"
    )
    BrandColorCode: str | None = Field(
        default=None, alias="BrandColorCode-301", description="Brand Color Code. Max length: 20"
    )
    ResponsibilityCenter: str | None = Field(
        default=None,
        alias="ResponsibilityCenter-5700",
        description="Responsibility Center. Max length: 10",
    )
    CheckAvailPeriodCalc: str | None = Field(
        default=None,
        alias="CheckAvailPeriodCalc-5791",
        description="Check-Avail. Period Calc.. Max length: 32",
    )
    CheckAvailTimeBucket: int | None = Field(
        default=None,
        alias="CheckAvailTimeBucket-5792",
        description="Check-Avail. Time Bucket. Max length: 4",
    )
    BaseCalendarCode: str | None = Field(
        default=None,
        alias="BaseCalendarCode-7600",
        description="Base Calendar Code. Max length: 10",
    )
    CalConvergenceTimeFrame: str | None = Field(
        default=None,
        alias="CalConvergenceTimeFrame-7601",
        description="Cal. Convergence Time Frame. Max length: 32",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class Currency(SparkModel):
    """Represents the table Currency

    Display Name: Currency
    Entity Name: Currency-4
    """

    Code: str | None = Field(default=None, alias="Code-1", description="Code. Max length: 10")
    ISOCode: str | None = Field(
        default=None, alias="ISOCode-4", description="ISO Code. Max length: 3"
    )
    Description: str | None = Field(
        default=None, alias="Description-15", description="Description. Max length: 30"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class CustLedgerEntry(SparkModel):
    """Represents the table Cust. Ledger Entry

    Display Name: Cust. Ledger Entry
    Entity Name: CustLedgerEntry-21
    """

    EntryNo: int | None = Field(
        default=None, alias="EntryNo-1", description="Entry No.. Max length: 4"
    )
    CustomerNo: str | None = Field(
        default=None, alias="CustomerNo-3", description="Customer No.. Max length: 20"
    )
    PostingDate: datetime.date | None = Field(
        default=None, alias="PostingDate-4", description="Posting Date. Max length: 4"
    )
    DocumentType: int | None = Field(
        default=None, alias="DocumentType-5", description="Document Type. Max length: 4"
    )
    DocumentNo: str | None = Field(
        default=None, alias="DocumentNo-6", description="Document No.. Max length: 20"
    )
    Description: str | None = Field(
        default=None, alias="Description-7", description="Description. Max length: 100"
    )
    CustomerName: str | None = Field(
        default=None, alias="CustomerName-8", description="Customer Name. Max length: 100"
    )
    YourReference: str | None = Field(
        default=None, alias="YourReference-10", description="Your Reference. Max length: 35"
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-11", description="Currency Code. Max length: 10"
    )
    SalesLCY: float | None = Field(
        default=None, alias="SalesLCY-18", description="Sales (LCY). Max length: 12"
    )
    ProfitLCY: float | None = Field(
        default=None, alias="ProfitLCY-19", description="Profit (LCY). Max length: 12"
    )
    InvDiscountLCY: float | None = Field(
        default=None, alias="InvDiscountLCY-20", description="Inv. Discount (LCY). Max length: 12"
    )
    SelltoCustomerNo: str | None = Field(
        default=None,
        alias="SelltoCustomerNo-21",
        description="Sell-to Customer No.. Max length: 20",
    )
    CustomerPostingGroup: str | None = Field(
        default=None,
        alias="CustomerPostingGroup-22",
        description="Customer Posting Group. Max length: 20",
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-23",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-24",
        description="Global Dimension 2 Code. Max length: 20",
    )
    SalespersonCode: str | None = Field(
        default=None, alias="SalespersonCode-25", description="Salesperson Code. Max length: 20"
    )
    UserID: str | None = Field(
        default=None, alias="UserID-27", description="User ID. Max length: 50"
    )
    SourceCode: str | None = Field(
        default=None, alias="SourceCode-28", description="Source Code. Max length: 10"
    )
    OnHold: str | None = Field(
        default=None, alias="OnHold-33", description="On Hold. Max length: 3"
    )
    AppliestoDocType: int | None = Field(
        default=None, alias="AppliestoDocType-34", description="Applies-to Doc. Type. Max length: 4"
    )
    AppliestoDocNo: str | None = Field(
        default=None, alias="AppliestoDocNo-35", description="Applies-to Doc. No.. Max length: 20"
    )
    Open: bool | None = Field(default=None, alias="Open-36", description="Open. Max length: 4")
    DueDate: datetime.date | None = Field(
        default=None, alias="DueDate-37", description="Due Date. Max length: 4"
    )
    PmtDiscountDate: datetime.date | None = Field(
        default=None, alias="PmtDiscountDate-38", description="Pmt. Discount Date. Max length: 4"
    )
    OriginalPmtDiscPossible: float | None = Field(
        default=None,
        alias="OriginalPmtDiscPossible-39",
        description="Original Pmt. Disc. Possible. Max length: 12",
    )
    PmtDiscGivenLCY: float | None = Field(
        default=None,
        alias="PmtDiscGivenLCY-40",
        description="Pmt. Disc. Given (LCY). Max length: 12",
    )
    OrigPmtDiscPossibleLCY: float | None = Field(
        default=None,
        alias="OrigPmtDiscPossibleLCY-42",
        description="Orig. Pmt. Disc. Possible(LCY). Max length: 12",
    )
    Positive: bool | None = Field(
        default=None, alias="Positive-43", description="Positive. Max length: 4"
    )
    ClosedbyEntryNo: int | None = Field(
        default=None, alias="ClosedbyEntryNo-44", description="Closed by Entry No.. Max length: 4"
    )
    ClosedatDate: datetime.date | None = Field(
        default=None, alias="ClosedatDate-45", description="Closed at Date. Max length: 4"
    )
    ClosedbyAmount: float | None = Field(
        default=None, alias="ClosedbyAmount-46", description="Closed by Amount. Max length: 12"
    )
    AppliestoID: str | None = Field(
        default=None, alias="AppliestoID-47", description="Applies-to ID. Max length: 50"
    )
    JournalTemplName: str | None = Field(
        default=None, alias="JournalTemplName-48", description="Journal Templ. Name. Max length: 10"
    )
    JournalBatchName: str | None = Field(
        default=None, alias="JournalBatchName-49", description="Journal Batch Name. Max length: 10"
    )
    ReasonCode: str | None = Field(
        default=None, alias="ReasonCode-50", description="Reason Code. Max length: 10"
    )
    BalAccountType: int | None = Field(
        default=None, alias="BalAccountType-51", description="Bal. Account Type. Max length: 4"
    )
    BalAccountNo: str | None = Field(
        default=None, alias="BalAccountNo-52", description="Bal. Account No.. Max length: 20"
    )
    TransactionNo: int | None = Field(
        default=None, alias="TransactionNo-53", description="Transaction No.. Max length: 4"
    )
    ClosedbyAmountLCY: float | None = Field(
        default=None,
        alias="ClosedbyAmountLCY-54",
        description="Closed by Amount (LCY). Max length: 12",
    )
    DocumentDate: datetime.date | None = Field(
        default=None, alias="DocumentDate-62", description="Document Date. Max length: 4"
    )
    ExternalDocumentNo: str | None = Field(
        default=None,
        alias="ExternalDocumentNo-63",
        description="External Document No.. Max length: 35",
    )
    CalculateInterest: bool | None = Field(
        default=None, alias="CalculateInterest-64", description="Calculate Interest. Max length: 4"
    )
    ClosingInterestCalculated: bool | None = Field(
        default=None,
        alias="ClosingInterestCalculated-65",
        description="Closing Interest Calculated. Max length: 4",
    )
    NoSeries: str | None = Field(
        default=None, alias="NoSeries-66", description="No. Series. Max length: 20"
    )
    ClosedbyCurrencyCode: str | None = Field(
        default=None,
        alias="ClosedbyCurrencyCode-67",
        description="Closed by Currency Code. Max length: 10",
    )
    ClosedbyCurrencyAmount: float | None = Field(
        default=None,
        alias="ClosedbyCurrencyAmount-68",
        description="Closed by Currency Amount. Max length: 12",
    )
    AdjustedCurrencyFactor: float | None = Field(
        default=None,
        alias="AdjustedCurrencyFactor-73",
        description="Adjusted Currency Factor. Max length: 12",
    )
    OriginalCurrencyFactor: float | None = Field(
        default=None,
        alias="OriginalCurrencyFactor-74",
        description="Original Currency Factor. Max length: 12",
    )
    RemainingPmtDiscPossible: float | None = Field(
        default=None,
        alias="RemainingPmtDiscPossible-77",
        description="Remaining Pmt. Disc. Possible. Max length: 12",
    )
    PmtDiscToleranceDate: datetime.date | None = Field(
        default=None,
        alias="PmtDiscToleranceDate-78",
        description="Pmt. Disc. Tolerance Date. Max length: 4",
    )
    MaxPaymentTolerance: float | None = Field(
        default=None,
        alias="MaxPaymentTolerance-79",
        description="Max. Payment Tolerance. Max length: 12",
    )
    LastIssuedReminderLevel: int | None = Field(
        default=None,
        alias="LastIssuedReminderLevel-80",
        description="Last Issued Reminder Level. Max length: 4",
    )
    AcceptedPaymentTolerance: float | None = Field(
        default=None,
        alias="AcceptedPaymentTolerance-81",
        description="Accepted Payment Tolerance. Max length: 12",
    )
    AcceptedPmtDiscTolerance: bool | None = Field(
        default=None,
        alias="AcceptedPmtDiscTolerance-82",
        description="Accepted Pmt. Disc. Tolerance. Max length: 4",
    )
    PmtToleranceLCY: float | None = Field(
        default=None, alias="PmtToleranceLCY-83", description="Pmt. Tolerance (LCY). Max length: 12"
    )
    AmounttoApply: float | None = Field(
        default=None, alias="AmounttoApply-84", description="Amount to Apply. Max length: 12"
    )
    ICPartnerCode: str | None = Field(
        default=None, alias="ICPartnerCode-85", description="IC Partner Code. Max length: 20"
    )
    ApplyingEntry: bool | None = Field(
        default=None, alias="ApplyingEntry-86", description="Applying Entry. Max length: 4"
    )
    Reversed: bool | None = Field(
        default=None, alias="Reversed-87", description="Reversed. Max length: 4"
    )
    ReversedbyEntryNo: int | None = Field(
        default=None,
        alias="ReversedbyEntryNo-88",
        description="Reversed by Entry No.. Max length: 4",
    )
    ReversedEntryNo: int | None = Field(
        default=None, alias="ReversedEntryNo-89", description="Reversed Entry No.. Max length: 4"
    )
    Prepayment: bool | None = Field(
        default=None, alias="Prepayment-90", description="Prepayment. Max length: 4"
    )
    PaymentReference: str | None = Field(
        default=None, alias="PaymentReference-171", description="Payment Reference. Max length: 50"
    )
    PaymentMethodCode: str | None = Field(
        default=None,
        alias="PaymentMethodCode-172",
        description="Payment Method Code. Max length: 10",
    )
    AppliestoExtDocNo: str | None = Field(
        default=None,
        alias="AppliestoExtDocNo-173",
        description="Applies-to Ext. Doc. No.. Max length: 35",
    )
    RecipientBankAccount: str | None = Field(
        default=None,
        alias="RecipientBankAccount-288",
        description="Recipient Bank Account. Max length: 20",
    )
    MessagetoRecipient: str | None = Field(
        default=None,
        alias="MessagetoRecipient-289",
        description="Message to Recipient. Max length: 140",
    )
    ExportedtoPaymentFile: bool | None = Field(
        default=None,
        alias="ExportedtoPaymentFile-290",
        description="Exported to Payment File. Max length: 4",
    )
    DimensionSetID: int | None = Field(
        default=None, alias="DimensionSetID-480", description="Dimension Set ID. Max length: 4"
    )
    DirectDebitMandateID: str | None = Field(
        default=None,
        alias="DirectDebitMandateID-1200",
        description="Direct Debit Mandate ID. Max length: 35",
    )
    PaymentPrediction: int | None = Field(
        default=None,
        alias="PaymentPrediction-1300",
        description="Payment Prediction. Max length: 4",
    )
    PredictionConfidence: int | None = Field(
        default=None,
        alias="PredictionConfidence-1301",
        description="Prediction Confidence. Max length: 4",
    )
    PredictionConfidence: float | None = Field(
        default=None,
        alias="PredictionConfidence-1302",
        description="Prediction Confidence %. Max length: 12",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class Customer(SparkModel):
    """Represents the table Customer

    Display Name: Customer
    Entity Name: Customer-18
    """

    No: str | None = Field(default=None, alias="No-1", description="No.. Max length: 20")
    Name: str | None = Field(default=None, alias="Name-2", description="Name. Max length: 100")
    Address: str | None = Field(
        default=None, alias="Address-5", description="Address. Max length: 100"
    )
    Address2: str | None = Field(
        default=None, alias="Address2-6", description="Address 2. Max length: 50"
    )
    City: str | None = Field(default=None, alias="City-7", description="City. Max length: 30")
    Contact: str | None = Field(
        default=None, alias="Contact-8", description="Contact. Max length: 100"
    )
    PhoneNo: str | None = Field(
        default=None, alias="PhoneNo-9", description="Phone No.. Max length: 30"
    )
    TelexNo: str | None = Field(
        default=None, alias="TelexNo-10", description="Telex No.. Max length: 20"
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-16",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-17",
        description="Global Dimension 2 Code. Max length: 20",
    )
    ChainName: str | None = Field(
        default=None, alias="ChainName-18", description="Chain Name. Max length: 10"
    )
    BudgetedAmount: float | None = Field(
        default=None, alias="BudgetedAmount-19", description="Budgeted Amount. Max length: 12"
    )
    CreditLimitLCY: float | None = Field(
        default=None, alias="CreditLimitLCY-20", description="Credit Limit (LCY). Max length: 12"
    )
    CustomerPostingGroup: str | None = Field(
        default=None,
        alias="CustomerPostingGroup-21",
        description="Customer Posting Group. Max length: 20",
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-22", description="Currency Code. Max length: 10"
    )
    CustomerPriceGroup: str | None = Field(
        default=None,
        alias="CustomerPriceGroup-23",
        description="Customer Price Group. Max length: 10",
    )
    SalespersonCode: str | None = Field(
        default=None, alias="SalespersonCode-29", description="Salesperson Code. Max length: 20"
    )
    CustomerDiscGroup: str | None = Field(
        default=None,
        alias="CustomerDiscGroup-34",
        description="Customer Disc. Group. Max length: 20",
    )
    CountryRegionCode: str | None = Field(
        default=None,
        alias="CountryRegionCode-35",
        description="Country/Region Code. Max length: 10",
    )
    Blocked: int | None = Field(
        default=None, alias="Blocked-39", description="Blocked. Max length: 4"
    )
    GenBusPostingGroup: str | None = Field(
        default=None,
        alias="GenBusPostingGroup-88",
        description="Gen. Bus. Posting Group. Max length: 20",
    )
    PostCode: str | None = Field(
        default=None, alias="PostCode-91", description="Post Code. Max length: 20"
    )
    County: str | None = Field(
        default=None, alias="County-92", description="County. Max length: 30"
    )
    PTEIndustryCode: str | None = Field(
        default=None, alias="PTEIndustryCode-65001", description="PTE Industry Code. Max length: 20"
    )
    PTEGroupCode: str | None = Field(
        default=None, alias="PTEGroupCode-65006", description="PTE Group Code. Max length: 12"
    )
    PTEGroupingCode: str | None = Field(
        default=None, alias="PTEGroupingCode-65007", description="PTE Grouping Code. Max length: 10"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class DetailedCustLedgEntry(SparkModel):
    """Represents the table Detailed Cust. Ledg. Entry

    Display Name: Detailed Cust. Ledg. Entry
    Entity Name: DetailedCustLedgEntry-379
    """

    EntryNo: int | None = Field(
        default=None, alias="EntryNo-1", description="Entry No.. Max length: 4"
    )
    CustLedgerEntryNo: int | None = Field(
        default=None,
        alias="CustLedgerEntryNo-2",
        description="Cust. Ledger Entry No.. Max length: 4",
    )
    EntryType: int | None = Field(
        default=None, alias="EntryType-3", description="Entry Type. Max length: 4"
    )
    PostingDate: datetime.date | None = Field(
        default=None, alias="PostingDate-4", description="Posting Date. Max length: 4"
    )
    DocumentType: int | None = Field(
        default=None, alias="DocumentType-5", description="Document Type. Max length: 4"
    )
    DocumentNo: str | None = Field(
        default=None, alias="DocumentNo-6", description="Document No.. Max length: 20"
    )
    Amount: float | None = Field(
        default=None, alias="Amount-7", description="Amount. Max length: 12"
    )
    AmountLCY: float | None = Field(
        default=None, alias="AmountLCY-8", description="Amount (LCY). Max length: 12"
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-10", description="Currency Code. Max length: 10"
    )
    SourceCode: str | None = Field(
        default=None, alias="SourceCode-12", description="Source Code. Max length: 10"
    )
    DebitAmount: float | None = Field(
        default=None, alias="DebitAmount-16", description="Debit Amount. Max length: 12"
    )
    CreditAmount: float | None = Field(
        default=None, alias="CreditAmount-17", description="Credit Amount. Max length: 12"
    )
    DebitAmountLCY: float | None = Field(
        default=None, alias="DebitAmountLCY-18", description="Debit Amount (LCY). Max length: 12"
    )
    CreditAmountLCY: float | None = Field(
        default=None, alias="CreditAmountLCY-19", description="Credit Amount (LCY). Max length: 12"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class DetailedVendorLedgEntry(SparkModel):
    """Represents the table Detailed Vendor Ledg. Entry

    Display Name: Detailed Vendor Ledg. Entry
    Entity Name: DetailedVendorLedgEntry-380
    """

    EntryNo: int | None = Field(
        default=None, alias="EntryNo-1", description="Entry No.. Max length: 4"
    )
    VendorLedgerEntryNo: int | None = Field(
        default=None,
        alias="VendorLedgerEntryNo-2",
        description="Vendor Ledger Entry No.. Max length: 4",
    )
    EntryType: int | None = Field(
        default=None, alias="EntryType-3", description="Entry Type. Max length: 4"
    )
    PostingDate: datetime.date | None = Field(
        default=None, alias="PostingDate-4", description="Posting Date. Max length: 4"
    )
    DocumentType: int | None = Field(
        default=None, alias="DocumentType-5", description="Document Type. Max length: 4"
    )
    DocumentNo: str | None = Field(
        default=None, alias="DocumentNo-6", description="Document No.. Max length: 20"
    )
    Amount: float | None = Field(
        default=None, alias="Amount-7", description="Amount. Max length: 12"
    )
    AmountLCY: float | None = Field(
        default=None, alias="AmountLCY-8", description="Amount (LCY). Max length: 12"
    )
    VendorNo: str | None = Field(
        default=None, alias="VendorNo-9", description="Vendor No.. Max length: 20"
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-10", description="Currency Code. Max length: 10"
    )
    SourceCode: str | None = Field(
        default=None, alias="SourceCode-12", description="Source Code. Max length: 10"
    )
    TransactionNo: int | None = Field(
        default=None, alias="TransactionNo-13", description="Transaction No.. Max length: 4"
    )
    JournalBatchName: str | None = Field(
        default=None, alias="JournalBatchName-14", description="Journal Batch Name. Max length: 10"
    )
    ReasonCode: str | None = Field(
        default=None, alias="ReasonCode-15", description="Reason Code. Max length: 10"
    )
    DebitAmount: float | None = Field(
        default=None, alias="DebitAmount-16", description="Debit Amount. Max length: 12"
    )
    CreditAmount: float | None = Field(
        default=None, alias="CreditAmount-17", description="Credit Amount. Max length: 12"
    )
    DebitAmountLCY: float | None = Field(
        default=None, alias="DebitAmountLCY-18", description="Debit Amount (LCY). Max length: 12"
    )
    CreditAmountLCY: float | None = Field(
        default=None, alias="CreditAmountLCY-19", description="Credit Amount (LCY). Max length: 12"
    )
    InitialEntryDueDate: datetime.date | None = Field(
        default=None,
        alias="InitialEntryDueDate-20",
        description="Initial Entry Due Date. Max length: 4",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class Dimension(SparkModel):
    """Represents the table Dimension

    Display Name: Dimension
    Entity Name: Dimension-348
    """

    Code: str | None = Field(default=None, alias="Code-1", description="Code. Max length: 20")
    Name: str | None = Field(default=None, alias="Name-2", description="Name. Max length: 30")
    CodeCaption: str | None = Field(
        default=None, alias="CodeCaption-3", description="Code Caption. Max length: 80"
    )
    FilterCaption: str | None = Field(
        default=None, alias="FilterCaption-4", description="Filter Caption. Max length: 80"
    )
    Description: str | None = Field(
        default=None, alias="Description-5", description="Description. Max length: 100"
    )
    Blocked: bool | None = Field(
        default=None, alias="Blocked-6", description="Blocked. Max length: 4"
    )
    ConsolidationCode: str | None = Field(
        default=None, alias="ConsolidationCode-7", description="Consolidation Code. Max length: 20"
    )
    MaptoICDimensionCode: str | None = Field(
        default=None,
        alias="MaptoICDimensionCode-8",
        description="Map-to IC Dimension Code. Max length: 20",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class DimensionSetEntry(SparkModel):
    """Represents the table Dimension Set Entry

    Display Name: Dimension Set Entry
    Entity Name: DimensionSetEntry-480
    """

    DimensionSetID: int | None = Field(
        default=None, alias="DimensionSetID-1", description="Dimension Set ID. Max length: 4"
    )
    DimensionCode: str | None = Field(
        default=None, alias="DimensionCode-2", description="Dimension Code. Max length: 20"
    )
    DimensionValueCode: str | None = Field(
        default=None,
        alias="DimensionValueCode-3",
        description="Dimension Value Code. Max length: 20",
    )
    DimensionValueID: int | None = Field(
        default=None, alias="DimensionValueID-4", description="Dimension Value ID. Max length: 4"
    )
    GlobalDimensionNo: int | None = Field(
        default=None, alias="GlobalDimensionNo-8", description="Global Dimension No.. Max length: 4"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class DimensionValue(SparkModel):
    """Represents the table Dimension Value

    Display Name: Dimension Value
    Entity Name: DimensionValue-349
    """

    DimensionCode: str | None = Field(
        default=None, alias="DimensionCode-1", description="Dimension Code. Max length: 20"
    )
    Code: str | None = Field(default=None, alias="Code-2", description="Code. Max length: 20")
    Name: str | None = Field(default=None, alias="Name-3", description="Name. Max length: 50")
    DimensionValueType: int | None = Field(
        default=None,
        alias="DimensionValueType-4",
        description="Dimension Value Type. Max length: 4",
    )
    Totaling: str | None = Field(
        default=None, alias="Totaling-5", description="Totaling. Max length: 250"
    )
    Blocked: bool | None = Field(
        default=None, alias="Blocked-6", description="Blocked. Max length: 4"
    )
    ConsolidationCode: str | None = Field(
        default=None, alias="ConsolidationCode-7", description="Consolidation Code. Max length: 20"
    )
    Indentation: int | None = Field(
        default=None, alias="Indentation-8", description="Indentation. Max length: 4"
    )
    GlobalDimensionNo: int | None = Field(
        default=None, alias="GlobalDimensionNo-9", description="Global Dimension No.. Max length: 4"
    )
    MaptoICDimensionCode: str | None = Field(
        default=None,
        alias="MaptoICDimensionCode-10",
        description="Map-to IC Dimension Code. Max length: 20",
    )
    MaptoICDimensionValueCode: str | None = Field(
        default=None,
        alias="MaptoICDimensionValueCode-11",
        description="Map-to IC Dimension Value Code. Max length: 20",
    )
    DimensionValueID: int | None = Field(
        default=None, alias="DimensionValueID-12", description="Dimension Value ID. Max length: 4"
    )
    DimensionId: str | None = Field(
        default=None, alias="DimensionId-8002", description="Dimension Id. Max length: 16"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class GLAccount(SparkModel):
    """Represents the table G/L Account

    Display Name: G/L Account
    Entity Name: GLAccount-15
    """

    No: str | None = Field(default=None, alias="No-1", description="No.. Max length: 20")
    Name: str | None = Field(default=None, alias="Name-2", description="Name. Max length: 100")
    SearchName: str | None = Field(
        default=None, alias="SearchName-3", description="Search Name. Max length: 100"
    )
    AccountType: int | None = Field(
        default=None, alias="AccountType-4", description="Account Type. Max length: 4"
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-6",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-7",
        description="Global Dimension 2 Code. Max length: 20",
    )
    AccountCategory: int | None = Field(
        default=None, alias="AccountCategory-8", description="Account Category. Max length: 4"
    )
    IncomeBalance: int | None = Field(
        default=None, alias="IncomeBalance-9", description="Income/Balance. Max length: 4"
    )
    DebitCredit: int | None = Field(
        default=None, alias="DebitCredit-10", description="Debit/Credit. Max length: 4"
    )
    No2: str | None = Field(default=None, alias="No2-11", description="No. 2. Max length: 20")
    Blocked: bool | None = Field(
        default=None, alias="Blocked-13", description="Blocked. Max length: 4"
    )
    DirectPosting: bool | None = Field(
        default=None, alias="DirectPosting-14", description="Direct Posting. Max length: 4"
    )
    ReconciliationAccount: bool | None = Field(
        default=None,
        alias="ReconciliationAccount-16",
        description="Reconciliation Account. Max length: 4",
    )
    NewPage: bool | None = Field(
        default=None, alias="NewPage-17", description="New Page. Max length: 4"
    )
    NoofBlankLines: int | None = Field(
        default=None, alias="NoofBlankLines-18", description="No. of Blank Lines. Max length: 4"
    )
    Indentation: int | None = Field(
        default=None, alias="Indentation-19", description="Indentation. Max length: 4"
    )
    LastModifiedDateTime: datetime.datetime | None = Field(
        default=None,
        alias="LastModifiedDateTime-25",
        description="Last Modified Date Time. Max length: 8",
    )
    LastDateModified: datetime.date | None = Field(
        default=None, alias="LastDateModified-26", description="Last Date Modified. Max length: 4"
    )
    Totaling: str | None = Field(
        default=None, alias="Totaling-34", description="Totaling. Max length: 250"
    )
    ConsolTranslationMethod: int | None = Field(
        default=None,
        alias="ConsolTranslationMethod-39",
        description="Consol. Translation Method. Max length: 4",
    )
    ConsolDebitAcc: str | None = Field(
        default=None, alias="ConsolDebitAcc-40", description="Consol. Debit Acc.. Max length: 20"
    )
    ConsolCreditAcc: str | None = Field(
        default=None, alias="ConsolCreditAcc-41", description="Consol. Credit Acc.. Max length: 20"
    )
    GenPostingType: int | None = Field(
        default=None, alias="GenPostingType-43", description="Gen. Posting Type. Max length: 4"
    )
    GenBusPostingGroup: str | None = Field(
        default=None,
        alias="GenBusPostingGroup-44",
        description="Gen. Bus. Posting Group. Max length: 20",
    )
    GenProdPostingGroup: str | None = Field(
        default=None,
        alias="GenProdPostingGroup-45",
        description="Gen. Prod. Posting Group. Max length: 20",
    )
    AutomaticExtTexts: bool | None = Field(
        default=None,
        alias="AutomaticExtTexts-49",
        description="Automatic Ext. Texts. Max length: 4",
    )
    TaxAreaCode: str | None = Field(
        default=None, alias="TaxAreaCode-54", description="Tax Area Code. Max length: 20"
    )
    TaxLiable: bool | None = Field(
        default=None, alias="TaxLiable-55", description="Tax Liable. Max length: 4"
    )
    TaxGroupCode: str | None = Field(
        default=None, alias="TaxGroupCode-56", description="Tax Group Code. Max length: 20"
    )
    VATBusPostingGroup: str | None = Field(
        default=None,
        alias="VATBusPostingGroup-57",
        description="VAT Bus. Posting Group. Max length: 20",
    )
    VATProdPostingGroup: str | None = Field(
        default=None,
        alias="VATProdPostingGroup-58",
        description="VAT Prod. Posting Group. Max length: 20",
    )
    ExchangeRateAdjustment: int | None = Field(
        default=None,
        alias="ExchangeRateAdjustment-63",
        description="Exchange Rate Adjustment. Max length: 4",
    )
    DefaultICPartnerGLAccNo: str | None = Field(
        default=None,
        alias="DefaultICPartnerGLAccNo-66",
        description="Default IC Partner G/L Acc. No. Max length: 20",
    )
    OmitDefaultDescrinJnl: bool | None = Field(
        default=None,
        alias="OmitDefaultDescrinJnl-70",
        description="Omit Default Descr. in Jnl.. Max length: 4",
    )
    AccountSubcategoryEntryNo: int | None = Field(
        default=None,
        alias="AccountSubcategoryEntryNo-80",
        description="Account Subcategory Entry No.. Max length: 4",
    )
    CostTypeNo: str | None = Field(
        default=None, alias="CostTypeNo-1100", description="Cost Type No.. Max length: 20"
    )
    DefaultDeferralTemplateCode: str | None = Field(
        default=None,
        alias="DefaultDeferralTemplateCode-1700",
        description="Default Deferral Template Code. Max length: 10",
    )
    APIAccountType: int | None = Field(
        default=None, alias="APIAccountType-9000", description="API Account Type. Max length: 4"
    )
    IRSNumber: str | None = Field(
        default=None, alias="IRSNumber-10900", description="IRS Number. Max length: 10"
    )
    IRSNo: str | None = Field(
        default=None, alias="IRSNo-14602", description="IRS No.. Max length: 10"
    )
    ReviewPolicy: int | None = Field(
        default=None, alias="ReviewPolicy-22200", description="Review Policy. Max length: 4"
    )
    PTENewNo: str | None = Field(
        default=None, alias="PTENewNo-65000", description="PTE New No.. Max length: 20"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class GLEntry(SparkModel):
    """Represents the table G/L Entry

    Display Name: G/L Entry
    Entity Name: GLEntry-17
    """

    EntryNo: int | None = Field(
        default=None, alias="EntryNo-1", description="Entry No.. Max length: 4"
    )
    GLAccountNo: str | None = Field(
        default=None, alias="GLAccountNo-3", description="G/L Account No.. Max length: 20"
    )
    PostingDate: datetime.date | None = Field(
        default=None, alias="PostingDate-4", description="Posting Date. Max length: 4"
    )
    DocumentType: int | None = Field(
        default=None, alias="DocumentType-5", description="Document Type. Max length: 4"
    )
    DocumentNo: str | None = Field(
        default=None, alias="DocumentNo-6", description="Document No.. Max length: 20"
    )
    Description: str | None = Field(
        default=None, alias="Description-7", description="Description. Max length: 100"
    )
    BalAccountNo: str | None = Field(
        default=None, alias="BalAccountNo-10", description="Bal. Account No.. Max length: 20"
    )
    Amount: float | None = Field(
        default=None, alias="Amount-17", description="Amount. Max length: 12"
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-23",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-24",
        description="Global Dimension 2 Code. Max length: 20",
    )
    UserID: str | None = Field(
        default=None, alias="UserID-27", description="User ID. Max length: 50"
    )
    SourceCode: str | None = Field(
        default=None, alias="SourceCode-28", description="Source Code. Max length: 10"
    )
    SystemCreatedEntry: bool | None = Field(
        default=None,
        alias="SystemCreatedEntry-29",
        description="System-Created Entry. Max length: 4",
    )
    PriorYearEntry: bool | None = Field(
        default=None, alias="PriorYearEntry-30", description="Prior-Year Entry. Max length: 4"
    )
    JobNo: str | None = Field(default=None, alias="JobNo-41", description="Job No.. Max length: 20")
    Quantity: float | None = Field(
        default=None, alias="Quantity-42", description="Quantity. Max length: 12"
    )
    VATAmount: float | None = Field(
        default=None, alias="VATAmount-43", description="VAT Amount. Max length: 12"
    )
    BusinessUnitCode: str | None = Field(
        default=None, alias="BusinessUnitCode-45", description="Business Unit Code. Max length: 20"
    )
    JournalBatchName: str | None = Field(
        default=None, alias="JournalBatchName-46", description="Journal Batch Name. Max length: 10"
    )
    ReasonCode: str | None = Field(
        default=None, alias="ReasonCode-47", description="Reason Code. Max length: 10"
    )
    GenPostingType: int | None = Field(
        default=None, alias="GenPostingType-48", description="Gen. Posting Type. Max length: 4"
    )
    GenBusPostingGroup: str | None = Field(
        default=None,
        alias="GenBusPostingGroup-49",
        description="Gen. Bus. Posting Group. Max length: 20",
    )
    GenProdPostingGroup: str | None = Field(
        default=None,
        alias="GenProdPostingGroup-50",
        description="Gen. Prod. Posting Group. Max length: 20",
    )
    BalAccountType: int | None = Field(
        default=None, alias="BalAccountType-51", description="Bal. Account Type. Max length: 4"
    )
    TransactionNo: int | None = Field(
        default=None, alias="TransactionNo-52", description="Transaction No.. Max length: 4"
    )
    DebitAmount: float | None = Field(
        default=None, alias="DebitAmount-53", description="Debit Amount. Max length: 12"
    )
    CreditAmount: float | None = Field(
        default=None, alias="CreditAmount-54", description="Credit Amount. Max length: 12"
    )
    DocumentDate: datetime.date | None = Field(
        default=None, alias="DocumentDate-55", description="Document Date. Max length: 4"
    )
    ExternalDocumentNo: str | None = Field(
        default=None,
        alias="ExternalDocumentNo-56",
        description="External Document No.. Max length: 35",
    )
    SourceType: int | None = Field(
        default=None, alias="SourceType-57", description="Source Type. Max length: 4"
    )
    SourceNo: str | None = Field(
        default=None, alias="SourceNo-58", description="Source No.. Max length: 20"
    )
    NoSeries: str | None = Field(
        default=None, alias="NoSeries-59", description="No. Series. Max length: 20"
    )
    TaxAreaCode: str | None = Field(
        default=None, alias="TaxAreaCode-60", description="Tax Area Code. Max length: 20"
    )
    TaxLiable: bool | None = Field(
        default=None, alias="TaxLiable-61", description="Tax Liable. Max length: 4"
    )
    TaxGroupCode: str | None = Field(
        default=None, alias="TaxGroupCode-62", description="Tax Group Code. Max length: 20"
    )
    UseTax: bool | None = Field(
        default=None, alias="UseTax-63", description="Use Tax. Max length: 4"
    )
    VATBusPostingGroup: str | None = Field(
        default=None,
        alias="VATBusPostingGroup-64",
        description="VAT Bus. Posting Group. Max length: 20",
    )
    VATProdPostingGroup: str | None = Field(
        default=None,
        alias="VATProdPostingGroup-65",
        description="VAT Prod. Posting Group. Max length: 20",
    )
    AdditionalCurrencyAmount: float | None = Field(
        default=None,
        alias="AdditionalCurrencyAmount-68",
        description="Additional-Currency Amount. Max length: 12",
    )
    AddCurrencyDebitAmount: float | None = Field(
        default=None,
        alias="AddCurrencyDebitAmount-69",
        description="Add.-Currency Debit Amount. Max length: 12",
    )
    AddCurrencyCreditAmount: float | None = Field(
        default=None,
        alias="AddCurrencyCreditAmount-70",
        description="Add.-Currency Credit Amount. Max length: 12",
    )
    CloseIncomeStatementDimID: int | None = Field(
        default=None,
        alias="CloseIncomeStatementDimID-71",
        description="Close Income Statement Dim. ID. Max length: 4",
    )
    ICPartnerCode: str | None = Field(
        default=None, alias="ICPartnerCode-72", description="IC Partner Code. Max length: 20"
    )
    Reversed: bool | None = Field(
        default=None, alias="Reversed-73", description="Reversed. Max length: 4"
    )
    ReversedbyEntryNo: int | None = Field(
        default=None,
        alias="ReversedbyEntryNo-74",
        description="Reversed by Entry No.. Max length: 4",
    )
    ReversedEntryNo: int | None = Field(
        default=None, alias="ReversedEntryNo-75", description="Reversed Entry No.. Max length: 4"
    )
    JournalTemplName: str | None = Field(
        default=None, alias="JournalTemplName-78", description="Journal Templ. Name. Max length: 10"
    )
    VATReportingDate: datetime.date | None = Field(
        default=None, alias="VATReportingDate-79", description="VAT Reporting Date. Max length: 4"
    )
    DimensionSetID: int | None = Field(
        default=None, alias="DimensionSetID-480", description="Dimension Set ID. Max length: 4"
    )
    LastDimCorrectionEntryNo: int | None = Field(
        default=None,
        alias="LastDimCorrectionEntryNo-495",
        description="Last Dim. Correction Entry No.. Max length: 4",
    )
    LastDimCorrectionNode: int | None = Field(
        default=None,
        alias="LastDimCorrectionNode-496",
        description="Last Dim. Correction Node. Max length: 4",
    )
    DimensionChangesCount: int | None = Field(
        default=None,
        alias="DimensionChangesCount-497",
        description="Dimension Changes Count. Max length: 4",
    )
    AllocationAccountNo: str | None = Field(
        default=None,
        alias="AllocationAccountNo-2678",
        description="Allocation Account No.. Max length: 20",
    )
    ProdOrderNo: str | None = Field(
        default=None, alias="ProdOrderNo-5400", description="Prod. Order No.. Max length: 20"
    )
    FAEntryType: int | None = Field(
        default=None, alias="FAEntryType-5600", description="FA Entry Type. Max length: 4"
    )
    FAEntryNo: int | None = Field(
        default=None, alias="FAEntryNo-5601", description="FA Entry No.. Max length: 4"
    )
    Comment: str | None = Field(
        default=None, alias="Comment-5618", description="Comment. Max length: 250"
    )
    NonDeductibleVATAmount: float | None = Field(
        default=None,
        alias="NonDeductibleVATAmount-6200",
        description="Non-Deductible VAT Amount. Max length: 12",
    )
    NonDeductibleVATAmountACY: float | None = Field(
        default=None,
        alias="NonDeductibleVATAmountACY-6201",
        description="Non-Deductible VAT Amount ACY. Max length: 12",
    )
    LastModifiedDateTime: datetime.datetime | None = Field(
        default=None,
        alias="LastModifiedDateTime-8005",
        description="Last Modified DateTime. Max length: 8",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class GeneralLedgerSetup(SparkModel):
    """Represents the table General Ledger Setup

    Display Name: General Ledger Setup
    Entity Name: GeneralLedgerSetup-98
    """

    AdditionalReportingCurrency: str | None = Field(
        default=None,
        alias="AdditionalReportingCurrency-68",
        description="Additional Reporting Currency. Max length: 10",
    )
    LCYCode: str | None = Field(
        default=None, alias="LCYCode-71", description="LCY Code. Max length: 10"
    )
    ShortcutDimension1Code: str | None = Field(
        default=None,
        alias="ShortcutDimension1Code-81",
        description="Shortcut Dimension 1 Code. Max length: 20",
    )
    ShortcutDimension2Code: str | None = Field(
        default=None,
        alias="ShortcutDimension2Code-82",
        description="Shortcut Dimension 2 Code. Max length: 20",
    )
    ShortcutDimension3Code: str | None = Field(
        default=None,
        alias="ShortcutDimension3Code-83",
        description="Shortcut Dimension 3 Code. Max length: 20",
    )
    ShortcutDimension4Code: str | None = Field(
        default=None,
        alias="ShortcutDimension4Code-84",
        description="Shortcut Dimension 4 Code. Max length: 20",
    )
    ShortcutDimension5Code: str | None = Field(
        default=None,
        alias="ShortcutDimension5Code-85",
        description="Shortcut Dimension 5 Code. Max length: 20",
    )
    ShortcutDimension6Code: str | None = Field(
        default=None,
        alias="ShortcutDimension6Code-86",
        description="Shortcut Dimension 6 Code. Max length: 20",
    )
    ShortcutDimension7Code: str | None = Field(
        default=None,
        alias="ShortcutDimension7Code-87",
        description="Shortcut Dimension 7 Code. Max length: 20",
    )
    ShortcutDimension8Code: str | None = Field(
        default=None,
        alias="ShortcutDimension8Code-88",
        description="Shortcut Dimension 8 Code. Max length: 20",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class Item(SparkModel):
    """Represents the table Item

    Display Name: Item
    Entity Name: Item-27
    """

    No: str | None = Field(default=None, alias="No-1", description="No.. Max length: 20")
    Description: str | None = Field(
        default=None, alias="Description-3", description="Description. Max length: 100"
    )
    Description2: str | None = Field(
        default=None, alias="Description2-5", description="Description 2. Max length: 50"
    )
    BaseUnitofMeasure: str | None = Field(
        default=None,
        alias="BaseUnitofMeasure-8",
        description="Base Unit of Measure. Max length: 10",
    )
    Type: int | None = Field(default=None, alias="Type-10", description="Type. Max length: 4")
    InventoryPostingGroup: str | None = Field(
        default=None,
        alias="InventoryPostingGroup-11",
        description="Inventory Posting Group. Max length: 20",
    )
    ItemDiscGroup: str | None = Field(
        default=None, alias="ItemDiscGroup-14", description="Item Disc. Group. Max length: 20"
    )
    UnitPrice: float | None = Field(
        default=None, alias="UnitPrice-18", description="Unit Price. Max length: 12"
    )
    UnitCost: float | None = Field(
        default=None, alias="UnitCost-22", description="Unit Cost. Max length: 12"
    )
    VendorNo: str | None = Field(
        default=None, alias="VendorNo-31", description="Vendor No.. Max length: 20"
    )
    VendorItemNo: str | None = Field(
        default=None, alias="VendorItemNo-32", description="Vendor Item No.. Max length: 50"
    )
    GrossWeight: float | None = Field(
        default=None, alias="GrossWeight-41", description="Gross Weight. Max length: 12"
    )
    NetWeight: float | None = Field(
        default=None, alias="NetWeight-42", description="Net Weight. Max length: 12"
    )
    UnitVolume: float | None = Field(
        default=None, alias="UnitVolume-44", description="Unit Volume. Max length: 12"
    )
    Blocked: bool | None = Field(
        default=None, alias="Blocked-54", description="Blocked. Max length: 4"
    )
    GenProdPostingGroup: str | None = Field(
        default=None,
        alias="GenProdPostingGroup-91",
        description="Gen. Prod. Posting Group. Max length: 20",
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-105",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-106",
        description="Global Dimension 2 Code. Max length: 20",
    )
    AssemblyPolicy: int | None = Field(
        default=None, alias="AssemblyPolicy-910", description="Assembly Policy. Max length: 4"
    )
    ItemCategoryCode: str | None = Field(
        default=None,
        alias="ItemCategoryCode-5702",
        description="Item Category Code. Max length: 20",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class Job(SparkModel):
    """Represents the table Job

    Display Name: Job
    Entity Name: Job-167
    """

    No: str | None = Field(default=None, alias="No-1", description="No.. Max length: 20")
    SearchDescription: str | None = Field(
        default=None, alias="SearchDescription-2", description="Search Description. Max length: 100"
    )
    Description: str | None = Field(
        default=None, alias="Description-3", description="Description. Max length: 100"
    )
    Description2: str | None = Field(
        default=None, alias="Description2-4", description="Description 2. Max length: 50"
    )
    BilltoCustomerNo: str | None = Field(
        default=None, alias="BilltoCustomerNo-5", description="Bill-to Customer No.. Max length: 20"
    )
    CreationDate: datetime.date | None = Field(
        default=None, alias="CreationDate-12", description="Creation Date. Max length: 4"
    )
    StartingDate: datetime.date | None = Field(
        default=None, alias="StartingDate-13", description="Starting Date. Max length: 4"
    )
    EndingDate: datetime.date | None = Field(
        default=None, alias="EndingDate-14", description="Ending Date. Max length: 4"
    )
    Status: int | None = Field(default=None, alias="Status-19", description="Status. Max length: 4")
    PersonResponsible: str | None = Field(
        default=None, alias="PersonResponsible-20", description="Person Responsible. Max length: 20"
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-21",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-22",
        description="Global Dimension 2 Code. Max length: 20",
    )
    JobPostingGroup: str | None = Field(
        default=None, alias="JobPostingGroup-23", description="Job Posting Group. Max length: 20"
    )
    Blocked: int | None = Field(
        default=None, alias="Blocked-24", description="Blocked. Max length: 4"
    )
    LastDateModified: datetime.date | None = Field(
        default=None, alias="LastDateModified-29", description="Last Date Modified. Max length: 4"
    )
    CustomerDiscGroup: str | None = Field(
        default=None,
        alias="CustomerDiscGroup-31",
        description="Customer Disc. Group. Max length: 20",
    )
    CustomerPriceGroup: str | None = Field(
        default=None,
        alias="CustomerPriceGroup-32",
        description="Customer Price Group. Max length: 10",
    )
    LanguageCode: str | None = Field(
        default=None, alias="LanguageCode-41", description="Language Code. Max length: 10"
    )
    BilltoName: str | None = Field(
        default=None, alias="BilltoName-58", description="Bill-to Name. Max length: 100"
    )
    BilltoAddress: str | None = Field(
        default=None, alias="BilltoAddress-59", description="Bill-to Address. Max length: 100"
    )
    BilltoAddress2: str | None = Field(
        default=None, alias="BilltoAddress2-60", description="Bill-to Address 2. Max length: 50"
    )
    BilltoCity: str | None = Field(
        default=None, alias="BilltoCity-61", description="Bill-to City. Max length: 30"
    )
    BilltoCounty: str | None = Field(
        default=None, alias="BilltoCounty-63", description="Bill-to County. Max length: 30"
    )
    BilltoPostCode: str | None = Field(
        default=None, alias="BilltoPostCode-64", description="Bill-to Post Code. Max length: 20"
    )
    NoSeries: str | None = Field(
        default=None, alias="NoSeries-66", description="No. Series. Max length: 20"
    )
    BilltoCountryRegionCode: str | None = Field(
        default=None,
        alias="BilltoCountryRegionCode-67",
        description="Bill-to Country/Region Code. Max length: 10",
    )
    BilltoName2: str | None = Field(
        default=None, alias="BilltoName2-68", description="Bill-to Name 2. Max length: 50"
    )
    Reserve: int | None = Field(
        default=None, alias="Reserve-117", description="Reserve. Max length: 4"
    )
    WIPMethod: str | None = Field(
        default=None, alias="WIPMethod-1000", description="WIP Method. Max length: 20"
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-1001", description="Currency Code. Max length: 10"
    )
    BilltoContactNo: str | None = Field(
        default=None,
        alias="BilltoContactNo-1002",
        description="Bill-to Contact No.. Max length: 20",
    )
    BilltoContact: str | None = Field(
        default=None, alias="BilltoContact-1003", description="Bill-to Contact. Max length: 100"
    )
    WIPPostingDate: datetime.date | None = Field(
        default=None, alias="WIPPostingDate-1008", description="WIP Posting Date. Max length: 4"
    )
    InvoiceCurrencyCode: str | None = Field(
        default=None,
        alias="InvoiceCurrencyCode-1011",
        description="Invoice Currency Code. Max length: 10",
    )
    ExchCalculationCost: int | None = Field(
        default=None,
        alias="ExchCalculationCost-1012",
        description="Exch. Calculation (Cost). Max length: 4",
    )
    ExchCalculationPrice: int | None = Field(
        default=None,
        alias="ExchCalculationPrice-1013",
        description="Exch. Calculation (Price). Max length: 4",
    )
    AllowScheduleContractLines: bool | None = Field(
        default=None,
        alias="AllowScheduleContractLines-1014",
        description="Allow Schedule/Contract Lines. Max length: 4",
    )
    Complete: bool | None = Field(
        default=None, alias="Complete-1015", description="Complete. Max length: 4"
    )
    ApplyUsageLink: bool | None = Field(
        default=None, alias="ApplyUsageLink-1025", description="Apply Usage Link. Max length: 4"
    )
    WIPPostingMethod: int | None = Field(
        default=None, alias="WIPPostingMethod-1027", description="WIP Posting Method. Max length: 4"
    )
    OverBudget: bool | None = Field(
        default=None, alias="OverBudget-1035", description="Over Budget. Max length: 4"
    )
    ProjectManager: str | None = Field(
        default=None, alias="ProjectManager-1036", description="Project Manager. Max length: 50"
    )
    SelltoCustomerNo: str | None = Field(
        default=None,
        alias="SelltoCustomerNo-2000",
        description="Sell-to Customer No.. Max length: 20",
    )
    SelltoCustomerName: str | None = Field(
        default=None,
        alias="SelltoCustomerName-2001",
        description="Sell-to Customer Name. Max length: 100",
    )
    SelltoCustomerName2: str | None = Field(
        default=None,
        alias="SelltoCustomerName2-2002",
        description="Sell-to Customer Name 2. Max length: 50",
    )
    SelltoAddress: str | None = Field(
        default=None, alias="SelltoAddress-2003", description="Sell-to Address. Max length: 100"
    )
    SelltoAddress2: str | None = Field(
        default=None, alias="SelltoAddress2-2004", description="Sell-to Address 2. Max length: 50"
    )
    SelltoCity: str | None = Field(
        default=None, alias="SelltoCity-2005", description="Sell-to City. Max length: 30"
    )
    SelltoContact: str | None = Field(
        default=None, alias="SelltoContact-2006", description="Sell-to Contact. Max length: 100"
    )
    SelltoPostCode: str | None = Field(
        default=None, alias="SelltoPostCode-2007", description="Sell-to Post Code. Max length: 20"
    )
    SelltoCounty: str | None = Field(
        default=None, alias="SelltoCounty-2008", description="Sell-to County. Max length: 30"
    )
    SelltoCountryRegionCode: str | None = Field(
        default=None,
        alias="SelltoCountryRegionCode-2009",
        description="Sell-to Country/Region Code. Max length: 10",
    )
    SelltoPhoneNo: str | None = Field(
        default=None, alias="SelltoPhoneNo-2010", description="Sell-to Phone No.. Max length: 30"
    )
    SelltoEMail: str | None = Field(
        default=None, alias="SelltoEMail-2011", description="Sell-to E-Mail. Max length: 80"
    )
    SelltoContactNo: str | None = Field(
        default=None,
        alias="SelltoContactNo-2012",
        description="Sell-to Contact No.. Max length: 20",
    )
    ShiptoCode: str | None = Field(
        default=None, alias="ShiptoCode-3000", description="Ship-to Code. Max length: 10"
    )
    ShiptoName: str | None = Field(
        default=None, alias="ShiptoName-3001", description="Ship-to Name. Max length: 100"
    )
    ShiptoName2: str | None = Field(
        default=None, alias="ShiptoName2-3002", description="Ship-to Name 2. Max length: 50"
    )
    ShiptoAddress: str | None = Field(
        default=None, alias="ShiptoAddress-3003", description="Ship-to Address. Max length: 100"
    )
    ShiptoAddress2: str | None = Field(
        default=None, alias="ShiptoAddress2-3004", description="Ship-to Address 2. Max length: 50"
    )
    ShiptoCity: str | None = Field(
        default=None, alias="ShiptoCity-3005", description="Ship-to City. Max length: 30"
    )
    ShiptoContact: str | None = Field(
        default=None, alias="ShiptoContact-3006", description="Ship-to Contact. Max length: 100"
    )
    ShiptoPostCode: str | None = Field(
        default=None, alias="ShiptoPostCode-3007", description="Ship-to Post Code. Max length: 20"
    )
    ShiptoCounty: str | None = Field(
        default=None, alias="ShiptoCounty-3008", description="Ship-to County. Max length: 30"
    )
    ShiptoCountryRegionCode: str | None = Field(
        default=None,
        alias="ShiptoCountryRegionCode-3009",
        description="Ship-to Country/Region Code. Max length: 10",
    )
    ExternalDocumentNo: str | None = Field(
        default=None,
        alias="ExternalDocumentNo-4000",
        description="External Document No.. Max length: 35",
    )
    PaymentMethodCode: str | None = Field(
        default=None,
        alias="PaymentMethodCode-4001",
        description="Payment Method Code. Max length: 10",
    )
    PaymentTermsCode: str | None = Field(
        default=None,
        alias="PaymentTermsCode-4002",
        description="Payment Terms Code. Max length: 10",
    )
    YourReference: str | None = Field(
        default=None, alias="YourReference-4003", description="Your Reference. Max length: 35"
    )
    PriceCalculationMethod: int | None = Field(
        default=None,
        alias="PriceCalculationMethod-7000",
        description="Price Calculation Method. Max length: 4",
    )
    CostCalculationMethod: int | None = Field(
        default=None,
        alias="CostCalculationMethod-7001",
        description="Cost Calculation Method. Max length: 4",
    )
    PTEGroupCode: str | None = Field(
        default=None, alias="PTEGroupCode-65000", description="PTE Group Code. Max length: 12"
    )
    PTEForbidtoReopenJobReq: bool | None = Field(
        default=None,
        alias="PTEForbidtoReopenJobReq-65001",
        description="PTE Forbid to Reopen Job Req.. Max length: 4",
    )
    PTETempoID: str | None = Field(
        default=None, alias="PTETempoID-65002", description="PTE Tempo ID. Max length: 30"
    )
    PTEJiraPrimaryProjectId: int | None = Field(
        default=None,
        alias="PTEJiraPrimaryProjectId-65003",
        description="PTE Jira Primary Project Id. Max length: 4",
    )
    PTETransactionReportssur: bool | None = Field(
        default=None,
        alias="PTETransactionReportssur-65004",
        description="PTE Transaction Report Össur. Max length: 4",
    )
    PTECategory: str | None = Field(
        default=None, alias="PTECategory-65005", description="PTE Category. Max length: 10"
    )
    PTEJobRequestRequired: bool | None = Field(
        default=None,
        alias="PTEJobRequestRequired-65006",
        description="PTE Job Request Required. Max length: 4",
    )
    WJOBNamedEmployees: bool | None = Field(
        default=None,
        alias="WJOBNamedEmployees-10007200",
        description="WJOBNamedEmployees. Max length: 4",
    )
    WJOBRSMReport: int | None = Field(
        default=None, alias="WJOBRSMReport-10007203", description="WJOB RSM Report. Max length: 4"
    )
    WJOBRSMReportTable: int | None = Field(
        default=None,
        alias="WJOBRSMReportTable-10007204",
        description="WJOB RSM Report Table. Max length: 4",
    )
    WJOBTemplate: bool | None = Field(
        default=None, alias="WJOBTemplate-10007208", description="WJOB Template. Max length: 4"
    )
    WJOBType: str | None = Field(
        default=None, alias="WJOBType-10007209", description="WJOB Type. Max length: 15"
    )
    WJOBWorkTypeCode: str | None = Field(
        default=None,
        alias="WJOBWorkTypeCode-10007211",
        description="WJOB Work Type Code. Max length: 10",
    )
    WJOBRSMCombine: int | None = Field(
        default=None, alias="WJOBRSMCombine-10007212", description="WJOB RSM Combine. Max length: 4"
    )
    WJOBScheduleisalsoContract: bool | None = Field(
        default=None,
        alias="WJOBScheduleisalsoContract-10007218",
        description="WJOBSchedule is also Contract. Max length: 4",
    )
    WJOBCreateSchedulewhenPoste: bool | None = Field(
        default=None,
        alias="WJOBCreateSchedulewhenPoste-10007219",
        description="WJOBCreate Schedule when Poste. Max length: 4",
    )
    WJOBMaxHoursWarning: bool | None = Field(
        default=None,
        alias="WJOBMaxHoursWarning-10007220",
        description="WJOB Max. Hours Warning. Max length: 4",
    )
    WJOBSupervisor: str | None = Field(
        default=None, alias="WJOBSupervisor-10007221", description="WJOB Supervisor. Max length: 20"
    )
    WJOBVATBusPostingGroup: str | None = Field(
        default=None,
        alias="WJOBVATBusPostingGroup-10007222",
        description="WJOB VAT Bus.Posting Group. Max length: 10",
    )
    WJOBJournalsReviewedDate: datetime.date | None = Field(
        default=None,
        alias="WJOBJournalsReviewedDate-10007223",
        description="WJOB Journals Reviewed Date. Max length: 4",
    )
    WJOBCreatedby: str | None = Field(
        default=None, alias="WJOBCreatedby-10007225", description="WJOB Created by. Max length: 50"
    )
    WJOBLastModifiedby: str | None = Field(
        default=None,
        alias="WJOBLastModifiedby-10007226",
        description="WJOB Last Modified by. Max length: 50",
    )
    WJOBShiptoCode: str | None = Field(
        default=None,
        alias="WJOBShiptoCode-10007249",
        description="WJOB Ship-to Code. Max length: 10",
    )
    WJobPAInvoiceMaxhours: float | None = Field(
        default=None,
        alias="WJobPAInvoiceMaxhours-10007251",
        description="WJobPA Invoice Max. (hours). Max length: 12",
    )
    WJobPAInvoiceMaxAmount: float | None = Field(
        default=None,
        alias="WJobPAInvoiceMaxAmount-10007252",
        description="WJobPA Invoice Max. Amount. Max length: 12",
    )
    WJobPADiscountLimitAmt: float | None = Field(
        default=None,
        alias="WJobPADiscountLimitAmt-10007253",
        description="WJobPA Discount Limit (Amt). Max length: 12",
    )
    WJobPAExcLimitDiscount: float | None = Field(
        default=None,
        alias="WJobPAExcLimitDiscount-10007254",
        description="WJobPA Exc. Limit Discount %. Max length: 12",
    )
    WJobPAProformaperTask: bool | None = Field(
        default=None,
        alias="WJobPAProformaperTask-10007256",
        description="WJobPA Proforma per Task. Max length: 4",
    )
    WJobPAIndexCode: str | None = Field(
        default=None,
        alias="WJobPAIndexCode-10007258",
        description="WJobPA Index Code. Max length: 10",
    )
    WJobPAContractRefIndex: float | None = Field(
        default=None,
        alias="WJobPAContractRefIndex-10007259",
        description="WJobPA Contract Ref. Index. Max length: 12",
    )
    WJobJRJobRequestsRequired: bool | None = Field(
        default=None,
        alias="WJobJRJobRequestsRequired-10007270",
        description="WJobJR Job Requests Required. Max length: 4",
    )
    WJOBShowonTransactionReport: int | None = Field(
        default=None,
        alias="WJOBShowonTransactionReport-10007550",
        description="WJOBShow on Transaction Report. Max length: 4",
    )
    WJOBTransactionGrouping: int | None = Field(
        default=None,
        alias="WJOBTransactionGrouping-10007551",
        description="WJOB Transaction Grouping. Max length: 4",
    )
    WJobPABilltoJobNo: str | None = Field(
        default=None,
        alias="WJobPABilltoJobNo-10027451",
        description="WJobPA Bill-to Job No.. Max length: 20",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class JobLedgerEntry(SparkModel):
    """Represents the table Job Ledger Entry

    Display Name: Job Ledger Entry
    Entity Name: JobLedgerEntry-169
    """

    EntryNo: int | None = Field(
        default=None, alias="EntryNo-1", description="Entry No.. Max length: 4"
    )
    JobNo: str | None = Field(default=None, alias="JobNo-2", description="Job No.. Max length: 20")
    PostingDate: datetime.date | None = Field(
        default=None, alias="PostingDate-3", description="Posting Date. Max length: 4"
    )
    DocumentNo: str | None = Field(
        default=None, alias="DocumentNo-4", description="Document No.. Max length: 20"
    )
    Type: int | None = Field(default=None, alias="Type-5", description="Type. Max length: 4")
    No: str | None = Field(default=None, alias="No-7", description="No.. Max length: 20")
    Description: str | None = Field(
        default=None, alias="Description-8", description="Description. Max length: 100"
    )
    Quantity: float | None = Field(
        default=None, alias="Quantity-9", description="Quantity. Max length: 12"
    )
    DirectUnitCostLCY: float | None = Field(
        default=None,
        alias="DirectUnitCostLCY-11",
        description="Direct Unit Cost (LCY). Max length: 12",
    )
    UnitCostLCY: float | None = Field(
        default=None, alias="UnitCostLCY-12", description="Unit Cost (LCY). Max length: 12"
    )
    TotalCostLCY: float | None = Field(
        default=None, alias="TotalCostLCY-13", description="Total Cost (LCY). Max length: 12"
    )
    UnitPriceLCY: float | None = Field(
        default=None, alias="UnitPriceLCY-14", description="Unit Price (LCY). Max length: 12"
    )
    TotalPriceLCY: float | None = Field(
        default=None, alias="TotalPriceLCY-15", description="Total Price (LCY). Max length: 12"
    )
    ResourceGroupNo: str | None = Field(
        default=None, alias="ResourceGroupNo-16", description="Resource Group No.. Max length: 20"
    )
    UnitofMeasureCode: str | None = Field(
        default=None,
        alias="UnitofMeasureCode-17",
        description="Unit of Measure Code. Max length: 10",
    )
    LocationCode: str | None = Field(
        default=None, alias="LocationCode-20", description="Location Code. Max length: 10"
    )
    JobPostingGroup: str | None = Field(
        default=None, alias="JobPostingGroup-29", description="Job Posting Group. Max length: 20"
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-30",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-31",
        description="Global Dimension 2 Code. Max length: 20",
    )
    WorkTypeCode: str | None = Field(
        default=None, alias="WorkTypeCode-32", description="Work Type Code. Max length: 10"
    )
    CustomerPriceGroup: str | None = Field(
        default=None,
        alias="CustomerPriceGroup-33",
        description="Customer Price Group. Max length: 10",
    )
    UserID: str | None = Field(
        default=None, alias="UserID-37", description="User ID. Max length: 50"
    )
    SourceCode: str | None = Field(
        default=None, alias="SourceCode-38", description="Source Code. Max length: 10"
    )
    ShptMethodCode: str | None = Field(
        default=None, alias="ShptMethodCode-40", description="Shpt. Method Code. Max length: 10"
    )
    AmttoPosttoGL: float | None = Field(
        default=None, alias="AmttoPosttoGL-60", description="Amt. to Post to G/L. Max length: 12"
    )
    AmtPostedtoGL: float | None = Field(
        default=None, alias="AmtPostedtoGL-61", description="Amt. Posted to G/L. Max length: 12"
    )
    EntryType: int | None = Field(
        default=None, alias="EntryType-64", description="Entry Type. Max length: 4"
    )
    JournalBatchName: str | None = Field(
        default=None, alias="JournalBatchName-75", description="Journal Batch Name. Max length: 10"
    )
    ReasonCode: str | None = Field(
        default=None, alias="ReasonCode-76", description="Reason Code. Max length: 10"
    )
    TransactionType: str | None = Field(
        default=None, alias="TransactionType-77", description="Transaction Type. Max length: 10"
    )
    TransportMethod: str | None = Field(
        default=None, alias="TransportMethod-78", description="Transport Method. Max length: 10"
    )
    CountryRegionCode: str | None = Field(
        default=None,
        alias="CountryRegionCode-79",
        description="Country/Region Code. Max length: 10",
    )
    GenBusPostingGroup: str | None = Field(
        default=None,
        alias="GenBusPostingGroup-80",
        description="Gen. Bus. Posting Group. Max length: 20",
    )
    GenProdPostingGroup: str | None = Field(
        default=None,
        alias="GenProdPostingGroup-81",
        description="Gen. Prod. Posting Group. Max length: 20",
    )
    EntryExitPoint: str | None = Field(
        default=None, alias="EntryExitPoint-82", description="Entry/Exit Point. Max length: 10"
    )
    DocumentDate: datetime.date | None = Field(
        default=None, alias="DocumentDate-83", description="Document Date. Max length: 4"
    )
    ExternalDocumentNo: str | None = Field(
        default=None,
        alias="ExternalDocumentNo-84",
        description="External Document No.. Max length: 35",
    )
    Area: str | None = Field(default=None, alias="Area-85", description="Area. Max length: 10")
    TransactionSpecification: str | None = Field(
        default=None,
        alias="TransactionSpecification-86",
        description="Transaction Specification. Max length: 10",
    )
    NoSeries: str | None = Field(
        default=None, alias="NoSeries-87", description="No. Series. Max length: 20"
    )
    AdditionalCurrencyTotalCost: float | None = Field(
        default=None,
        alias="AdditionalCurrencyTotalCost-88",
        description="Additional-Currency Total Cost. Max length: 12",
    )
    AddCurrencyTotalPrice: float | None = Field(
        default=None,
        alias="AddCurrencyTotalPrice-89",
        description="Add.-Currency Total Price. Max length: 12",
    )
    AddCurrencyLineAmount: float | None = Field(
        default=None,
        alias="AddCurrencyLineAmount-94",
        description="Add.-Currency Line Amount. Max length: 12",
    )
    DimensionSetID: int | None = Field(
        default=None, alias="DimensionSetID-480", description="Dimension Set ID. Max length: 4"
    )
    JobTaskNo: str | None = Field(
        default=None, alias="JobTaskNo-1000", description="Job Task No.. Max length: 20"
    )
    LineAmountLCY: float | None = Field(
        default=None, alias="LineAmountLCY-1001", description="Line Amount (LCY). Max length: 12"
    )
    UnitCost: float | None = Field(
        default=None, alias="UnitCost-1002", description="Unit Cost. Max length: 12"
    )
    TotalCost: float | None = Field(
        default=None, alias="TotalCost-1003", description="Total Cost. Max length: 12"
    )
    UnitPrice: float | None = Field(
        default=None, alias="UnitPrice-1004", description="Unit Price. Max length: 12"
    )
    TotalPrice: float | None = Field(
        default=None, alias="TotalPrice-1005", description="Total Price. Max length: 12"
    )
    LineAmount: float | None = Field(
        default=None, alias="LineAmount-1006", description="Line Amount. Max length: 12"
    )
    LineDiscountAmount: float | None = Field(
        default=None,
        alias="LineDiscountAmount-1007",
        description="Line Discount Amount. Max length: 12",
    )
    LineDiscountAmountLCY: float | None = Field(
        default=None,
        alias="LineDiscountAmountLCY-1008",
        description="Line Discount Amount (LCY). Max length: 12",
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-1009", description="Currency Code. Max length: 10"
    )
    CurrencyFactor: float | None = Field(
        default=None, alias="CurrencyFactor-1010", description="Currency Factor. Max length: 12"
    )
    Description2: str | None = Field(
        default=None, alias="Description2-1016", description="Description 2. Max length: 50"
    )
    LedgerEntryType: int | None = Field(
        default=None, alias="LedgerEntryType-1017", description="Ledger Entry Type. Max length: 4"
    )
    LedgerEntryNo: int | None = Field(
        default=None, alias="LedgerEntryNo-1018", description="Ledger Entry No.. Max length: 4"
    )
    SerialNo: str | None = Field(
        default=None, alias="SerialNo-1019", description="Serial No.. Max length: 50"
    )
    LotNo: str | None = Field(
        default=None, alias="LotNo-1020", description="Lot No.. Max length: 50"
    )
    LineDiscount: float | None = Field(
        default=None, alias="LineDiscount-1021", description="Line Discount %. Max length: 12"
    )
    LineType: int | None = Field(
        default=None, alias="LineType-1022", description="Line Type. Max length: 4"
    )
    OriginalUnitCostLCY: float | None = Field(
        default=None,
        alias="OriginalUnitCostLCY-1023",
        description="Original Unit Cost (LCY). Max length: 12",
    )
    OriginalTotalCostLCY: float | None = Field(
        default=None,
        alias="OriginalTotalCostLCY-1024",
        description="Original Total Cost (LCY). Max length: 12",
    )
    OriginalUnitCost: float | None = Field(
        default=None,
        alias="OriginalUnitCost-1025",
        description="Original Unit Cost. Max length: 12",
    )
    OriginalTotalCost: float | None = Field(
        default=None,
        alias="OriginalTotalCost-1026",
        description="Original Total Cost. Max length: 12",
    )
    OriginalTotalCostACY: float | None = Field(
        default=None,
        alias="OriginalTotalCostACY-1027",
        description="Original Total Cost (ACY). Max length: 12",
    )
    Adjusted: bool | None = Field(
        default=None, alias="Adjusted-1028", description="Adjusted. Max length: 4"
    )
    DateTimeAdjusted: datetime.datetime | None = Field(
        default=None, alias="DateTimeAdjusted-1029", description="DateTime Adjusted. Max length: 8"
    )
    VariantCode: str | None = Field(
        default=None, alias="VariantCode-5402", description="Variant Code. Max length: 10"
    )
    BinCode: str | None = Field(
        default=None, alias="BinCode-5403", description="Bin Code. Max length: 20"
    )
    QtyperUnitofMeasure: float | None = Field(
        default=None,
        alias="QtyperUnitofMeasure-5404",
        description="Qty. per Unit of Measure. Max length: 12",
    )
    QuantityBase: float | None = Field(
        default=None, alias="QuantityBase-5405", description="Quantity (Base). Max length: 12"
    )
    ServiceOrderNo: str | None = Field(
        default=None, alias="ServiceOrderNo-5900", description="Service Order No.. Max length: 20"
    )
    PostedServiceShipmentNo: str | None = Field(
        default=None,
        alias="PostedServiceShipmentNo-5901",
        description="Posted Service Shipment No.. Max length: 20",
    )
    PackageNo: str | None = Field(
        default=None, alias="PackageNo-6515", description="Package No.. Max length: 50"
    )
    PTEOldPhaseCode: str | None = Field(
        default=None,
        alias="PTEOldPhaseCode-65000",
        description="PTE Old Phase Code. Max length: 10",
    )
    PTEJobAnalyzesType: int | None = Field(
        default=None,
        alias="PTEJobAnalyzesType-65001",
        description="PTE Job Analyzes Type. Max length: 4",
    )
    PTECustomerNo: str | None = Field(
        default=None, alias="PTECustomerNo-65002", description="PTE Customer No.. Max length: 20"
    )
    PTECustomerName: str | None = Field(
        default=None, alias="PTECustomerName-65003", description="PTE Customer Name. Max length: 50"
    )
    PTEEmployeeDivision: str | None = Field(
        default=None,
        alias="PTEEmployeeDivision-65004",
        description="PTE Employee Division. Max length: 20",
    )
    PTEJiraWorklogid: int | None = Field(
        default=None,
        alias="PTEJiraWorklogid-65005",
        description="PTE Jira Worklog_id. Max length: 4",
    )
    PTEDescription3: str | None = Field(
        default=None, alias="PTEDescription3-65020", description="PTE Description3. Max length: 80"
    )
    WJOBReviewedby: str | None = Field(
        default=None,
        alias="WJOBReviewedby-10007200",
        description="WJOB Reviewed by. Max length: 50",
    )
    WJOBWork: bool | None = Field(
        default=None, alias="WJOBWork-10007201", description="WJOB Work. Max length: 4"
    )
    WJOBJobType: str | None = Field(
        default=None, alias="WJOBJobType-10007202", description="WJOB Job Type. Max length: 15"
    )
    WJOBManuallyModified: bool | None = Field(
        default=None,
        alias="WJOBManuallyModified-10007203",
        description="WJOB Manually Modified. Max length: 4",
    )
    WJOBReviewed: bool | None = Field(
        default=None, alias="WJOBReviewed-10007204", description="WJOB Reviewed. Max length: 4"
    )
    WJOBRegbyResource: str | None = Field(
        default=None,
        alias="WJOBRegbyResource-10007206",
        description="WJOB Reg. by Resource. Max length: 20",
    )
    WJOBResourceName: str | None = Field(
        default=None,
        alias="WJOBResourceName-10007208",
        description="WJOB Resource Name. Max length: 100",
    )
    WJobExternalDocument2: str | None = Field(
        default=None,
        alias="WJobExternalDocument2-10007210",
        description="WJob External Document 2. Max length: 30",
    )
    WJobPAProformaNo: str | None = Field(
        default=None,
        alias="WJobPAProformaNo-10007255",
        description="WJobPA Proforma No.. Max length: 20",
    )
    WJobJRRequestNo: str | None = Field(
        default=None,
        alias="WJobJRRequestNo-10007270",
        description="WJobJR Request No.. Max length: 20",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class Resource(SparkModel):
    """Represents the table Resource

    Display Name: Resource
    Entity Name: Resource-156
    """

    No: str | None = Field(default=None, alias="No-1", description="No.. Max length: 20")
    Type: int | None = Field(default=None, alias="Type-2", description="Type. Max length: 4")
    Name: str | None = Field(default=None, alias="Name-3", description="Name. Max length: 100")
    Name2: str | None = Field(default=None, alias="Name2-5", description="Name 2. Max length: 50")
    Address: str | None = Field(
        default=None, alias="Address-6", description="Address. Max length: 100"
    )
    Address2: str | None = Field(
        default=None, alias="Address2-7", description="Address 2. Max length: 50"
    )
    City: str | None = Field(default=None, alias="City-8", description="City. Max length: 30")
    SocialSecurityNo: str | None = Field(
        default=None, alias="SocialSecurityNo-9", description="Social Security No.. Max length: 30"
    )
    JobTitle: str | None = Field(
        default=None, alias="JobTitle-10", description="Job Title. Max length: 30"
    )
    Education: str | None = Field(
        default=None, alias="Education-11", description="Education. Max length: 30"
    )
    EmploymentDate: datetime.date | None = Field(
        default=None, alias="EmploymentDate-13", description="Employment Date. Max length: 4"
    )
    ResourceGroupNo: str | None = Field(
        default=None, alias="ResourceGroupNo-14", description="Resource Group No.. Max length: 20"
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-16",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-17",
        description="Global Dimension 2 Code. Max length: 20",
    )
    BaseUnitofMeasure: str | None = Field(
        default=None,
        alias="BaseUnitofMeasure-18",
        description="Base Unit of Measure. Max length: 10",
    )
    DirectUnitCost: float | None = Field(
        default=None, alias="DirectUnitCost-19", description="Direct Unit Cost. Max length: 12"
    )
    UnitCost: float | None = Field(
        default=None, alias="UnitCost-21", description="Unit Cost. Max length: 12"
    )
    UnitPrice: float | None = Field(
        default=None, alias="UnitPrice-24", description="Unit Price. Max length: 12"
    )
    VendorNo: str | None = Field(
        default=None, alias="VendorNo-25", description="Vendor No.. Max length: 20"
    )
    Blocked: bool | None = Field(
        default=None, alias="Blocked-38", description="Blocked. Max length: 4"
    )
    GenProdPostingGroup: str | None = Field(
        default=None,
        alias="GenProdPostingGroup-51",
        description="Gen. Prod. Posting Group. Max length: 20",
    )
    PostCode: str | None = Field(
        default=None, alias="PostCode-53", description="Post Code. Max length: 20"
    )
    County: str | None = Field(
        default=None, alias="County-54", description="County. Max length: 30"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class SalesInvoiceHeader(SparkModel):
    """Represents the table Sales Invoice Header

    Display Name: Sales Invoice Header
    Entity Name: SalesInvoiceHeader-112
    """

    SelltoCustomerNo: str | None = Field(
        default=None, alias="SelltoCustomerNo-2", description="Sell-to Customer No.. Max length: 20"
    )
    No: str | None = Field(default=None, alias="No-3", description="No.. Max length: 20")
    BilltoCustomerNo: str | None = Field(
        default=None, alias="BilltoCustomerNo-4", description="Bill-to Customer No.. Max length: 20"
    )
    BilltoName: str | None = Field(
        default=None, alias="BilltoName-5", description="Bill-to Name. Max length: 100"
    )
    BilltoName2: str | None = Field(
        default=None, alias="BilltoName2-6", description="Bill-to Name 2. Max length: 50"
    )
    BilltoAddress: str | None = Field(
        default=None, alias="BilltoAddress-7", description="Bill-to Address. Max length: 100"
    )
    BilltoAddress2: str | None = Field(
        default=None, alias="BilltoAddress2-8", description="Bill-to Address 2. Max length: 50"
    )
    BilltoCity: str | None = Field(
        default=None, alias="BilltoCity-9", description="Bill-to City. Max length: 30"
    )
    BilltoContact: str | None = Field(
        default=None, alias="BilltoContact-10", description="Bill-to Contact. Max length: 100"
    )
    YourReference: str | None = Field(
        default=None, alias="YourReference-11", description="Your Reference. Max length: 35"
    )
    ShiptoCode: str | None = Field(
        default=None, alias="ShiptoCode-12", description="Ship-to Code. Max length: 10"
    )
    ShiptoName: str | None = Field(
        default=None, alias="ShiptoName-13", description="Ship-to Name. Max length: 100"
    )
    ShiptoName2: str | None = Field(
        default=None, alias="ShiptoName2-14", description="Ship-to Name 2. Max length: 50"
    )
    ShiptoAddress: str | None = Field(
        default=None, alias="ShiptoAddress-15", description="Ship-to Address. Max length: 100"
    )
    ShiptoAddress2: str | None = Field(
        default=None, alias="ShiptoAddress2-16", description="Ship-to Address 2. Max length: 50"
    )
    ShiptoCity: str | None = Field(
        default=None, alias="ShiptoCity-17", description="Ship-to City. Max length: 30"
    )
    ShiptoContact: str | None = Field(
        default=None, alias="ShiptoContact-18", description="Ship-to Contact. Max length: 100"
    )
    OrderDate: datetime.date | None = Field(
        default=None, alias="OrderDate-19", description="Order Date. Max length: 4"
    )
    PostingDate: datetime.date | None = Field(
        default=None, alias="PostingDate-20", description="Posting Date. Max length: 4"
    )
    ShipmentDate: datetime.date | None = Field(
        default=None, alias="ShipmentDate-21", description="Shipment Date. Max length: 4"
    )
    PostingDescription: str | None = Field(
        default=None,
        alias="PostingDescription-22",
        description="Posting Description. Max length: 100",
    )
    PaymentTermsCode: str | None = Field(
        default=None, alias="PaymentTermsCode-23", description="Payment Terms Code. Max length: 10"
    )
    DueDate: datetime.date | None = Field(
        default=None, alias="DueDate-24", description="Due Date. Max length: 4"
    )
    PaymentDiscount: float | None = Field(
        default=None, alias="PaymentDiscount-25", description="Payment Discount %. Max length: 12"
    )
    PmtDiscountDate: datetime.date | None = Field(
        default=None, alias="PmtDiscountDate-26", description="Pmt. Discount Date. Max length: 4"
    )
    ShipmentMethodCode: str | None = Field(
        default=None,
        alias="ShipmentMethodCode-27",
        description="Shipment Method Code. Max length: 10",
    )
    LocationCode: str | None = Field(
        default=None, alias="LocationCode-28", description="Location Code. Max length: 10"
    )
    ShortcutDimension1Code: str | None = Field(
        default=None,
        alias="ShortcutDimension1Code-29",
        description="Shortcut Dimension 1 Code. Max length: 20",
    )
    ShortcutDimension2Code: str | None = Field(
        default=None,
        alias="ShortcutDimension2Code-30",
        description="Shortcut Dimension 2 Code. Max length: 20",
    )
    CustomerPostingGroup: str | None = Field(
        default=None,
        alias="CustomerPostingGroup-31",
        description="Customer Posting Group. Max length: 20",
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-32", description="Currency Code. Max length: 10"
    )
    CurrencyFactor: float | None = Field(
        default=None, alias="CurrencyFactor-33", description="Currency Factor. Max length: 12"
    )
    CustomerPriceGroup: str | None = Field(
        default=None,
        alias="CustomerPriceGroup-34",
        description="Customer Price Group. Max length: 10",
    )
    PricesIncludingVAT: bool | None = Field(
        default=None,
        alias="PricesIncludingVAT-35",
        description="Prices Including VAT. Max length: 4",
    )
    InvoiceDiscCode: str | None = Field(
        default=None, alias="InvoiceDiscCode-37", description="Invoice Disc. Code. Max length: 20"
    )
    CustomerDiscGroup: str | None = Field(
        default=None,
        alias="CustomerDiscGroup-40",
        description="Customer Disc. Group. Max length: 20",
    )
    LanguageCode: str | None = Field(
        default=None, alias="LanguageCode-41", description="Language Code. Max length: 10"
    )
    FormatRegion: str | None = Field(
        default=None, alias="FormatRegion-42", description="Format Region. Max length: 80"
    )
    SalespersonCode: str | None = Field(
        default=None, alias="SalespersonCode-43", description="Salesperson Code. Max length: 20"
    )
    OrderNo: str | None = Field(
        default=None, alias="OrderNo-44", description="Order No.. Max length: 20"
    )
    NoPrinted: int | None = Field(
        default=None, alias="NoPrinted-47", description="No. Printed. Max length: 4"
    )
    OnHold: str | None = Field(
        default=None, alias="OnHold-51", description="On Hold. Max length: 3"
    )
    AppliestoDocType: int | None = Field(
        default=None, alias="AppliestoDocType-52", description="Applies-to Doc. Type. Max length: 4"
    )
    AppliestoDocNo: str | None = Field(
        default=None, alias="AppliestoDocNo-53", description="Applies-to Doc. No.. Max length: 20"
    )
    BalAccountNo: str | None = Field(
        default=None, alias="BalAccountNo-55", description="Bal. Account No.. Max length: 20"
    )
    VATRegistrationNo: str | None = Field(
        default=None,
        alias="VATRegistrationNo-70",
        description="VAT Registration No.. Max length: 20",
    )
    ReasonCode: str | None = Field(
        default=None, alias="ReasonCode-73", description="Reason Code. Max length: 10"
    )
    GenBusPostingGroup: str | None = Field(
        default=None,
        alias="GenBusPostingGroup-74",
        description="Gen. Bus. Posting Group. Max length: 20",
    )
    EU3PartyTrade: bool | None = Field(
        default=None, alias="EU3PartyTrade-75", description="EU 3-Party Trade. Max length: 4"
    )
    TransactionType: str | None = Field(
        default=None, alias="TransactionType-76", description="Transaction Type. Max length: 10"
    )
    TransportMethod: str | None = Field(
        default=None, alias="TransportMethod-77", description="Transport Method. Max length: 10"
    )
    VATCountryRegionCode: str | None = Field(
        default=None,
        alias="VATCountryRegionCode-78",
        description="VAT Country/Region Code. Max length: 10",
    )
    SelltoCustomerName: str | None = Field(
        default=None,
        alias="SelltoCustomerName-79",
        description="Sell-to Customer Name. Max length: 100",
    )
    SelltoCustomerName2: str | None = Field(
        default=None,
        alias="SelltoCustomerName2-80",
        description="Sell-to Customer Name 2. Max length: 50",
    )
    SelltoAddress: str | None = Field(
        default=None, alias="SelltoAddress-81", description="Sell-to Address. Max length: 100"
    )
    SelltoAddress2: str | None = Field(
        default=None, alias="SelltoAddress2-82", description="Sell-to Address 2. Max length: 50"
    )
    SelltoCity: str | None = Field(
        default=None, alias="SelltoCity-83", description="Sell-to City. Max length: 30"
    )
    SelltoContact: str | None = Field(
        default=None, alias="SelltoContact-84", description="Sell-to Contact. Max length: 100"
    )
    BilltoPostCode: str | None = Field(
        default=None, alias="BilltoPostCode-85", description="Bill-to Post Code. Max length: 20"
    )
    BilltoCounty: str | None = Field(
        default=None, alias="BilltoCounty-86", description="Bill-to County. Max length: 30"
    )
    BilltoCountryRegionCode: str | None = Field(
        default=None,
        alias="BilltoCountryRegionCode-87",
        description="Bill-to Country/Region Code. Max length: 10",
    )
    SelltoPostCode: str | None = Field(
        default=None, alias="SelltoPostCode-88", description="Sell-to Post Code. Max length: 20"
    )
    SelltoCounty: str | None = Field(
        default=None, alias="SelltoCounty-89", description="Sell-to County. Max length: 30"
    )
    SelltoCountryRegionCode: str | None = Field(
        default=None,
        alias="SelltoCountryRegionCode-90",
        description="Sell-to Country/Region Code. Max length: 10",
    )
    ShiptoPostCode: str | None = Field(
        default=None, alias="ShiptoPostCode-91", description="Ship-to Post Code. Max length: 20"
    )
    ShiptoCounty: str | None = Field(
        default=None, alias="ShiptoCounty-92", description="Ship-to County. Max length: 30"
    )
    ShiptoCountryRegionCode: str | None = Field(
        default=None,
        alias="ShiptoCountryRegionCode-93",
        description="Ship-to Country/Region Code. Max length: 10",
    )
    BalAccountType: int | None = Field(
        default=None, alias="BalAccountType-94", description="Bal. Account Type. Max length: 4"
    )
    ExitPoint: str | None = Field(
        default=None, alias="ExitPoint-97", description="Exit Point. Max length: 10"
    )
    Correction: bool | None = Field(
        default=None, alias="Correction-98", description="Correction. Max length: 4"
    )
    DocumentDate: datetime.date | None = Field(
        default=None, alias="DocumentDate-99", description="Document Date. Max length: 4"
    )
    ExternalDocumentNo: str | None = Field(
        default=None,
        alias="ExternalDocumentNo-100",
        description="External Document No.. Max length: 35",
    )
    Area: str | None = Field(default=None, alias="Area-101", description="Area. Max length: 10")
    TransactionSpecification: str | None = Field(
        default=None,
        alias="TransactionSpecification-102",
        description="Transaction Specification. Max length: 10",
    )
    PaymentMethodCode: str | None = Field(
        default=None,
        alias="PaymentMethodCode-104",
        description="Payment Method Code. Max length: 10",
    )
    ShippingAgentCode: str | None = Field(
        default=None,
        alias="ShippingAgentCode-105",
        description="Shipping Agent Code. Max length: 10",
    )
    PackageTrackingNo: str | None = Field(
        default=None,
        alias="PackageTrackingNo-106",
        description="Package Tracking No.. Max length: 30",
    )
    PreAssignedNoSeries: str | None = Field(
        default=None,
        alias="PreAssignedNoSeries-107",
        description="Pre-Assigned No. Series. Max length: 20",
    )
    NoSeries: str | None = Field(
        default=None, alias="NoSeries-108", description="No. Series. Max length: 20"
    )
    OrderNoSeries: str | None = Field(
        default=None, alias="OrderNoSeries-110", description="Order No. Series. Max length: 20"
    )
    PreAssignedNo: str | None = Field(
        default=None, alias="PreAssignedNo-111", description="Pre-Assigned No.. Max length: 20"
    )
    UserID: str | None = Field(
        default=None, alias="UserID-112", description="User ID. Max length: 50"
    )
    SourceCode: str | None = Field(
        default=None, alias="SourceCode-113", description="Source Code. Max length: 10"
    )
    TaxAreaCode: str | None = Field(
        default=None, alias="TaxAreaCode-114", description="Tax Area Code. Max length: 20"
    )
    TaxLiable: bool | None = Field(
        default=None, alias="TaxLiable-115", description="Tax Liable. Max length: 4"
    )
    VATBusPostingGroup: str | None = Field(
        default=None,
        alias="VATBusPostingGroup-116",
        description="VAT Bus. Posting Group. Max length: 20",
    )
    VATBaseDiscount: float | None = Field(
        default=None, alias="VATBaseDiscount-119", description="VAT Base Discount %. Max length: 12"
    )
    InvoiceDiscountCalculation: int | None = Field(
        default=None,
        alias="InvoiceDiscountCalculation-121",
        description="Invoice Discount Calculation. Max length: 4",
    )
    InvoiceDiscountValue: float | None = Field(
        default=None,
        alias="InvoiceDiscountValue-122",
        description="Invoice Discount Value. Max length: 12",
    )
    PrepaymentNoSeries: str | None = Field(
        default=None,
        alias="PrepaymentNoSeries-131",
        description="Prepayment No. Series. Max length: 20",
    )
    PrepaymentInvoice: bool | None = Field(
        default=None, alias="PrepaymentInvoice-136", description="Prepayment Invoice. Max length: 4"
    )
    PrepaymentOrderNo: str | None = Field(
        default=None,
        alias="PrepaymentOrderNo-137",
        description="Prepayment Order No.. Max length: 20",
    )
    QuoteNo: str | None = Field(
        default=None, alias="QuoteNo-151", description="Quote No.. Max length: 20"
    )
    CompanyBankAccountCode: str | None = Field(
        default=None,
        alias="CompanyBankAccountCode-163",
        description="Company Bank Account Code. Max length: 20",
    )
    SelltoPhoneNo: str | None = Field(
        default=None, alias="SelltoPhoneNo-171", description="Sell-to Phone No.. Max length: 30"
    )
    SelltoEMail: str | None = Field(
        default=None, alias="SelltoEMail-172", description="Sell-to E-Mail. Max length: 80"
    )
    VATReportingDate: datetime.date | None = Field(
        default=None, alias="VATReportingDate-179", description="VAT Reporting Date. Max length: 4"
    )
    PaymentReference: str | None = Field(
        default=None, alias="PaymentReference-180", description="Payment Reference. Max length: 50"
    )
    DimensionSetID: int | None = Field(
        default=None, alias="DimensionSetID-480", description="Dimension Set ID. Max length: 4"
    )
    PaymentServiceSetID: int | None = Field(
        default=None,
        alias="PaymentServiceSetID-600",
        description="Payment Service Set ID. Max length: 4",
    )
    DocumentExchangeIdentifier: str | None = Field(
        default=None,
        alias="DocumentExchangeIdentifier-710",
        description="Document Exchange Identifier. Max length: 50",
    )
    DocumentExchangeStatus: int | None = Field(
        default=None,
        alias="DocumentExchangeStatus-711",
        description="Document Exchange Status. Max length: 4",
    )
    DocExchOriginalIdentifier: str | None = Field(
        default=None,
        alias="DocExchOriginalIdentifier-712",
        description="Doc. Exch. Original Identifier. Max length: 50",
    )
    CoupledtoCRM: bool | None = Field(
        default=None, alias="CoupledtoCRM-720", description="Coupled to CRM. Max length: 4"
    )
    DirectDebitMandateID: str | None = Field(
        default=None,
        alias="DirectDebitMandateID-1200",
        description="Direct Debit Mandate ID. Max length: 35",
    )
    CustLedgerEntryNo: int | None = Field(
        default=None,
        alias="CustLedgerEntryNo-1304",
        description="Cust. Ledger Entry No.. Max length: 4",
    )
    CampaignNo: str | None = Field(
        default=None, alias="CampaignNo-5050", description="Campaign No.. Max length: 20"
    )
    SelltoContactNo: str | None = Field(
        default=None,
        alias="SelltoContactNo-5052",
        description="Sell-to Contact No.. Max length: 20",
    )
    BilltoContactNo: str | None = Field(
        default=None,
        alias="BilltoContactNo-5053",
        description="Bill-to Contact No.. Max length: 20",
    )
    OpportunityNo: str | None = Field(
        default=None, alias="OpportunityNo-5055", description="Opportunity No.. Max length: 20"
    )
    ResponsibilityCenter: str | None = Field(
        default=None,
        alias="ResponsibilityCenter-5700",
        description="Responsibility Center. Max length: 10",
    )
    PriceCalculationMethod: int | None = Field(
        default=None,
        alias="PriceCalculationMethod-7000",
        description="Price Calculation Method. Max length: 4",
    )
    AllowLineDisc: bool | None = Field(
        default=None, alias="AllowLineDisc-7001", description="Allow Line Disc.. Max length: 4"
    )
    GetShipmentUsed: bool | None = Field(
        default=None, alias="GetShipmentUsed-7200", description="Get Shipment Used. Max length: 4"
    )
    DraftInvoiceSystemId: str | None = Field(
        default=None,
        alias="DraftInvoiceSystemId-8001",
        description="Draft Invoice SystemId. Max length: 16",
    )
    ShpfyOrderId: int | None = Field(
        default=None, alias="ShpfyOrderId-30100", description="Shpfy Order Id. Max length: 8"
    )
    ShpfyOrderNo: str | None = Field(
        default=None, alias="ShpfyOrderNo-30101", description="Shpfy Order No.. Max length: 50"
    )
    PTENmerverkbeini: str | None = Field(
        default=None,
        alias="PTENmerverkbeini-65100",
        description="PTE Númer verkbeiðni. Max length: 10",
    )
    PTEDocumentSendingProfile: str | None = Field(
        default=None,
        alias="PTEDocumentSendingProfile-65101",
        description="PTE Document Sending Profile. Max length: 20",
    )
    PTEOCRband: str | None = Field(
        default=None, alias="PTEOCRband-65102", description="PTE OCR band. Max length: 80"
    )
    PTEBankAccountCode: str | None = Field(
        default=None,
        alias="PTEBankAccountCode-65103",
        description="PTE Bank Account Code. Max length: 20",
    )
    PTEUseSaleslines: bool | None = Field(
        default=None,
        alias="PTEUseSaleslines-65104",
        description="PTE Use Saleslines. Max length: 4",
    )
    AMSAgreementNo: str | None = Field(
        default=None,
        alias="AMSAgreementNo-10007050",
        description="AMSAgreement No.. Max length: 20",
    )
    AMSNextPeriodDate: datetime.date | None = Field(
        default=None,
        alias="AMSNextPeriodDate-10007052",
        description="AMSNext Period Date. Max length: 4",
    )
    AMSPeriod: datetime.date | None = Field(
        default=None, alias="AMSPeriod-10007053", description="AMSPeriod. Max length: 4"
    )
    AMSAgreementType: str | None = Field(
        default=None,
        alias="AMSAgreementType-10007055",
        description="AMSAgreement Type. Max length: 20",
    )
    AMSAgreementPeriodGroup: str | None = Field(
        default=None,
        alias="AMSAgreementPeriodGroup-10007056",
        description="AMSAgreement Period Group. Max length: 20",
    )
    AMSPeriodEntryNo: int | None = Field(
        default=None,
        alias="AMSPeriodEntryNo-10007059",
        description="AMSPeriod Entry No.. Max length: 4",
    )
    AMSPeriodInvoicingType: int | None = Field(
        default=None,
        alias="AMSPeriodInvoicingType-10007065",
        description="AMSPeriod Invoicing Type. Max length: 4",
    )
    AMSNotOverageRelated: bool | None = Field(
        default=None,
        alias="AMSNotOverageRelated-10007066",
        description="AMSNot Overage Related. Max length: 4",
    )
    AMSBillingRunBatchNo: int | None = Field(
        default=None,
        alias="AMSBillingRunBatchNo-10007067",
        description="AMSBilling Run Batch No.. Max length: 4",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class SalesInvoiceLine(SparkModel):
    """Represents the table Sales Invoice Line

    Display Name: Sales Invoice Line
    Entity Name: SalesInvoiceLine-113
    """

    SelltoCustomerNo: str | None = Field(
        default=None, alias="SelltoCustomerNo-2", description="Sell-to Customer No.. Max length: 20"
    )
    DocumentNo: str | None = Field(
        default=None, alias="DocumentNo-3", description="Document No.. Max length: 20"
    )
    LineNo: int | None = Field(
        default=None, alias="LineNo-4", description="Line No.. Max length: 4"
    )
    Type: int | None = Field(default=None, alias="Type-5", description="Type. Max length: 4")
    No: str | None = Field(default=None, alias="No-6", description="No.. Max length: 20")
    LocationCode: str | None = Field(
        default=None, alias="LocationCode-7", description="Location Code. Max length: 10"
    )
    PostingGroup: str | None = Field(
        default=None, alias="PostingGroup-8", description="Posting Group. Max length: 20"
    )
    ShipmentDate: datetime.date | None = Field(
        default=None, alias="ShipmentDate-10", description="Shipment Date. Max length: 4"
    )
    Description: str | None = Field(
        default=None, alias="Description-11", description="Description. Max length: 100"
    )
    Description2: str | None = Field(
        default=None, alias="Description2-12", description="Description 2. Max length: 50"
    )
    UnitofMeasure: str | None = Field(
        default=None, alias="UnitofMeasure-13", description="Unit of Measure. Max length: 50"
    )
    Quantity: float | None = Field(
        default=None, alias="Quantity-15", description="Quantity. Max length: 12"
    )
    UnitPrice: float | None = Field(
        default=None, alias="UnitPrice-22", description="Unit Price. Max length: 12"
    )
    UnitCostLCY: float | None = Field(
        default=None, alias="UnitCostLCY-23", description="Unit Cost (LCY). Max length: 12"
    )
    VAT: float | None = Field(default=None, alias="VAT-25", description="VAT %. Max length: 12")
    LineDiscount: float | None = Field(
        default=None, alias="LineDiscount-27", description="Line Discount %. Max length: 12"
    )
    LineDiscountAmount: float | None = Field(
        default=None,
        alias="LineDiscountAmount-28",
        description="Line Discount Amount. Max length: 12",
    )
    Amount: float | None = Field(
        default=None, alias="Amount-29", description="Amount. Max length: 12"
    )
    AmountIncludingVAT: float | None = Field(
        default=None,
        alias="AmountIncludingVAT-30",
        description="Amount Including VAT. Max length: 12",
    )
    AllowInvoiceDisc: bool | None = Field(
        default=None, alias="AllowInvoiceDisc-32", description="Allow Invoice Disc.. Max length: 4"
    )
    GrossWeight: float | None = Field(
        default=None, alias="GrossWeight-34", description="Gross Weight. Max length: 12"
    )
    NetWeight: float | None = Field(
        default=None, alias="NetWeight-35", description="Net Weight. Max length: 12"
    )
    UnitsperParcel: float | None = Field(
        default=None, alias="UnitsperParcel-36", description="Units per Parcel. Max length: 12"
    )
    UnitVolume: float | None = Field(
        default=None, alias="UnitVolume-37", description="Unit Volume. Max length: 12"
    )
    AppltoItemEntry: int | None = Field(
        default=None, alias="AppltoItemEntry-38", description="Appl.-to Item Entry. Max length: 4"
    )
    ShortcutDimension1Code: str | None = Field(
        default=None,
        alias="ShortcutDimension1Code-40",
        description="Shortcut Dimension 1 Code. Max length: 20",
    )
    ShortcutDimension2Code: str | None = Field(
        default=None,
        alias="ShortcutDimension2Code-41",
        description="Shortcut Dimension 2 Code. Max length: 20",
    )
    CustomerPriceGroup: str | None = Field(
        default=None,
        alias="CustomerPriceGroup-42",
        description="Customer Price Group. Max length: 10",
    )
    JobNo: str | None = Field(default=None, alias="JobNo-45", description="Job No.. Max length: 20")
    WorkTypeCode: str | None = Field(
        default=None, alias="WorkTypeCode-52", description="Work Type Code. Max length: 10"
    )
    ShipmentNo: str | None = Field(
        default=None, alias="ShipmentNo-63", description="Shipment No.. Max length: 20"
    )
    ShipmentLineNo: int | None = Field(
        default=None, alias="ShipmentLineNo-64", description="Shipment Line No.. Max length: 4"
    )
    OrderNo: str | None = Field(
        default=None, alias="OrderNo-65", description="Order No.. Max length: 20"
    )
    OrderLineNo: int | None = Field(
        default=None, alias="OrderLineNo-66", description="Order Line No.. Max length: 4"
    )
    BilltoCustomerNo: str | None = Field(
        default=None,
        alias="BilltoCustomerNo-68",
        description="Bill-to Customer No.. Max length: 20",
    )
    InvDiscountAmount: float | None = Field(
        default=None,
        alias="InvDiscountAmount-69",
        description="Inv. Discount Amount. Max length: 12",
    )
    DropShipment: bool | None = Field(
        default=None, alias="DropShipment-73", description="Drop Shipment. Max length: 4"
    )
    GenBusPostingGroup: str | None = Field(
        default=None,
        alias="GenBusPostingGroup-74",
        description="Gen. Bus. Posting Group. Max length: 20",
    )
    GenProdPostingGroup: str | None = Field(
        default=None,
        alias="GenProdPostingGroup-75",
        description="Gen. Prod. Posting Group. Max length: 20",
    )
    VATCalculationType: int | None = Field(
        default=None,
        alias="VATCalculationType-77",
        description="VAT Calculation Type. Max length: 4",
    )
    TransactionType: str | None = Field(
        default=None, alias="TransactionType-78", description="Transaction Type. Max length: 10"
    )
    TransportMethod: str | None = Field(
        default=None, alias="TransportMethod-79", description="Transport Method. Max length: 10"
    )
    AttachedtoLineNo: int | None = Field(
        default=None, alias="AttachedtoLineNo-80", description="Attached to Line No.. Max length: 4"
    )
    ExitPoint: str | None = Field(
        default=None, alias="ExitPoint-81", description="Exit Point. Max length: 10"
    )
    Area: str | None = Field(default=None, alias="Area-82", description="Area. Max length: 10")
    TransactionSpecification: str | None = Field(
        default=None,
        alias="TransactionSpecification-83",
        description="Transaction Specification. Max length: 10",
    )
    TaxCategory: str | None = Field(
        default=None, alias="TaxCategory-84", description="Tax Category. Max length: 10"
    )
    TaxAreaCode: str | None = Field(
        default=None, alias="TaxAreaCode-85", description="Tax Area Code. Max length: 20"
    )
    TaxLiable: bool | None = Field(
        default=None, alias="TaxLiable-86", description="Tax Liable. Max length: 4"
    )
    TaxGroupCode: str | None = Field(
        default=None, alias="TaxGroupCode-87", description="Tax Group Code. Max length: 20"
    )
    VATClauseCode: str | None = Field(
        default=None, alias="VATClauseCode-88", description="VAT Clause Code. Max length: 20"
    )
    VATBusPostingGroup: str | None = Field(
        default=None,
        alias="VATBusPostingGroup-89",
        description="VAT Bus. Posting Group. Max length: 20",
    )
    VATProdPostingGroup: str | None = Field(
        default=None,
        alias="VATProdPostingGroup-90",
        description="VAT Prod. Posting Group. Max length: 20",
    )
    BlanketOrderNo: str | None = Field(
        default=None, alias="BlanketOrderNo-97", description="Blanket Order No.. Max length: 20"
    )
    BlanketOrderLineNo: int | None = Field(
        default=None,
        alias="BlanketOrderLineNo-98",
        description="Blanket Order Line No.. Max length: 4",
    )
    VATBaseAmount: float | None = Field(
        default=None, alias="VATBaseAmount-99", description="VAT Base Amount. Max length: 12"
    )
    UnitCost: float | None = Field(
        default=None, alias="UnitCost-100", description="Unit Cost. Max length: 12"
    )
    SystemCreatedEntry: bool | None = Field(
        default=None,
        alias="SystemCreatedEntry-101",
        description="System-Created Entry. Max length: 4",
    )
    LineAmount: float | None = Field(
        default=None, alias="LineAmount-103", description="Line Amount. Max length: 12"
    )
    VATDifference: float | None = Field(
        default=None, alias="VATDifference-104", description="VAT Difference. Max length: 12"
    )
    VATIdentifier: str | None = Field(
        default=None, alias="VATIdentifier-106", description="VAT Identifier. Max length: 20"
    )
    ICPartnerRefType: int | None = Field(
        default=None,
        alias="ICPartnerRefType-107",
        description="IC Partner Ref. Type. Max length: 4",
    )
    ICPartnerReference: str | None = Field(
        default=None,
        alias="ICPartnerReference-108",
        description="IC Partner Reference. Max length: 20",
    )
    PrepaymentLine: bool | None = Field(
        default=None, alias="PrepaymentLine-123", description="Prepayment Line. Max length: 4"
    )
    ICPartnerCode: str | None = Field(
        default=None, alias="ICPartnerCode-130", description="IC Partner Code. Max length: 20"
    )
    PostingDate: datetime.date | None = Field(
        default=None, alias="PostingDate-131", description="Posting Date. Max length: 4"
    )
    ICItemReferenceNo: str | None = Field(
        default=None,
        alias="ICItemReferenceNo-138",
        description="IC Item Reference No.. Max length: 50",
    )
    PmtDiscountAmount: float | None = Field(
        default=None,
        alias="PmtDiscountAmount-145",
        description="Pmt. Discount Amount. Max length: 12",
    )
    LineDiscountCalculation: int | None = Field(
        default=None,
        alias="LineDiscountCalculation-180",
        description="Line Discount Calculation. Max length: 4",
    )
    DimensionSetID: int | None = Field(
        default=None, alias="DimensionSetID-480", description="Dimension Set ID. Max length: 4"
    )
    JobTaskNo: str | None = Field(
        default=None, alias="JobTaskNo-1001", description="Job Task No.. Max length: 20"
    )
    JobContractEntryNo: int | None = Field(
        default=None,
        alias="JobContractEntryNo-1002",
        description="Job Contract Entry No.. Max length: 4",
    )
    DeferralCode: str | None = Field(
        default=None, alias="DeferralCode-1700", description="Deferral Code. Max length: 10"
    )
    AllocationAccountNo: str | None = Field(
        default=None,
        alias="AllocationAccountNo-2678",
        description="Allocation Account No.. Max length: 20",
    )
    VariantCode: str | None = Field(
        default=None, alias="VariantCode-5402", description="Variant Code. Max length: 10"
    )
    BinCode: str | None = Field(
        default=None, alias="BinCode-5403", description="Bin Code. Max length: 20"
    )
    QtyperUnitofMeasure: float | None = Field(
        default=None,
        alias="QtyperUnitofMeasure-5404",
        description="Qty. per Unit of Measure. Max length: 12",
    )
    UnitofMeasureCode: str | None = Field(
        default=None,
        alias="UnitofMeasureCode-5407",
        description="Unit of Measure Code. Max length: 10",
    )
    QuantityBase: float | None = Field(
        default=None, alias="QuantityBase-5415", description="Quantity (Base). Max length: 12"
    )
    FAPostingDate: datetime.date | None = Field(
        default=None, alias="FAPostingDate-5600", description="FA Posting Date. Max length: 4"
    )
    DepreciationBookCode: str | None = Field(
        default=None,
        alias="DepreciationBookCode-5602",
        description="Depreciation Book Code. Max length: 10",
    )
    DepruntilFAPostingDate: bool | None = Field(
        default=None,
        alias="DepruntilFAPostingDate-5605",
        description="Depr. until FA Posting Date. Max length: 4",
    )
    DuplicateinDepreciationBook: str | None = Field(
        default=None,
        alias="DuplicateinDepreciationBook-5612",
        description="Duplicate in Depreciation Book. Max length: 10",
    )
    UseDuplicationList: bool | None = Field(
        default=None,
        alias="UseDuplicationList-5613",
        description="Use Duplication List. Max length: 4",
    )
    ResponsibilityCenter: str | None = Field(
        default=None,
        alias="ResponsibilityCenter-5700",
        description="Responsibility Center. Max length: 10",
    )
    ItemCategoryCode: str | None = Field(
        default=None,
        alias="ItemCategoryCode-5709",
        description="Item Category Code. Max length: 20",
    )
    Nonstock: bool | None = Field(
        default=None, alias="Nonstock-5710", description="Nonstock. Max length: 4"
    )
    PurchasingCode: str | None = Field(
        default=None, alias="PurchasingCode-5711", description="Purchasing Code. Max length: 10"
    )
    ItemReferenceNo: str | None = Field(
        default=None, alias="ItemReferenceNo-5725", description="Item Reference No.. Max length: 50"
    )
    ItemReferenceUnitofMeasure: str | None = Field(
        default=None,
        alias="ItemReferenceUnitofMeasure-5726",
        description="Item Reference Unit of Measure. Max length: 10",
    )
    ItemReferenceType: int | None = Field(
        default=None,
        alias="ItemReferenceType-5727",
        description="Item Reference Type. Max length: 4",
    )
    ItemReferenceTypeNo: str | None = Field(
        default=None,
        alias="ItemReferenceTypeNo-5728",
        description="Item Reference Type No.. Max length: 30",
    )
    ApplfromItemEntry: int | None = Field(
        default=None,
        alias="ApplfromItemEntry-5811",
        description="Appl.-from Item Entry. Max length: 4",
    )
    ReturnReasonCode: str | None = Field(
        default=None,
        alias="ReturnReasonCode-6608",
        description="Return Reason Code. Max length: 10",
    )
    PriceCalculationMethod: int | None = Field(
        default=None,
        alias="PriceCalculationMethod-7000",
        description="Price Calculation Method. Max length: 4",
    )
    AllowLineDisc: bool | None = Field(
        default=None, alias="AllowLineDisc-7001", description="Allow Line Disc.. Max length: 4"
    )
    CustomerDiscGroup: str | None = Field(
        default=None,
        alias="CustomerDiscGroup-7002",
        description="Customer Disc. Group. Max length: 20",
    )
    Pricedescription: str | None = Field(
        default=None, alias="Pricedescription-7004", description="Price description. Max length: 80"
    )
    ShpfyOrderLineId: int | None = Field(
        default=None,
        alias="ShpfyOrderLineId-30100",
        description="Shpfy Order Line Id. Max length: 8",
    )
    ShpfyOrderNo: str | None = Field(
        default=None, alias="ShpfyOrderNo-30101", description="Shpfy Order No.. Max length: 50"
    )
    PTEDescription3: str | None = Field(
        default=None, alias="PTEDescription3-65008", description="PTE Description3. Max length: 80"
    )
    AMSAgreementNo: str | None = Field(
        default=None,
        alias="AMSAgreementNo-10007050",
        description="AMSAgreement No.. Max length: 20",
    )
    AMSIndex: str | None = Field(
        default=None, alias="AMSIndex-10007051", description="AMSIndex. Max length: 10"
    )
    AMSIndexBaseRate: float | None = Field(
        default=None,
        alias="AMSIndexBaseRate-10007052",
        description="AMSIndex Base Rate. Max length: 12",
    )
    AMSIndexRate: float | None = Field(
        default=None, alias="AMSIndexRate-10007053", description="AMSIndex Rate. Max length: 12"
    )
    AMSAgreementLineType: int | None = Field(
        default=None,
        alias="AMSAgreementLineType-10007054",
        description="AMSAgreement Line Type. Max length: 4",
    )
    AMSAgreementLineNo: str | None = Field(
        default=None,
        alias="AMSAgreementLineNo-10007055",
        description="AMSAgreement Line No.. Max length: 20",
    )
    AMSAgreementLineLineNo: int | None = Field(
        default=None,
        alias="AMSAgreementLineLineNo-10007059",
        description="AMSAgreement Line Line No.. Max length: 4",
    )
    AMSInvoicePrintingGroup: str | None = Field(
        default=None,
        alias="AMSInvoicePrintingGroup-10007060",
        description="AMSInvoice Printing Group. Max length: 20",
    )
    WJOBPrintOrder: int | None = Field(
        default=None, alias="WJOBPrintOrder-10007200", description="WJOB Print Order. Max length: 4"
    )
    WJOBCostResource: bool | None = Field(
        default=None,
        alias="WJOBCostResource-10007201",
        description="WJOB Cost Resource. Max length: 4",
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class Vendor(SparkModel):
    """Represents the table Vendor

    Display Name: Vendor
    Entity Name: Vendor-23
    """

    No: str | None = Field(default=None, alias="No-1", description="No.. Max length: 20")
    Name: str | None = Field(default=None, alias="Name-2", description="Name. Max length: 100")
    SearchName: str | None = Field(
        default=None, alias="SearchName-3", description="Search Name. Max length: 100"
    )
    Name2: str | None = Field(default=None, alias="Name2-4", description="Name 2. Max length: 50")
    Address: str | None = Field(
        default=None, alias="Address-5", description="Address. Max length: 100"
    )
    Address2: str | None = Field(
        default=None, alias="Address2-6", description="Address 2. Max length: 50"
    )
    City: str | None = Field(default=None, alias="City-7", description="City. Max length: 30")
    Contact: str | None = Field(
        default=None, alias="Contact-8", description="Contact. Max length: 100"
    )
    PhoneNo: str | None = Field(
        default=None, alias="PhoneNo-9", description="Phone No.. Max length: 30"
    )
    TelexNo: str | None = Field(
        default=None, alias="TelexNo-10", description="Telex No.. Max length: 20"
    )
    OurAccountNo: str | None = Field(
        default=None, alias="OurAccountNo-14", description="Our Account No.. Max length: 20"
    )
    TerritoryCode: str | None = Field(
        default=None, alias="TerritoryCode-15", description="Territory Code. Max length: 10"
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-16",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-17",
        description="Global Dimension 2 Code. Max length: 20",
    )
    BudgetedAmount: float | None = Field(
        default=None, alias="BudgetedAmount-19", description="Budgeted Amount. Max length: 12"
    )
    VendorPostingGroup: str | None = Field(
        default=None,
        alias="VendorPostingGroup-21",
        description="Vendor Posting Group. Max length: 20",
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-22", description="Currency Code. Max length: 10"
    )
    LanguageCode: str | None = Field(
        default=None, alias="LanguageCode-24", description="Language Code. Max length: 10"
    )
    RegistrationNumber: str | None = Field(
        default=None,
        alias="RegistrationNumber-25",
        description="Registration Number. Max length: 50",
    )
    StatisticsGroup: int | None = Field(
        default=None, alias="StatisticsGroup-26", description="Statistics Group. Max length: 4"
    )
    PaymentTermsCode: str | None = Field(
        default=None, alias="PaymentTermsCode-27", description="Payment Terms Code. Max length: 10"
    )
    FinChargeTermsCode: str | None = Field(
        default=None,
        alias="FinChargeTermsCode-28",
        description="Fin. Charge Terms Code. Max length: 10",
    )
    PurchaserCode: str | None = Field(
        default=None, alias="PurchaserCode-29", description="Purchaser Code. Max length: 20"
    )
    ShipmentMethodCode: str | None = Field(
        default=None,
        alias="ShipmentMethodCode-30",
        description="Shipment Method Code. Max length: 10",
    )
    ShippingAgentCode: str | None = Field(
        default=None,
        alias="ShippingAgentCode-31",
        description="Shipping Agent Code. Max length: 10",
    )
    InvoiceDiscCode: str | None = Field(
        default=None, alias="InvoiceDiscCode-33", description="Invoice Disc. Code. Max length: 20"
    )
    CountryRegionCode: str | None = Field(
        default=None,
        alias="CountryRegionCode-35",
        description="Country/Region Code. Max length: 10",
    )
    Blocked: int | None = Field(
        default=None, alias="Blocked-39", description="Blocked. Max length: 4"
    )
    PaytoVendorNo: str | None = Field(
        default=None, alias="PaytoVendorNo-45", description="Pay-to Vendor No.. Max length: 20"
    )
    Priority: int | None = Field(
        default=None, alias="Priority-46", description="Priority. Max length: 4"
    )
    PaymentMethodCode: str | None = Field(
        default=None,
        alias="PaymentMethodCode-47",
        description="Payment Method Code. Max length: 10",
    )
    FormatRegion: str | None = Field(
        default=None, alias="FormatRegion-48", description="Format Region. Max length: 80"
    )
    LastModifiedDateTime: datetime.datetime | None = Field(
        default=None,
        alias="LastModifiedDateTime-53",
        description="Last Modified Date Time. Max length: 8",
    )
    LastDateModified: datetime.date | None = Field(
        default=None, alias="LastDateModified-54", description="Last Date Modified. Max length: 4"
    )
    ApplicationMethod: int | None = Field(
        default=None, alias="ApplicationMethod-80", description="Application Method. Max length: 4"
    )
    PricesIncludingVAT: bool | None = Field(
        default=None,
        alias="PricesIncludingVAT-82",
        description="Prices Including VAT. Max length: 4",
    )
    FaxNo: str | None = Field(default=None, alias="FaxNo-84", description="Fax No.. Max length: 30")
    TelexAnswerBack: str | None = Field(
        default=None, alias="TelexAnswerBack-85", description="Telex Answer Back. Max length: 20"
    )
    VATRegistrationNo: str | None = Field(
        default=None,
        alias="VATRegistrationNo-86",
        description="VAT Registration No.. Max length: 20",
    )
    GenBusPostingGroup: str | None = Field(
        default=None,
        alias="GenBusPostingGroup-88",
        description="Gen. Bus. Posting Group. Max length: 20",
    )
    GLN: str | None = Field(default=None, alias="GLN-90", description="GLN. Max length: 13")
    PostCode: str | None = Field(
        default=None, alias="PostCode-91", description="Post Code. Max length: 20"
    )
    County: str | None = Field(
        default=None, alias="County-92", description="County. Max length: 30"
    )
    EORINumber: str | None = Field(
        default=None, alias="EORINumber-93", description="EORI Number. Max length: 40"
    )
    EMail: str | None = Field(default=None, alias="EMail-102", description="E-Mail. Max length: 80")
    HomePage: str | None = Field(
        default=None, alias="HomePage-103", description="Home Page. Max length: 80"
    )
    NoSeries: str | None = Field(
        default=None, alias="NoSeries-107", description="No. Series. Max length: 20"
    )
    TaxAreaCode: str | None = Field(
        default=None, alias="TaxAreaCode-108", description="Tax Area Code. Max length: 20"
    )
    TaxLiable: bool | None = Field(
        default=None, alias="TaxLiable-109", description="Tax Liable. Max length: 4"
    )
    VATBusPostingGroup: str | None = Field(
        default=None,
        alias="VATBusPostingGroup-110",
        description="VAT Bus. Posting Group. Max length: 20",
    )
    BlockPaymentTolerance: bool | None = Field(
        default=None,
        alias="BlockPaymentTolerance-116",
        description="Block Payment Tolerance. Max length: 4",
    )
    ICPartnerCode: str | None = Field(
        default=None, alias="ICPartnerCode-119", description="IC Partner Code. Max length: 20"
    )
    Prepayment: float | None = Field(
        default=None, alias="Prepayment-124", description="Prepayment %. Max length: 12"
    )
    PartnerType: int | None = Field(
        default=None, alias="PartnerType-132", description="Partner Type. Max length: 4"
    )
    IntrastatPartnerType: int | None = Field(
        default=None,
        alias="IntrastatPartnerType-133",
        description="Intrastat Partner Type. Max length: 4",
    )
    ExcludefromPmtPractices: bool | None = Field(
        default=None,
        alias="ExcludefromPmtPractices-134",
        description="Exclude from Pmt. Practices. Max length: 4",
    )
    CompanySizeCode: str | None = Field(
        default=None, alias="CompanySizeCode-135", description="Company Size Code. Max length: 20"
    )
    PrivacyBlocked: bool | None = Field(
        default=None, alias="PrivacyBlocked-150", description="Privacy Blocked. Max length: 4"
    )
    DisableSearchbyName: bool | None = Field(
        default=None,
        alias="DisableSearchbyName-160",
        description="Disable Search by Name. Max length: 4",
    )
    CreditorNo: str | None = Field(
        default=None, alias="CreditorNo-170", description="Creditor No.. Max length: 20"
    )
    AllowMultiplePostingGroups: bool | None = Field(
        default=None,
        alias="AllowMultiplePostingGroups-175",
        description="Allow Multiple Posting Groups. Max length: 4",
    )
    PreferredBankAccountCode: str | None = Field(
        default=None,
        alias="PreferredBankAccountCode-288",
        description="Preferred Bank Account Code. Max length: 20",
    )
    CoupledtoCRM: bool | None = Field(
        default=None, alias="CoupledtoCRM-720", description="Coupled to CRM. Max length: 4"
    )
    CashFlowPaymentTermsCode: str | None = Field(
        default=None,
        alias="CashFlowPaymentTermsCode-840",
        description="Cash Flow Payment Terms Code. Max length: 10",
    )
    PrimaryContactNo: str | None = Field(
        default=None,
        alias="PrimaryContactNo-5049",
        description="Primary Contact No.. Max length: 20",
    )
    MobilePhoneNo: str | None = Field(
        default=None, alias="MobilePhoneNo-5061", description="Mobile Phone No.. Max length: 30"
    )
    ResponsibilityCenter: str | None = Field(
        default=None,
        alias="ResponsibilityCenter-5700",
        description="Responsibility Center. Max length: 10",
    )
    LocationCode: str | None = Field(
        default=None, alias="LocationCode-5701", description="Location Code. Max length: 10"
    )
    LeadTimeCalculation: str | None = Field(
        default=None,
        alias="LeadTimeCalculation-5790",
        description="Lead Time Calculation. Max length: 32",
    )
    PriceCalculationMethod: int | None = Field(
        default=None,
        alias="PriceCalculationMethod-7000",
        description="Price Calculation Method. Max length: 4",
    )
    BaseCalendarCode: str | None = Field(
        default=None,
        alias="BaseCalendarCode-7600",
        description="Base Calendar Code. Max length: 10",
    )
    DocumentSendingProfile: str | None = Field(
        default=None,
        alias="DocumentSendingProfile-7601",
        description="Document Sending Profile. Max length: 20",
    )
    ValidateEUVatRegNo: bool | None = Field(
        default=None,
        alias="ValidateEUVatRegNo-7602",
        description="Validate EU Vat Reg. No.. Max length: 4",
    )
    CurrencyId: str | None = Field(
        default=None, alias="CurrencyId-8001", description="Currency Id. Max length: 16"
    )
    PaymentTermsId: str | None = Field(
        default=None, alias="PaymentTermsId-8002", description="Payment Terms Id. Max length: 16"
    )
    PaymentMethodId: str | None = Field(
        default=None, alias="PaymentMethodId-8003", description="Payment Method Id. Max length: 16"
    )
    OverReceiptCode: str | None = Field(
        default=None, alias="OverReceiptCode-8510", description="Over-Receipt Code. Max length: 20"
    )
    PTEMicrosoftafslttur: float | None = Field(
        default=None,
        alias="PTEMicrosoftafslttur-65000",
        description="PTE Microsoft afsláttur. Max length: 12",
    )
    PTEGjaldkeri: str | None = Field(
        default=None, alias="PTEGjaldkeri-65001", description="PTE Gjaldkeri. Max length: 30"
    )
    PTESmigjaldkera: str | None = Field(
        default=None,
        alias="PTESmigjaldkera-65002",
        description="PTE Sími gjaldkera. Max length: 30",
    )
    PTEHeitikeju: str | None = Field(
        default=None, alias="PTEHeitikeju-65003", description="PTE Heiti keðju. Max length: 10"
    )
    PTEAfmrkuntgjaldaflokks: str | None = Field(
        default=None,
        alias="PTEAfmrkuntgjaldaflokks-65004",
        description="PTE Afmörkun útgjaldaflokks. Max length: 10",
    )
    PTEEkkiuppskrifa: bool | None = Field(
        default=None,
        alias="PTEEkkiuppskrifa-65005",
        description="PTE Ekki uppáskrifað. Max length: 4",
    )
    PTEPrentalaunamia: bool | None = Field(
        default=None,
        alias="PTEPrentalaunamia-65006",
        description="PTE Prenta launamiða. Max length: 4",
    )
    PTEjafnaargreislur: float | None = Field(
        default=None,
        alias="PTEjafnaargreislur-65007",
        description="PTE Ójafnaðar greiðslur. Max length: 12",
    )
    PTEEkkigreislutillgu: bool | None = Field(
        default=None,
        alias="PTEEkkigreislutillgu-65008",
        description="PTE Ekki í greiðslutillögu. Max length: 4",
    )
    PTEContractor: bool | None = Field(
        default=None, alias="PTEContractor-65009", description="PTE Contractor. Max length: 4"
    )
    PTESleppagreislutillgu: bool | None = Field(
        default=None,
        alias="PTESleppagreislutillgu-65010",
        description="PTE Sleppa í greiðslutillögu. Max length: 4",
    )
    CTRContractor: bool | None = Field(
        default=None, alias="CTRContractor-10027575", description="CTR Contractor. Max length: 4"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )


class VendorLedgerEntry(SparkModel):
    """Represents the table Vendor Ledger Entry

    Display Name: Vendor Ledger Entry
    Entity Name: VendorLedgerEntry-25
    """

    EntryNo: int | None = Field(
        default=None, alias="EntryNo-1", description="Entry No.. Max length: 4"
    )
    VendorNo: str | None = Field(
        default=None, alias="VendorNo-3", description="Vendor No.. Max length: 20"
    )
    PostingDate: datetime.date | None = Field(
        default=None, alias="PostingDate-4", description="Posting Date. Max length: 4"
    )
    DocumentType: int | None = Field(
        default=None, alias="DocumentType-5", description="Document Type. Max length: 4"
    )
    DocumentNo: str | None = Field(
        default=None, alias="DocumentNo-6", description="Document No.. Max length: 20"
    )
    Description: str | None = Field(
        default=None, alias="Description-7", description="Description. Max length: 100"
    )
    VendorName: str | None = Field(
        default=None, alias="VendorName-8", description="Vendor Name. Max length: 100"
    )
    CurrencyCode: str | None = Field(
        default=None, alias="CurrencyCode-11", description="Currency Code. Max length: 10"
    )
    PurchaseLCY: float | None = Field(
        default=None, alias="PurchaseLCY-18", description="Purchase (LCY). Max length: 12"
    )
    InvDiscountLCY: float | None = Field(
        default=None, alias="InvDiscountLCY-20", description="Inv. Discount (LCY). Max length: 12"
    )
    BuyfromVendorNo: str | None = Field(
        default=None, alias="BuyfromVendorNo-21", description="Buy-from Vendor No.. Max length: 20"
    )
    VendorPostingGroup: str | None = Field(
        default=None,
        alias="VendorPostingGroup-22",
        description="Vendor Posting Group. Max length: 20",
    )
    GlobalDimension1Code: str | None = Field(
        default=None,
        alias="GlobalDimension1Code-23",
        description="Global Dimension 1 Code. Max length: 20",
    )
    GlobalDimension2Code: str | None = Field(
        default=None,
        alias="GlobalDimension2Code-24",
        description="Global Dimension 2 Code. Max length: 20",
    )
    PurchaserCode: str | None = Field(
        default=None, alias="PurchaserCode-25", description="Purchaser Code. Max length: 20"
    )
    UserID: str | None = Field(
        default=None, alias="UserID-27", description="User ID. Max length: 50"
    )
    SourceCode: str | None = Field(
        default=None, alias="SourceCode-28", description="Source Code. Max length: 10"
    )
    OnHold: str | None = Field(
        default=None, alias="OnHold-33", description="On Hold. Max length: 3"
    )
    AppliestoDocType: int | None = Field(
        default=None, alias="AppliestoDocType-34", description="Applies-to Doc. Type. Max length: 4"
    )
    AppliestoDocNo: str | None = Field(
        default=None, alias="AppliestoDocNo-35", description="Applies-to Doc. No.. Max length: 20"
    )
    Open: bool | None = Field(default=None, alias="Open-36", description="Open. Max length: 4")
    DueDate: datetime.date | None = Field(
        default=None, alias="DueDate-37", description="Due Date. Max length: 4"
    )
    PmtDiscountDate: datetime.date | None = Field(
        default=None, alias="PmtDiscountDate-38", description="Pmt. Discount Date. Max length: 4"
    )
    OriginalPmtDiscPossible: float | None = Field(
        default=None,
        alias="OriginalPmtDiscPossible-39",
        description="Original Pmt. Disc. Possible. Max length: 12",
    )
    PmtDiscRcdLCY: float | None = Field(
        default=None, alias="PmtDiscRcdLCY-40", description="Pmt. Disc. Rcd.(LCY). Max length: 12"
    )
    OrigPmtDiscPossibleLCY: float | None = Field(
        default=None,
        alias="OrigPmtDiscPossibleLCY-42",
        description="Orig. Pmt. Disc. Possible(LCY). Max length: 12",
    )
    Positive: bool | None = Field(
        default=None, alias="Positive-43", description="Positive. Max length: 4"
    )
    ClosedbyEntryNo: int | None = Field(
        default=None, alias="ClosedbyEntryNo-44", description="Closed by Entry No.. Max length: 4"
    )
    ClosedatDate: datetime.date | None = Field(
        default=None, alias="ClosedatDate-45", description="Closed at Date. Max length: 4"
    )
    ClosedbyAmount: float | None = Field(
        default=None, alias="ClosedbyAmount-46", description="Closed by Amount. Max length: 12"
    )
    AppliestoID: str | None = Field(
        default=None, alias="AppliestoID-47", description="Applies-to ID. Max length: 50"
    )
    JournalTemplName: str | None = Field(
        default=None, alias="JournalTemplName-48", description="Journal Templ. Name. Max length: 10"
    )
    JournalBatchName: str | None = Field(
        default=None, alias="JournalBatchName-49", description="Journal Batch Name. Max length: 10"
    )
    ReasonCode: str | None = Field(
        default=None, alias="ReasonCode-50", description="Reason Code. Max length: 10"
    )
    BalAccountType: int | None = Field(
        default=None, alias="BalAccountType-51", description="Bal. Account Type. Max length: 4"
    )
    BalAccountNo: str | None = Field(
        default=None, alias="BalAccountNo-52", description="Bal. Account No.. Max length: 20"
    )
    TransactionNo: int | None = Field(
        default=None, alias="TransactionNo-53", description="Transaction No.. Max length: 4"
    )
    ClosedbyAmountLCY: float | None = Field(
        default=None,
        alias="ClosedbyAmountLCY-54",
        description="Closed by Amount (LCY). Max length: 12",
    )
    DocumentDate: datetime.date | None = Field(
        default=None, alias="DocumentDate-62", description="Document Date. Max length: 4"
    )
    ExternalDocumentNo: str | None = Field(
        default=None,
        alias="ExternalDocumentNo-63",
        description="External Document No.. Max length: 35",
    )
    NoSeries: str | None = Field(
        default=None, alias="NoSeries-64", description="No. Series. Max length: 20"
    )
    ClosedbyCurrencyCode: str | None = Field(
        default=None,
        alias="ClosedbyCurrencyCode-65",
        description="Closed by Currency Code. Max length: 10",
    )
    ClosedbyCurrencyAmount: float | None = Field(
        default=None,
        alias="ClosedbyCurrencyAmount-66",
        description="Closed by Currency Amount. Max length: 12",
    )
    AdjustedCurrencyFactor: float | None = Field(
        default=None,
        alias="AdjustedCurrencyFactor-73",
        description="Adjusted Currency Factor. Max length: 12",
    )
    OriginalCurrencyFactor: float | None = Field(
        default=None,
        alias="OriginalCurrencyFactor-74",
        description="Original Currency Factor. Max length: 12",
    )
    RemainingPmtDiscPossible: float | None = Field(
        default=None,
        alias="RemainingPmtDiscPossible-77",
        description="Remaining Pmt. Disc. Possible. Max length: 12",
    )
    PmtDiscToleranceDate: datetime.date | None = Field(
        default=None,
        alias="PmtDiscToleranceDate-78",
        description="Pmt. Disc. Tolerance Date. Max length: 4",
    )
    MaxPaymentTolerance: float | None = Field(
        default=None,
        alias="MaxPaymentTolerance-79",
        description="Max. Payment Tolerance. Max length: 12",
    )
    AcceptedPaymentTolerance: float | None = Field(
        default=None,
        alias="AcceptedPaymentTolerance-81",
        description="Accepted Payment Tolerance. Max length: 12",
    )
    AcceptedPmtDiscTolerance: bool | None = Field(
        default=None,
        alias="AcceptedPmtDiscTolerance-82",
        description="Accepted Pmt. Disc. Tolerance. Max length: 4",
    )
    PmtToleranceLCY: float | None = Field(
        default=None, alias="PmtToleranceLCY-83", description="Pmt. Tolerance (LCY). Max length: 12"
    )
    AmounttoApply: float | None = Field(
        default=None, alias="AmounttoApply-84", description="Amount to Apply. Max length: 12"
    )
    ICPartnerCode: str | None = Field(
        default=None, alias="ICPartnerCode-85", description="IC Partner Code. Max length: 20"
    )
    ApplyingEntry: bool | None = Field(
        default=None, alias="ApplyingEntry-86", description="Applying Entry. Max length: 4"
    )
    Reversed: bool | None = Field(
        default=None, alias="Reversed-87", description="Reversed. Max length: 4"
    )
    ReversedbyEntryNo: int | None = Field(
        default=None,
        alias="ReversedbyEntryNo-88",
        description="Reversed by Entry No.. Max length: 4",
    )
    ReversedEntryNo: int | None = Field(
        default=None, alias="ReversedEntryNo-89", description="Reversed Entry No.. Max length: 4"
    )
    Prepayment: bool | None = Field(
        default=None, alias="Prepayment-90", description="Prepayment. Max length: 4"
    )
    CreditorNo: str | None = Field(
        default=None, alias="CreditorNo-170", description="Creditor No.. Max length: 20"
    )
    PaymentReference: str | None = Field(
        default=None, alias="PaymentReference-171", description="Payment Reference. Max length: 50"
    )
    PaymentMethodCode: str | None = Field(
        default=None,
        alias="PaymentMethodCode-172",
        description="Payment Method Code. Max length: 10",
    )
    AppliestoExtDocNo: str | None = Field(
        default=None,
        alias="AppliestoExtDocNo-173",
        description="Applies-to Ext. Doc. No.. Max length: 35",
    )
    InvoiceReceivedDate: datetime.date | None = Field(
        default=None,
        alias="InvoiceReceivedDate-175",
        description="Invoice Received Date. Max length: 4",
    )
    RecipientBankAccount: str | None = Field(
        default=None,
        alias="RecipientBankAccount-288",
        description="Recipient Bank Account. Max length: 20",
    )
    MessagetoRecipient: str | None = Field(
        default=None,
        alias="MessagetoRecipient-289",
        description="Message to Recipient. Max length: 140",
    )
    ExportedtoPaymentFile: bool | None = Field(
        default=None,
        alias="ExportedtoPaymentFile-290",
        description="Exported to Payment File. Max length: 4",
    )
    DimensionSetID: int | None = Field(
        default=None, alias="DimensionSetID-480", description="Dimension Set ID. Max length: 4"
    )
    RemittoCode: str | None = Field(
        default=None, alias="RemittoCode-1000", description="Remit-to Code. Max length: 20"
    )
    PTEGreia: bool | None = Field(
        default=None, alias="PTEGreia-65000", description="PTE Greiða. Max length: 4"
    )
    PTEtgjaldaflokkur: str | None = Field(
        default=None,
        alias="PTEtgjaldaflokkur-65001",
        description="PTE Útgjaldaflokkur. Max length: 10",
    )
    PTEFastanmer: str | None = Field(
        default=None, alias="PTEFastanmer-65002", description="PTE Fastanúmer. Max length: 7"
    )
    PTEContractor: bool | None = Field(
        default=None, alias="PTEContractor-65003", description="PTE Contractor. Max length: 4"
    )
    PTEGrseill: bool | None = Field(
        default=None, alias="PTEGrseill-65004", description="PTE Gíróseðill. Max length: 4"
    )
    PTEUppskrifaaf: str | None = Field(
        default=None, alias="PTEUppskrifaaf-65005", description="PTE Uppáskrifað af. Max length: 50"
    )
    PTEsamykktur: bool | None = Field(
        default=None, alias="PTEsamykktur-65006", description="PTE Ósamþykktur. Max length: 4"
    )
    PTEMttkudagsetning: datetime.date | None = Field(
        default=None,
        alias="PTEMttkudagsetning-65007",
        description="PTE Móttökudagsetning. Max length: 4",
    )
    PTEBankatexti: str | None = Field(
        default=None, alias="PTEBankatexti-65008", description="PTE Bankatexti. Max length: 250"
    )
    PTEGreisluform: int | None = Field(
        default=None, alias="PTEGreisluform-65009", description="PTE Greiðsluform. Max length: 4"
    )
    PTEbkaur: bool | None = Field(
        default=None, alias="PTEbkaur-65010", description="PTE Óbókaður. Max length: 4"
    )
    CTRContractor: bool | None = Field(
        default=None, alias="CTRContractor-10027575", description="CTR Contractor. Max length: 4"
    )
    timestamp: int | None = Field(
        default=None, alias="timestamp-0", description="timestamp. Max length: 8"
    )
    SystemId: str | None = Field(
        default=None, alias="systemId-2000000000", description="$systemId. Max length: 16"
    )
    SystemCreatedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemCreatedAt-2000000001",
        description="SystemCreatedAt. Max length: 8",
    )
    SystemCreatedBy: str | None = Field(
        default=None,
        alias="SystemCreatedBy-2000000002",
        description="SystemCreatedBy. Max length: 16",
    )
    SystemModifiedAt: datetime.datetime | None = Field(
        default=None,
        alias="SystemModifiedAt-2000000003",
        description="SystemModifiedAt. Max length: 8",
    )
    SystemModifiedBy: str | None = Field(
        default=None,
        alias="SystemModifiedBy-2000000004",
        description="SystemModifiedBy. Max length: 16",
    )
    Company: str | None = Field(
        default=None, alias="$Company", description="$Company. Max length: 16"
    )
