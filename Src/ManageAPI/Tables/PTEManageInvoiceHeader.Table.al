table 62001 "PTE Manage Invoice Header"
{
    Caption = 'Manage Invoice Header';
    DataClassification = CustomerContent;
    LookupPageId = "PTE Manage API Invoice List";

    fields
    {
        field(1; "Billing Log Id"; Integer)
        {
            Caption = 'Billing Log Id';
        }
        field(2; "Invoice Number"; Code[10])
        {
            Caption = 'Invoice Number';
        }
        field(3; "Invoice Type"; Enum "PTE Manage Invoice Type")
        {
            Caption = 'Invoice Type';
        }
        field(4; "Customer Name"; Text[100])
        {
            Caption = 'Customer Name';
        }
        field(5; "Total Amount"; Decimal)
        {
            Caption = 'Total Amount';
        }
        field(6; "Total Amount without VAT"; Decimal)
        {
            Caption = 'Total Amount without VAT';
        }
        field(7; Site; Text[100])
        {
            Caption = 'Site';
        }
        field(8; "Invoice ID"; Integer)
        {
            Caption = 'Invoice ID';
        }
        field(9; "VAT %"; Decimal)
        {
            Caption = 'VAT %';
        }
        field(10; "Social Security No."; Code[20])
        {
            Caption = 'Social Security No.';
        }
        field(11; Project; Text[50])
        {
            Caption = 'Project';
        }
        field(12; "GLEntry Ids"; Text[500])
        {
            Caption = 'GLEntry Ids';
        }
        field(13; Closed; Boolean)
        {
            Caption = 'Closed';
        }
        field(14; "Sales Invoice No."; Code[20])
        {
            Caption = 'Sales Invoice No.';
            TableRelation = "Sales Header"."No." where("Document Type" = filter(Invoice));
        }
        field(15; "Job Journal Lines Amount"; Decimal)
        {
            Caption = 'Project Journal Lines Amount';
            FieldClass = FlowField;
            Calcformula = sum("Job Journal Line"."Line Amount" where("Document No." = field("Invoice Number"), "WJOB Qty to Invoice" = filter(> 0)));
            Editable = false;
        }
        field(16; "Data Errors"; Integer)
        {
            Caption = 'Data Errors';
            FieldClass = FlowField;
            Calcformula = count("PTE Manage Invoice Error Logo" where("Invoice Number" = field("Invoice Number")));
            Editable = false;
        }
        field(17; "Invoice Date"; DateTime)
        {
            Caption = 'Invoice Date';
        }
        field(18; "Agreement Number"; Code[10])
        {
            Caption = 'Agreement Number';
        }
        field(19; "Total Amount Imported"; Decimal)
        {
            Caption = 'Total Amount Imported';
            FieldClass = FlowField;
            CalcFormula = sum("PTE Manage Invoice Line"."Line Amount" where("Invoice Number" = field("Invoice Number"), "Line Amount" = filter(< 0)));
            Editable = false;
        }
        field(20; "Posted Project Lines Amount"; Decimal)
        {
            Caption = 'Posted Project Lines Amount';
            FieldClass = FlowField;
            CalcFormula = sum("Job Ledger Entry"."Line Amount" where("Document No." = field("Invoice Number"), "Entry Type" = filter(0), "Job No." = field("Agreement Number")));
            Editable = false;
        }
    }
    keys
    {
        key(PK; "Billing Log Id")
        {
            Clustered = true;
        }
        key(key1; "Invoice Type")
        {
        }
    }
    trigger OnDelete()
    var
        ManageProductData: Record "PTE Manage Product";
        ManageExpensesData: Record "PTE Manage Invoice Expense";
        ManageInvoiceLine: Record "PTE Manage Invoice Line";
        ManageInvoiceErrorLogo: Record "PTE Manage Invoice Error Logo";
        ErrorTxt: Label 'Invoice is closed or have related documents';
    begin
        if Closed or ("Sales Invoice No." <> '') or ("Job Journal Lines Amount" > 0) then
            Error(ErrorTxt);

        if "Invoice Type" = "PTE Manage Invoice Type"::Agreement then begin
            ManageProductData.SetRange("Invoice Number", "Invoice Number");
            ManageProductData.DeleteAll();
        end else begin
            ManageExpensesData.SetRange("Invoice ID", "Invoice ID");
            ManageExpensesData.DeleteAll();
        end;

        ManageInvoiceLine.SetRange("Invoice Number", "Invoice Number");
        ManageInvoiceLine.DeleteAll(true);

        ManageInvoiceErrorLogo.SetRange("Invoice Number", "Invoice Number");
        ManageInvoiceErrorLogo.DeleteAll();
    end;
}
