table 62004 "PTE Manage Invoice Expense"
{
    Caption = 'Manage Invoice Expense';
    DataClassification = CustomerContent;

    fields
    {
        field(1; "Invoice ID"; Integer)
        {
            Caption = 'Invoice ID';
        }
        field(2; "Line No."; Integer)
        {
            Caption = 'Line No.';
        }
        field(3; Type; Text[50])
        {
            Caption = 'Type';
        }
        field(4; Quantity; Decimal)
        {
            Caption = 'Quantity';
        }
        field(5; Amount; Decimal)
        {
            Caption = 'Amount';
        }
        field(6; Employee; Code[10])
        {
            Caption = 'Employee';
        }
        field(7; "Job Journal Line No."; Integer)
        {
            Caption = 'Job Journal Line No.';
        }
        field(8; "Work Date"; DateTime)
        {
            Caption = 'Work Date';
        }
        field(9; "Agreement Id"; Integer)
        {
            Caption = 'Agreement Id';
        }
        field(10; "Agreement Number"; Code[10])
        {
            Caption = 'Agreement Number';
        }
        field(11; "Parent Agreement Id"; Integer)
        {
            Caption = 'Parent Agreement Id';
        }
        field(12; "Invoice Number"; Code[10])
        {
            Caption = 'Invoice Number';
        }
        field(13; "Agreement Type"; Text[50])
        {
            Caption = 'Agreement Type';
        }
    }
    keys
    {
        key(PK; "Invoice ID", "Line No.")
        {
            Clustered = true;
        }
    }
}
