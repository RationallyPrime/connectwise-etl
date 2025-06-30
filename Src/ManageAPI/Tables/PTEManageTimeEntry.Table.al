table 62003 "PTE Manage Time Entry"
{
    Caption = 'Manage Time Entry';
    DataClassification = CustomerContent;

    fields
    {
        field(1; "Time Entry Id"; Integer)
        {
            Caption = 'Time Entry Id';
        }
        field(2; Employee; Code[10])
        {
            Caption = 'Employee';
        }
        field(3; "Agreement Id"; Integer)
        {
            Caption = 'Agreement Id';
        }
        field(4; "Work Role Id"; Integer)
        {
            Caption = 'Work Role Id';
        }
        field(5; "Work Type"; Text[50])
        {
            Caption = 'Work Type';
        }
        field(6; "Hourly Rate"; Decimal)
        {
            Caption = 'Hourly Rate';
        }
        field(7; "Agreement Hours"; Decimal)
        {
            Caption = 'Agreement Hours';
        }
        field(8; "Agreement Amount"; Decimal)
        {
            Caption = 'Agreement Amount';
        }
        field(9; "Global Hourly Rate"; Decimal)
        {
            Caption = 'Global Hourly Rate';
        }
        field(10; "Ticket Id"; Integer)
        {
            Caption = 'Ticket Id';
        }
        field(11; "Site Name"; Text[100])
        {
            Caption = 'Site Name';
        }
        field(12; "Rate Type"; Code[10])
        {
            Caption = 'Rate Type';
        }
        field(13; "Agreement Work Role Rate"; Decimal)
        {
            Caption = 'Agreement Work Role Rate';
        }
        field(14; Note; Text[1200])
        {
            Caption = 'Note';
        }
        field(15; "Agreement Number"; Code[10])
        {
            Caption = 'Agreement Number';
        }
        field(16; "Invoice Number"; Code[10])
        {
            Caption = 'Invoice Number';
        }
        field(17; "Ticket Summary"; Text[100])
        {
            Caption = 'Ticket Summary';
        }
        field(18; "Work Date"; DateTime)
        {
            Caption = 'Work Date';
        }
        field(19; "Agreement Type"; Text[50])
        {
            Caption = 'Agreement Type';
        }
        field(20; "Parent Agreement Id"; Integer)
        {
            Caption = 'Parent Agreement Id';
        }
    }
    keys
    {
        key(PK; "Time Entry Id", "Invoice Number")
        {
            Clustered = true;
        }
    }
}
