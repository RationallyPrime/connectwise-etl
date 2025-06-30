table 62002 "PTE Manage Invoice Line"
{
    Caption = 'Manage Invoice Line';
    DataClassification = CustomerContent;

    fields
    {
        field(1; "Invoice Number"; Code[10])
        {
            Caption = 'Invoice Number';
            TableRelation = "PTE Manage Invoice Header";
        }
        field(2; "Line No."; Integer)
        {
            Caption = 'Line No.';
        }
        field(3; Memo; Text[250])
        {
            Caption = 'Memo';
        }
        field(4; Quantity; Decimal)
        {
            Caption = 'Quantity';
            DecimalPlaces = 0 : 2;
        }
        field(5; "Line Amount"; Decimal)
        {
            Caption = 'Line Amount';
        }
        field(6; Description; Text[50])
        {
            Caption = 'Description';
        }
        field(7; Cost; Decimal)
        {
            Caption = 'Cost';
        }
        field(8; Price; Decimal)
        {
            Caption = 'Price';
        }
        field(9; "Product ID"; Integer)
        {
            Caption = 'Product ID';
        }
        field(10; "Item Identifier"; Code[20])
        {
            Caption = 'Item Identifier';
        }
        field(11; "Time Entry Id"; Integer)
        {
            Caption = 'Time Entry Id';
        }
        field(12; "Job Journal Line No."; Integer)
        {
            Caption = 'Job Journal Line No.';
        }
        field(13; "Document Date"; Date)
        {
            Caption = 'Document Date';
        }
    }
    keys
    {
        key(PK; "Invoice Number", "Line No.")
        {
            Clustered = true;
        }
    }
    trigger OnDelete()
    var
        ManageTimeEntry: Record "PTE Manage Time Entry";
    begin
        if "Time Entry Id" > 0 then begin
            ManageTimeEntry.SetRange("Time Entry Id", "Time Entry Id");
            ManageTimeEntry.DeleteAll();
        end;
    end;
}
