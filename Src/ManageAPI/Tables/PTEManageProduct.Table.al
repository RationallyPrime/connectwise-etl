table 62005 "PTE Manage Product"
{
    Caption = 'Manage Product';
    DataClassification = CustomerContent;

    fields
    {
        field(1; "Product Id"; Integer)
        {
            Caption = 'Product Id';
        }
        field(2; "Invoice Number"; Code[10])
        {
            Caption = 'Invoice Number';
        }
        field(3; "Agreement Id"; Integer)
        {
            Caption = 'Agreement Id';
        }
        field(4; Description; Text[100])
        {
            Caption = 'Description';
        }
        field(5; "Discount %"; Decimal)
        {
            Caption = 'Discount %';
        }
        field(6; "Agreement Number"; Code[10])
        {
            Caption = 'Agreement Number';
        }
        field(7; "Parent Agreement Id"; Integer)
        {
            Caption = 'Parent Agreement Id';
        }
    }
    keys
    {
        key(PK; "Invoice Number", "Product Id")
        {
            Clustered = true;
        }
    }
}
