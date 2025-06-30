table 62006 "PTE Manage Invoice Error Logo"
{
    Caption = 'Manage Invoice Error Logo';
    DataClassification = CustomerContent;
    DrillDownPageId = "PTE Manage Invoice Errors";

    fields
    {
        field(1; "Invoice Number"; Code[10])
        {
            TableRelation = "PTE Manage Invoice Header"."Invoice Number";
            Caption = 'Error Table Id';
        }
        field(2; "Error Table id"; Integer)
        {
            Caption = 'Error Table Id';
        }
        field(3; "Table Name"; Text[30])
        {
            Caption = 'Table Name';
            Fieldclass = FlowField;
            Calcformula = lookup(Allobj."Object Name" where("Object ID" = field("Error Table id"), "Object Type" = const(Table)));
        }
        field(4; "Missing Value"; Text[50])
        {
            Caption = 'Missing Value';
        }
        field(5; "Error Message"; Text[250])
        {
            Caption = 'Error Message';
        }
        field(6; "Error Type"; Option)
        {
            Caption = 'Error Type';
            OptionMembers = "Error Type";
        }
    }
    keys
    {
        key(PK; "Invoice Number", "Error Table id", "Missing Value")
        {
            Clustered = true;

        }
    }
}
