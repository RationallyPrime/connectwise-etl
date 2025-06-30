table 62000 "PTE Manage API Setup"
{
    Caption = 'Manage API Setup';
    DataClassification = CustomerContent;

    fields
    {
        field(1; Code; Code[10])
        {
            Caption = 'Code';
        }
        field(2; "Web Service URL"; Text[200])
        {
            Caption = 'Web Service URL';
        }
        field(3; "User Name"; Text[50])
        {
            Caption = 'User Name';
        }
        field(4; Password; Text[50])
        {
            Caption = 'Password';
            ExtendedDatatype = Masked;
        }
        field(5; "Client Id"; Text[50])
        {
            Caption = 'Client Id';
        }
        field(6; "Job Journal Template Name"; Code[10])
        {
            Caption = 'Project Journal Template Name';
            TableRelation = "Job Journal Template";
        }
        field(7; "Job Journal Batch Name"; Code[10])
        {
            Caption = 'Project Journal Batch Name';
            TableRelation = "Job Journal Batch".Name where("Journal Template Name" = field("Job Journal Template Name"));
        }
        field(8; "Resource Code For Expenses"; Code[20])
        {
            Caption = 'Resource Code For Expenses';
            TableRelation = Resource;
        }
        field(9; "Sales Invoice Reason"; Code[10])
        {
            Caption = 'Reason Code For Sales Invoices';
            TableRelation = "Reason Code";
        }
        field(10; "Time Entry Reason Code"; Code[10])
        {
            Caption = 'Reason Code For Time Entries';
            TableRelation = "Reason Code";
        }
    }
    keys
    {
        key(PK; Code)
        {
            Clustered = true;
        }
    }
}
