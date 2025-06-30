page 62000 "PTE Manage API Setup"
{
    ApplicationArea = All;
    Caption = 'Manage API Setup';
    DeleteAllowed = false;
    InsertAllowed = false;
    PageType = Card;
    SourceTable = "PTE Manage API Setup";
    UsageCategory = Administration;

    layout
    {
        area(content)
        {
            group(General)
            {
                Caption = 'General';
                field("Web Service URL"; Rec."Web Service URL")
                {
                    ToolTip = '', Locked = true;
                }
                field("Client Id"; Rec."Client Id")
                {
                    ToolTip = '', Locked = true;
                }
                field("User Name"; Rec."User Name")
                {
                    ToolTip = '', Locked = true;
                }
                field(Password; Rec.Password)
                {
                    ToolTip = '', Locked = true;
                }
                field("Job Journal Template Name"; Rec."Job Journal Template Name")
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Job Journal Batch Name"; Rec."Job Journal Batch Name")
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Sales Invoice Reason"; Rec."Sales Invoice Reason")
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Time Entry Reason Code"; rec."Time Entry Reason Code")
                {
                    ToolTip = ' ', Locked = true;
                }
            }
        }
    }
    trigger OnOpenPage()
    begin
        Rec.Reset();
        if not Rec.Get() then begin
            Rec.Init();
            Rec.Insert();
        end;
    end;
}
