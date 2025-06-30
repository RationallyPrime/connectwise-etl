page 62002 "Manage API Time Entries"
{
    ApplicationArea = All;
    Caption = 'Manage API Time Entries';
    PageType = List;
    InsertAllowed = false;
    SourceTable = "PTE Manage Time Entry";
    UsageCategory = Lists;

    layout
    {
        area(Content)
        {
            repeater(General)
            {
                field("Invoice Number"; Rec."Invoice Number")
                {
                    ToolTip = '', Locked = true;
                }
                field("Time Entry Id"; Rec."Time Entry Id")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Id"; Rec."Agreement Id")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Number"; Rec."Agreement Number")
                {
                    ToolTip = '', Locked = true;
                }
                field(Employee; Rec.Employee)
                {
                    ToolTip = '', Locked = true;
                }
                field("Site Name"; Rec."Site Name")
                {
                    ToolTip = '', Locked = true;
                }
                field(Note; Rec.Note)
                {
                    ToolTip = '', Locked = true;
                }
                field("Work Role Id"; Rec."Work Role Id")
                {
                    ToolTip = '', Locked = true;
                }
                field("Work Type"; Rec."Work Type")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Amount"; Rec."Agreement Amount")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Hours"; Rec."Agreement Hours")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Work Role Rate"; Rec."Agreement Work Role Rate")
                {
                    ToolTip = '', Locked = true;
                }
                field("Ticket Id"; Rec."Ticket Id")
                {
                    ToolTip = '', Locked = true;
                }
            }
        }
    }
}
