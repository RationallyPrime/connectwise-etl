page 62006 "PTE Manage Time Entry"
{
    ApplicationArea = All;
    Caption = 'Time Entry';
    Editable = false;
    PageType = Document;
    SourceTable = "PTE Manage Time Entry";
    DataCaptionExpression = format(Rec."Time Entry Id");

    layout
    {
        area(content)
        {
            group(General)
            {
                Caption = 'General';

                field("Time Entry Id"; Rec."Time Entry Id")
                {
                    ToolTip = '', Locked = true;
                }
                field(Employee; Rec.Employee)
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Id"; Rec."Agreement Id")
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Agreement Number"; Rec."Agreement Number")
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Work Role Id"; Rec."Work Role Id")
                {
                    ToolTip = '', Locked = true;
                }
                field("Work Type"; Rec."Work Type")
                {
                    ToolTip = '', Locked = true;
                }
                field("Site Name"; Rec."Site Name")
                {
                    ToolTip = '', Locked = true;
                }
                field("Hourly Rate"; Rec."Hourly Rate")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Hours"; Rec."Agreement Hours")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Amount"; Rec."Agreement Amount")
                {
                    ToolTip = '', Locked = true;
                }
                field("Global Hourly Rate"; Rec."Global Hourly Rate")
                {
                    ToolTip = '', Locked = true;
                }
                field("Rate Type"; Rec."Rate Type")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Work Role Rate"; Rec."Agreement Work Role Rate")
                {
                    ToolTip = '', Locked = true;
                }
                field(Note; Rec.Note)
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Ticket Id"; Rec."Ticket Id")
                {
                    ToolTip = '', Locked = true;
                }
                field("Ticket Summary"; Rec."Ticket Summary")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Type"; rec."Agreement Type")
                {
                    ToolTip = '', Locked = true;
                }
            }
        }
    }
}
