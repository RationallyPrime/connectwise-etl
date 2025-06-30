page 62008 "PTE Manage Invoice Errors"
{
    ApplicationArea = All;
    Caption = 'Posting Errors';
    PageType = List;
    SourceTable = "PTE Manage Invoice Error Logo";
    Editable = false;

    layout
    {
        area(content)
        {
            repeater(General)
            {
                field("Error Table id"; Rec."Error Table id")
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Table Name"; Rec."Table Name")
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Missing Value"; Rec."Missing Value")
                {
                    ToolTip = ' ', Locked = true;
                }
                field("Error Message"; Rec."Error Message")
                {
                    ToolTip = ' ', Locked = true;
                }
            }
        }
    }
}
