pageextension 62001 "PTE Work Type" extends "Work Types"
{
    layout
    {
        addlast(Control1)
        {
            field("PTE Manage Work Type Name"; Rec."PTE Manage Work Type Name")
            {
                ToolTip = ' ', Locked = true;
                ApplicationArea = All;
            }
        }
    }
}

