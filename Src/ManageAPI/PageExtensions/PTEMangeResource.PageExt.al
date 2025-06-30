pageextension 62004 "PTE Mange Resource" extends "Resource Card"
{
    layout
    {
        addlast(General)
        {
            field("PTE Manage Resource Id"; Rec."PTE Manage Resource Id")
            {
                ApplicationArea = All;
                ToolTip = 'Resource code in Manage system';
            }
        }
    }
}
