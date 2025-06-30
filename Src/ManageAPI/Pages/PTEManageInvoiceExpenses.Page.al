page 62001 "PTE Manage Invoice Expenses"
{
    ApplicationArea = All;
    Caption = 'Invoice Expenses';
    Editable = false;
    PageType = List;
    SourceTable = "PTE Manage Invoice Expense";

    layout
    {
        area(content)
        {
            repeater(General)
            {
                field("Invoice Number"; Rec."Invoice Number")
                {
                    ToolTip = '', Locked = true;
                }
                field("Type"; Rec."Type")
                {
                    ToolTip = '', Locked = true;
                }
                field(Employee; Rec.Employee)
                {
                    ToolTip = '', Locked = true;
                }
                field(Quantity; Rec.Quantity)
                {
                    ToolTip = '', Locked = true;
                }
                field(Amount; Rec.Amount)
                {
                    ToolTip = '', Locked = true;
                }
                field("Work Date"; DT2Date(Rec."Work Date"))
                {
                    ToolTip = '', Locked = true;
                    Caption = 'Work Date';
                }
                field("Agreement Number"; Rec."Agreement Number")
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Type"; Rec."Agreement Type")
                {
                    ToolTip = '', Locked = true;
                }
            }
        }
    }
}
