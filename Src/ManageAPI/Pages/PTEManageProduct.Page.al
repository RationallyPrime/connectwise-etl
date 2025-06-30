page 62007 "PTE Manage Product"
{
    ApplicationArea = All;
    Caption = 'Product';
    DataCaptionExpression = Format(Rec."Product Id");
    Editable = false;
    PageType = Card;
    SourceTable = "PTE Manage Product";

    layout
    {
        area(content)
        {
            group(General)
            {
                Caption = 'General';

                field("Product Id"; Rec."Product Id")
                {
                    ToolTip = '', Locked = true;
                }
                field(Description; Rec.Description)
                {
                    ToolTip = '', Locked = true;
                }
                field(Discount; Rec."Discount %")
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
            }
        }
    }
}
