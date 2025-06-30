page 62004 "PTE Manage Invoice Lines"
{
    ApplicationArea = All;
    Caption = 'Invoice Lines';
    PageType = ListPart;
    SourceTable = "PTE Manage Invoice Line";

    layout
    {
        area(content)
        {
            repeater(General)
            {
                field(Memo; Rec.Memo)
                {
                    ToolTip = '', Locked = true;
                }
                field(Description; Rec.Description)
                {
                    ToolTip = '', Locked = true;
                }
                field("Document Date"; Rec."Document Date")
                {
                    ToolTip = '', Locked = true;
                }
                field("Time Entry Id"; Rec."Time Entry Id")
                {
                    ToolTip = '', Locked = true;
                    Visible = ShowStandardField;
                }
                field("Product ID"; Rec."Product ID")
                {
                    ToolTip = '', Locked = true;
                    Visible = ShowAgreementField;
                }
                field("Item Identifier"; Rec."Item Identifier")
                {
                    ToolTip = '', Locked = true;
                    Visible = ShowAgreementField;
                }
                field(Quantity; Rec.Quantity)
                {
                    ToolTip = '', Locked = true;
                }
                field(Price; Rec.Price)
                {
                    ToolTip = '', Locked = true;
                    Visible = ShowAgreementField;
                }
                field(Cost; Rec.Cost)
                {
                    ToolTip = '', Locked = true;
                    Visible = ShowAgreementField;
                }
                field("Line Amount"; Rec."Line Amount")
                {
                    ToolTip = '', Locked = true;
                }
            }
        }
    }
    actions
    {
        area(Processing)
        {
            action(ShowTimeEntryDetails)
            {
                Caption = 'Time Entry Details';
                Image = Timeline;
                RunObject = page "PTE Manage Time Entry";
                RunPageLink = "Time Entry Id" = field("Time Entry Id");
                ToolTip = '', Locked = true;
                Visible = ShowStandardField;
            }
            action(ShowProdcutDetails)
            {
                Caption = 'Product Details';
                Image = Production;
                RunObject = page "PTE Manage Product";
                RunPageLink = "Product Id" = field("Product ID");
                ToolTip = '', Locked = true;
                Visible = ShowAgreementField;
            }
        }
    }
    trigger OnAfterGetRecord()
    var
        ManageInvoiceHeader: Record "PTE Manage Invoice Header";
    begin
        ManageInvoiceHeader.SetRange("Invoice Number", Rec."Invoice Number");
        if ManageInvoiceHeader.FindFirst() then
            if ManageInvoiceHeader."Invoice Type" = ManageInvoiceHeader."Invoice Type"::Standard then
                ShowStandardField := true
            else
                ShowAgreementField := true;
    end;

    var
        ShowAgreementField: Boolean;
        ShowStandardField: Boolean;
}
