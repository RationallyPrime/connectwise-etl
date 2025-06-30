reportextension 62000 "PTE Job Sales Invoice Wise" extends "REP Job Sales Invoice Std."
{
    dataset
    {
        add("Sales Invoice Line")
        {
            column(WorkTypeCode;"Sales Invoice Line"."Work Type Code") { }
            column(WorkTypeCaption; workTypeLbl) { }
        }
    }
    rendering
    {
        layout(Custom)
        {
            Type = RDLC;
            LayoutFile = './Src/ManageAPI/ReportExtensions/PTEJobSalesInvoiceStd.rdlc';
            Caption = 'Custom layout';
        }
    }
    var
        workTypeLbl: Label 'WT', Locked = true;

}
