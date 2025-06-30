tableextension 62001 "PTE Manage Sales Header" extends "Sales Header"
{

    trigger OnAfterDelete()
    var
        ManageInvoiceHeader: Record "PTE Manage Invoice Header";
    begin
        if "Document Type" = "Document Type"::Invoice then begin
            ManageInvoiceHeader.SetRange("Sales Invoice No.", "No.");
            if ManageInvoiceHeader.FindFirst() then begin
                ManageInvoiceHeader."Sales Invoice No." := '';
                ManageInvoiceHeader.Modify();
            end;
        end;
    end;

}