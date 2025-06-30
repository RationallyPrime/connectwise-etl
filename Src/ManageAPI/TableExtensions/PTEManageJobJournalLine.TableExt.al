tableextension 62000 "PTE Manage Job Journal Line" extends "Job Journal Line"
{
    trigger OnAfterDelete()
    var
        ManageInvoiceLine: Record "PTE Manage Invoice Line";
        ManageExpenses: Record "PTE Manage Invoice Expense";
    begin
        ManageInvoiceLine.SetRange("Invoice Number", "Document No.");
        ManageInvoiceLine.SetRange("Job Journal Line No.", "Line No.");
        if ManageInvoiceLine.FindFirst() then begin
            ManageInvoiceLine."Job Journal Line No." := 0;
            ManageInvoiceLine.Modify();
        end;

        ManageExpenses.SetRange("Job Journal Line No.", "Line No.");
        if ManageExpenses.FindFirst() then begin
            ManageExpenses."Job Journal Line No." := 0;
            ManageExpenses.Modify();
        end;
    end;
}