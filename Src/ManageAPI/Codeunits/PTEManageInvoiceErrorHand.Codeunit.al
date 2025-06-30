codeunit 62004 "PTE Manage Invoice Error Hand"
{
    procedure CreateManageError(InvoiceNumber: Code[10]; TableId: Integer; FieldValue: Variant; ErrorText: Text[250])
    var
        ManageError: Record "PTE Manage Invoice Error Logo";
    begin
        ManageError.SetRange("Invoice Number", InvoiceNumber);
        ManageError.SetRange("Error Table id", TableId);
        ManageError.SetRange("Missing Value", FieldValue);
        if not ManageError.FindFirst() then begin
            ManageError.Init();
            ManageError.Validate("Invoice Number", InvoiceNumber);
            ManageError.Validate("Error Table id", TableId);
            ManageError.Validate("Missing Value", FieldValue);
            ManageError.Validate("Error Message", ErrorText);
            ManageError.Insert();
        end else begin
            ManageError.Validate("Error Message", ErrorText);
            ManageError.Modify();
        end;
    end;

    procedure CheckIfManageInvoiceHeaderHasErrors(ManageInvoiceHeader: Record "PTE Manage Invoice Header") ErrorExists: Boolean
    var
        ManageError: Record "PTE Manage Invoice Error Logo";
    begin
        ManageError.SetRange("Invoice Number", ManageInvoiceHeader."Invoice Number");
        ManageError.DeleteAll();

        if ManageInvoiceHeader."Invoice Type" = ManageInvoiceHeader."Invoice Type"::Agreement then
            if CollectManagementItemError(ManageInvoiceHeader) or CollectManagementCustomerError(ManageInvoiceHeader) or CheckIfAgreementNumberIsMissing(ManageInvoiceHeader) then
                exit(true);
    end;

    procedure CollectManagementItemError(ManageInvoiceHeader: Record "PTE Manage Invoice Header") ErrorExists: Boolean
    var
        ManageInvoicelines: Record "PTE Manage Invoice Line";
        Item: Record Item;
        CreateSalesInvoices: Codeunit "PTE Create Sales Invoices";
        ItemErrorMessageLbl: Label 'Manage Item  %1 not found in the Item List', Comment = '%1 Item No.';
    begin
        ManageInvoicelines.SetRange("Invoice Number", ManageInvoiceHeader."Invoice Number");
        ManageInvoicelines.SetFilter("Item Identifier", '<>%1&<>%2', '', 'SALE0000');
        ManageInvoicelines.SetFilter(Memo, '<>%1', 'Auð lína');
        if ManageInvoicelines.FindSet() then
            repeat
                if CreateSalesInvoices.GetItemNumber(ManageInvoicelines."Item Identifier") = '' then begin
                    CreateManageError(ManageInvoicelines."Invoice Number", Item.RecordId.TableNo, ManageInvoicelines."Item Identifier", StrSubstNo(ItemErrorMessageLbl, ManageInvoicelines."Item Identifier"));
                    if not ErrorExists then
                        ErrorExists := true;
                end;
            until ManageInvoicelines.Next() = 0;
    end;

    procedure CollectManagementCustomerError(ManageInvoiceHeader: Record "PTE Manage Invoice Header") ErrorExists: Boolean
    var
        Customer: Record Customer;
        CreateSalesInvoices: Codeunit "PTE Create Sales Invoices";
        CustomerNo: Code[20];
        CustomerErrorMessageLbl: Label 'Manage Customer %1 not found in the Customer List', Comment = '%1 Customer No.';
    begin
        CustomerNo := CreateSalesInvoices.GetCustomerNoInBC(ManageInvoiceHeader."Social Security No.");
        if not Customer.Get(CustomerNo) then begin
            CreateManageError(ManageInvoiceHeader."Invoice Number", Customer.RecordId.TableNo, CustomerNo, StrSubstNo(CustomerErrorMessageLbl, CustomerNo));
            if not ErrorExists then
                ErrorExists := true;
        end;
    end;

    procedure CheckIfAgreementNumberIsMissing(ManageInvoiceHeader: Record "PTE Manage Invoice Header") ErrorExists: Boolean
    var
        AgreementNumberErrorMessageLbl: Label 'Agreement Number is missing for Invoice %1', Comment = '%1 Invoice No.';
    begin
        if ManageInvoiceHeader."Agreement Number" = '' then begin
            CreateManageError(ManageInvoiceHeader."Invoice Number", ManageInvoiceHeader.RecordId.TableNo, '', StrSubstNo(AgreementNumberErrorMessageLbl, ManageInvoiceHeader."Invoice Number"));
            ErrorExists := true;
        end;
    end;
}
