codeunit 62002 "PTE Create Sales Invoices"
{
    TableNo = "PTE Manage Invoice Header";
    trigger OnRun()
    begin
        ManageHeader.Copy(Rec);
        CreateSalesInvoiceForManageInvoice();
        Rec := ManageHeader;
    end;

    var
        ManageHeader: Record "PTE Manage Invoice Header";

    local procedure CreateSalesInvoiceForManageInvoice()
    var
        SalesHeader: Record "Sales Header";
        ManageAPISetup: Record "PTE Manage API Setup";
    begin
        if ManageHeader."Sales Invoice No." <> '' then
            exit;

        ManageAPISetup.Get();
        ManageAPISetup.TestField("Sales Invoice Reason");

        SalesHeader.Init();
        SalesHeader."Document Type" := SalesHeader."Document Type"::Invoice;
        SalesHeader.Validate("Sell-to Customer No.", GetCustomerNoInBC(ManageHeader."Social Security No."));
        SalesHeader.Validate("Posting Date", DT2Date(ManageHeader."Invoice Date"));
        SalesHeader."External Document No." := ManageHeader."Agreement Number";
        SalesHeader."Your Reference" := 'M' + ManageHeader."Invoice Number";
        SalesHeader."Reason Code" := ManageAPISetup."Sales Invoice Reason";
        if CheckIfShipToCodeExist(SalesHeader."Sell-to Customer No.", CopyStr(SalesHeader."External Document No.", 1, 10)) then
            SalesHeader.Validate("Ship-to Code", SalesHeader."External Document No.");
        SalesHeader.Insert(true);
        SalesHeader.SetWorkDescription(ManageHeader.Site);
        SalesHeader.Validate("Document Date", DT2Date(ManageHeader."Invoice Date"));
        SalesHeader.Modify(true);

        CreateSalesInvoiceLinesForManageInvoice(ManageHeader."Invoice Number", SalesHeader, DT2Date(ManageHeader."Invoice Date"));
        ManageHeader."Sales Invoice No." := SalesHeader."No.";
        ManageHeader.Modify();
    end;

    local procedure CreateSalesInvoiceLinesForManageInvoice(ManageInvoiceNo: Code[10]; SalesHeader: Record "Sales Header"; InvoiceDate: Date)
    var
        ManageInvoiceLine: Record "PTE Manage Invoice Line";
        SalesInvoiceLine: Record "Sales Line";
        LineNo: Integer;
    begin
        InsertPeriodInfoLine(SalesInvoiceLine, SalesHeader."Language Code", SalesHeader."No.", InvoiceDate);
        InsertEmptyLine(SalesInvoiceLine, SalesHeader."No.");

        LineNo := 30000;
        ManageInvoiceLine.SetRange("Invoice Number", ManageInvoiceNo);
        ManageInvoiceLine.SetFilter("Item Identifier", '<>%1', '');
        ManageInvoiceLine.SetFilter(Memo, '<>%1', 'Auð lína');
        if ManageInvoiceLine.FindSet() then
            repeat
                SalesInvoiceLine.Init();
                SalesInvoiceLine.InitHeaderDefaults(SalesHeader);
                SalesInvoiceLine."Document Type" := SalesInvoiceLine."Document Type"::Invoice;
                SalesInvoiceLine."Document No." := SalesHeader."No.";
                SalesInvoiceLine."Line No." := LineNo;
                if ManageInvoiceLine."Item Identifier" = 'SALE0000' then begin
                    SalesInvoiceLine.Type := SalesInvoiceLine.Type::" ";
                    SalesInvoiceLine.Description := CopyStr(ManageInvoiceLine.Memo, 1, MaxStrLen(SalesInvoiceLine.Description));
                end else begin
                    SalesInvoiceLine.Type := SalesInvoiceLine.Type::Item;
                    SalesInvoiceLine.Validate("No.", GetItemNumber(ManageInvoiceLine."Item Identifier"));
                    SalesInvoiceLine.Description := CopyStr(ManageInvoiceLine.Memo, 1, MaxStrLen(SalesInvoiceLine.Description));
                    SalesInvoiceLine.Validate("Unit Price", Round(GetUnitPriceForManageItem(ManageInvoiceLine), 1));
                    SalesInvoiceLine.Validate("Quantity", ManageInvoiceLine."Quantity" * -1);
                    SalesInvoiceLine.Validate("Line Discount %", GetDiscountForManageItem(ManageInvoiceLine));
                end;
                SalesInvoiceLine.Insert();
                LineNo += 10000;
            until ManageInvoiceLine.Next() = 0;
    end;

    local procedure GetUnitPriceForManageItem(ManageInvoiceLine: Record "PTE Manage Invoice Line"): Decimal
    var
        DiscountProcent: Decimal;
    begin
        DiscountProcent := GetDiscountForManageItem(ManageInvoiceLine);
        if (DiscountProcent > 0) and (DiscountProcent < 100) then
            exit((ManageInvoiceLine."Line Amount" * 100) / (ManageInvoiceLine.Quantity * (100 - DiscountProcent)))
        else
            exit(ManageInvoiceLine."Line Amount" / ManageInvoiceLine.Quantity);
    end;

    local procedure GetDiscountForManageItem(ManageInvoiceLine: Record "PTE Manage Invoice Line"): Decimal
    var
        ManageProduct: Record "PTE Manage Product";
    begin
        if ManageProduct.Get(ManageInvoiceLine."Invoice Number", ManageInvoiceLine."Product ID") then
            exit(ManageProduct."Discount %");
    end;

    procedure GetCustomerNoInBC(SocialSecurityNo: Code[20]): Code[20]
    var
        CustomerNo: Code[20];
    begin
        if StrLen(SocialSecurityNo) = 10 then begin
            CustomerNo := CopyStr(SocialSecurityNo, 1, 6) + '-' + CopyStr(SocialSecurityNo, 7, 4);
            exit(CustomerNo);
        end else
            exit(SocialSecurityNo);
    end;

    local procedure InsertPeriodInfoLine(var SalesInvoiceLine: Record "Sales Line"; SalesHeaderLanguage: Code[10]; SalesInvoiceNo: Code[20]; InvoiceDate: Date)
    var
        Language: Codeunit Language;
        PeriodTxt: Label 'Period: %1 - %2', Comment = '%1 = From Date, %2 = To Date.';
        FromDate: Date;
        ToDate: Date;
        CurrentLangNo: Integer;
        SalesHeaderLangNo: Integer;
    begin
        GetPeriodFromToDates(InvoiceDate, FromDate, ToDate);
        SalesInvoiceLine.Init();
        SalesInvoiceLine."Document Type" := SalesInvoiceLine."Document Type"::Invoice;
        SalesInvoiceLine."Document No." := SalesInvoiceNo;
        SalesInvoiceLine."Line No." := 10000;
        SalesInvoiceLine.Type := SalesInvoiceLine.Type::" ";
        SalesInvoiceLine.Description := StrSubstNo(PeriodTxt, FromDate, ToDate);
        if (SalesHeaderLanguage <> '') then begin
            SalesHeaderLangNo := Language.GetLanguageID(SalesHeaderLanguage);
            CurrentLangNo := GlobalLanguage;
            if (SalesHeaderLangNo <> CurrentLangNo) then begin
                GlobalLanguage := Language.GetLanguageID(SalesHeaderLanguage);
                SalesInvoiceLine.Description := StrSubstNo(PeriodTxt, FromDate, ToDate);
                GlobalLanguage := CurrentLangNo;
            end;
        end;
        SalesInvoiceLine.Insert();
    end;

    local procedure GetPeriodFromToDates(InvoiceDate: Date; var FromDate: Date; var ToDate: Date)
    begin
        if InvoiceDate = CALCDATE('<-CM>', InvoiceDate) then begin
            FromDate := InvoiceDate;
            ToDate := CALCDATE('<CM>', InvoiceDate);
        end else begin
            FromDate := CALCDATE('<-CM>', InvoiceDate);
            ToDate := InvoiceDate;
        end;
    end;

    local procedure CheckIfShipToCodeExist(CustomerNo: Code[20]; ShipToCode: Code[10]): Boolean
    var
        ShipToAdress: Record "Ship-to Address";
    begin
        ShipToAdress.SetRange("Customer No.", CustomerNo);
        ShipToAdress.SetRange("Code", ShipToCode);
        if not ShipToAdress.IsEmpty() then
            exit(true);
    end;

    local procedure InsertEmptyLine(var SalesInvoiceLine: Record "Sales Line"; SalesInvoiceNo: Code[20])
    begin
        SalesInvoiceLine.Init();
        SalesInvoiceLine."Document Type" := SalesInvoiceLine."Document Type"::Invoice;
        SalesInvoiceLine."Document No." := SalesInvoiceNo;
        SalesInvoiceLine."Line No." := 20000;
        SalesInvoiceLine.Type := SalesInvoiceLine.Type::" ";
        SalesInvoiceLine.Validate("No.", '02');
        SalesInvoiceLine.Insert();
    end;

    procedure GetItemNumber(ItemIdentifier: Code[20]): Code[20]
    var
        Item: Record Item;
        ItemReference: Record "Item Reference";
    begin
        Item.SetLoadFields("No.");
        if Item.Get(ItemIdentifier) then
            exit(Item."No.");

        ItemReference.SetLoadFields("Item No.");
        ItemReference.SetRange("Reference No.", ItemIdentifier);
        if ItemReference.FindFirst() then
            exit(ItemReference."Item No.");
    end;
}