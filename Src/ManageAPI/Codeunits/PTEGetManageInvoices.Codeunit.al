codeunit 62000 "PTE Get Manage Invoices"
{
    var
        Client: HttpClient;
        IsInitialized: Boolean;

    procedure GetInvoices()
    var
        InvoiceHeader: Record "PTE Manage Invoice Header";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        RecRef: RecordRef;
        InvoiceArray: JsonArray;
        InvoiceToken: JsonToken;
        RequestParameters: array[3] of Variant;
        ImportedInvoicesCount: Integer;
        InvoicesImportedLbl: Label 'Invoices have been imported.';
    begin
        InitializeClient();
        InvoiceArray.ReadFrom(ManageAPICalls.GetResponse('GET', 1, RequestParameters, Client));
        foreach InvoiceToken in InvoiceArray do
            if not CheckIfInvoiceExists(InvoiceToken.AsObject()) then begin
                RecRef.Open(Database::"PTE Manage Invoice Header");
                RecRef.Init();
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, InvoiceHeader.FieldNo("Billing Log Id"), InvoiceToken.AsObject(), 'billingLogId');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, InvoiceHeader.FieldNo("Invoice Number"), InvoiceToken.AsObject(), 'invoiceNumber');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, InvoiceHeader.FieldNo("Invoice Date"), InvoiceToken.AsObject(), 'invoiceDate');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, InvoiceHeader.FieldNo(Site), GetObjectFromParentObject(InvoiceToken.AsObject(), 'shipToSite'), 'name');
                ConvertInvoiceTypeTextToEnum(InvoiceToken.AsObject(), RecRef);
                Recref.Insert();
                RequestParameters[1] := RecRef.Field(InvoiceHeader.FieldNo("Billing Log Id")).Value;
                ProcessInvoiceDetails(ManageAPICalls.GetResponse('POST', 2, RequestParameters, Client), RecRef);
                ImportedInvoicesCount += 1;
            end;
        Message(Format(ImportedInvoicesCount) + ' ' + InvoicesImportedLbl);
    end;

    local procedure ProcessInvoiceDetails(ResponseText: Text; var RecRef: RecordRef)
    var
        ManageInvoiceHeader: Record "PTE Manage Invoice Header";
        PTEJsonManagement: Codeunit "PTE Json Management";
        InvoiceDetailsJson: JsonObject;
        CustomerJsonToken: JsonToken;
        DetailJsonToken: JsonToken;
        TransactionJsonToken: JsonToken;
    begin
        InvoiceDetailsJson.ReadFrom(ResponseText);
        if InvoiceDetailsJson.SelectToken('customers', CustomerJsonToken) then
            if CustomerJsonToken.AsArray().Get(0, CustomerJsonToken) then begin
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageInvoiceHeader.FieldNo("Social Security No."), CustomerJsonToken.AsObject(), 'accountNumber');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageInvoiceHeader.FieldNo("Customer Name"), GetObjectFromParentObject(CustomerJsonToken.AsObject(), 'company'), 'identifier');
            end;

        if InvoiceDetailsJson.SelectToken('transactions', TransactionJsonToken) then
            if TransactionJsonToken.AsArray().Get(0, TransactionJsonToken) then begin
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageInvoiceHeader.FieldNo("Total Amount"), TransactionJsonToken.AsObject(), 'total');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageInvoiceHeader.FieldNo("Total Amount without VAT"), TransactionJsonToken.AsObject(), 'taxableTotal');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageInvoiceHeader.FieldNo("Invoice ID"), TransactionJsonToken.AsObject(), 'id');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageInvoiceHeader.FieldNo("VAT %"), TransactionJsonToken.AsObject(), 'taxGroupRate');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageInvoiceHeader.FieldNo("GLEntry Ids"), TransactionJsonToken.AsObject(), 'glEntryIds');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageInvoiceHeader.FieldNo(Project), GetObjectFromParentObject(TransactionJsonToken.AsObject(), 'project'), 'name');
                ConvertVATToProcent(Recref);
            end;
        GetExpenses(Recref.Field(ManageInvoiceHeader.FieldNo("Invoice ID")).Value, RecRef.Field(ManageInvoiceHeader.FieldNo("Invoice Type")).Value, RecRef.Field(ManageInvoiceHeader.FieldNo("Invoice Number")).Value);

        RecRef.Modify();
        TransactionJsonToken.SelectToken('detail', DetailJsonToken);
        ProcessTransactionDetails(RecRef.Field(ManageInvoiceHeader.FieldNo("Invoice Number")).Value, DetailJsonToken, RecRef.Field(ManageInvoiceHeader.FieldNo("Invoice Type")).Value);
        RecRef.Close();
    end;

    local procedure ProcessTransactionDetails(InvoiceNumber: Code[10]; DetailJsonToken: JsonToken; InvoiceType: Enum "PTE Manage Invoice Type")
    var
        ManageinvoiceLine: Record "PTE Manage Invoice Line";
        PTEJsonManagement: Codeunit "PTE Json Management";
        RecRef: RecordRef;
        DetailToken: JsonToken;
        SkipLine: Boolean;
    begin
        RecRef.Open(Database::"PTE Manage Invoice Line");
        foreach DetailToken in DetailJsonToken.AsArray() do begin
            Clear(SkipLine);
            RecRef.Init();
            RecRef.Field(ManageinvoiceLine.FieldNo("Invoice Number")).Value := InvoiceNumber;
            RecRef.Field(ManageinvoiceLine.FieldNo("Line No.")).Value := GetManageinvoiceLineNo(InvoiceNumber);
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo(Memo), DetailToken.AsObject(), 'memo');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo(Quantity), DetailToken.AsObject(), 'quantity');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo("Line Amount"), DetailToken.AsObject(), 'total');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo(Description), DetailToken.AsObject(), 'description');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo("Document Date"), DetailToken.AsObject(), 'documentDate');
            if InvoiceType = InvoiceType::Agreement then begin
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo(Price), DetailToken.AsObject(), 'itemPrice');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo(Cost), DetailToken.AsObject(), 'itemCost');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo("Item Identifier"), GetObjectFromParentObject(DetailToken.AsObject(), 'item'), 'identifier');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo("Product ID"), GetObjectFromParentObject(DetailToken.AsObject(), 'product'), 'id');
                GetProductData(RecRef.Field(ManageinvoiceLine.FieldNo("Product ID")).Value, InvoiceNumber, RecRef.Field(ManageinvoiceLine.FieldNo("Item Identifier")).Value);
            end else begin
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageinvoiceLine.FieldNo("Time Entry ID"), GetObjectFromParentObject(DetailToken.AsObject(), 'timeEntry'), 'id');
                GetTimeEntryData(RecRef.Field(ManageinvoiceLine.FieldNo("Time Entry ID")).Value, InvoiceNumber, SkipLine);
            end;
            if not SkipLine then
                RecRef.Insert();
        end;
        RecRef.Close();
    end;

    local procedure GetTimeEntryData(TimeEntryId: Integer; InvoiceNumber: Code[10]; var SkipLine: Boolean)
    var
        ManageTimeEntry: Record "PTE Manage Time Entry";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        RecRef: RecordRef;
        TimeEntryJson: JsonObject;
        RequestParameters: array[3] of Variant;
    begin
        if TimeEntryId = 0 then
            exit;

        RequestParameters[1] := TimeEntryId;
        TimeEntryJson.ReadFrom(ManageAPICalls.GetResponse('GET', 3, RequestParameters, Client));
        RecRef.Open(Database::"PTE Manage Time Entry");
        RecRef.Init();
        RecRef.Field(ManageTimeEntry.FieldNo("Time Entry Id")).Value := TimeEntryId;
        RecRef.Field(ManageTimeEntry.FieldNo("Invoice Number")).Value := InvoiceNumber;
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo(Employee), GetObjectFromParentObject(TimeEntryJson, 'member'), 'identifier');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Agreement ID"), GetObjectFromParentObject(TimeEntryJson, 'agreement'), 'id');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Work Role Id"), GetObjectFromParentObject(TimeEntryJson, 'workRole'), 'id');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Work Type"), GetObjectFromParentObject(TimeEntryJson, 'workType'), 'name');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Hourly Rate"), TimeEntryJson, 'hourlyRate');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Agreement Hours"), TimeEntryJson, 'agreementHours');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Agreement Amount"), TimeEntryJson, 'agreementAmount');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo(Note), TimeEntryJson, 'notes');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Ticket Id"), GetObjectFromParentObject(TimeEntryJson, 'ticket'), 'id');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Work Date"), TimeEntryJson, 'dateEntered');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Ticket Summary"), GetObjectFromParentObject(TimeEntryJson, 'ticket'), 'summary');
        GetParentAgreementIdAndType(RecRef, RecRef.Field(ManageTimeEntry.FieldNo("Agreement ID")).Value);
        GetAgreementNumber(RecRef, RecRef.Field(ManageTimeEntry.FieldNo("Agreement ID")).Value);
        CheckAgreementNumber(RecRef, RecRef.Field(ManageTimeEntry.FieldNo("Agreement Number")).Value, RecRef.Field(ManageTimeEntry.FieldNo("Parent Agreement Id")).Value);
        SkipLine := CheckIfAgreementIsTimapottur(RecRef.Field(ManageTimeEntry.FieldNo("Agreement Type")).Value);
        if not SkipLine then
            RecRef.Insert();
        RecRef.Close();
    end;

    local procedure GetGlobalHouryRate(var RecRef: RecordRef)
    var
        ManageTimeEntry: Record "PTE Manage Time Entry";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        WorkRolesJson: JsonObject;
        RequestParameters: array[3] of Variant;
        WorkRoleId: Integer;
    begin
        WorkRoleId := RecRef.Field(ManageTimeEntry.FieldNo("Work Role Id")).Value;
        if WorkRoleId = 0 then
            exit;

        RequestParameters[1] := WorkRoleId;
        WorkRolesJson.ReadFrom(ManageAPICalls.GetResponse('GET', 4, RequestParameters, Client));
        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageTimeEntry.FieldNo("Global Hourly Rate"), WorkRolesJson, 'hourlyRate');
    end;

    local procedure GetAgreementWorkRoleRate(var RecRef: RecordRef)
    var
        ManageTimeEntry: Record "PTE Manage Time Entry";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        WorkRoleJsonArray: JsonArray;
        RoleJsonToken: JsonToken;
        RequestParameters: array[3] of Variant;
        AgreementId: Integer;
        WorkRoleId: Integer;
    begin
        AgreementId := RecRef.Field(ManageTimeEntry.FieldNo("Agreement Id")).Value;
        WorkRoleId := RecRef.Field(ManageTimeEntry.FieldNo("Work Role Id")).Value;
        if (AgreementId = 0) or (WorkRoleId = 0) then
            exit;

        RequestParameters[1] := AgreementId;
        RequestParameters[2] := WorkRoleId;
        if WorkRoleJsonArray.ReadFrom(ManageAPICalls.GetResponse('GET', 5, RequestParameters, Client)) then
            if WorkRoleJsonArray.Get(0, RoleJsonToken) then begin
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageTimeEntry.FieldNo("Rate Type"), RoleJsonToken.AsObject(), 'rateType');
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageTimeEntry.FieldNo("Agreement Work Role Rate"), RoleJsonToken.AsObject(), 'rate');
            end;
    end;

    local procedure GetSiteName(InvoiceNumber: Code[10]; var RecRef: RecordRef)
    var
        ManageInvoiceHeader: Record "PTE Manage Invoice Header";
        ManageTimeEntry: Record "PTE Manage Time Entry";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        SiteJson: JsonObject;
        RequestParameters: array[3] of Variant;
        TicketId: Integer;
        AgreementId: Integer;
    begin
        TicketId := RecRef.Field(ManageTimeEntry.FieldNo("Ticket Id")).Value;
        if TicketId > 0 then begin
            RequestParameters[1] := TicketId;
            ManageInvoiceHeader.SetRange("Invoice Number", InvoiceNumber);
            if ManageInvoiceHeader.FindFirst() then
                if ManageInvoiceHeader.Project <> '' then
                    SiteJson.ReadFrom(ManageAPICalls.GetResponse('GET', 6, RequestParameters, Client))
                else
                    SiteJson.ReadFrom(ManageAPICalls.GetResponse('GET', 7, RequestParameters, Client));
        end;
        if not SiteJson.Contains('site') then begin
            AgreementId := RecRef.Field(ManageTimeEntry.FieldNo("Agreement Id")).Value;
            if AgreementId > 0 then begin
                RequestParameters[1] := AgreementId;
                SiteJson.ReadFrom(ManageAPICalls.GetResponse('GET', 8, RequestParameters, Client));
            end;
        end;
        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageTimeEntry.FieldNo("Site Name"), GetObjectFromParentObject(SiteJson, 'site'), 'name');
    end;

    local procedure GetExpenses(InvoiceId: Integer; InvoiceType: Enum "PTE Manage Invoice Type"; InvoiceNumber: Code[10])
    var
        ManageInvoiceExpenses: Record "PTE Manage Invoice Expense";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        RecRef: RecordRef;
        ExpenseJsonArray: JsonArray;
        ExpenseJsonToken: JsonToken;
        RequestParameters: array[3] of Variant;
    begin
        if (InvoiceType = InvoiceType::Agreement) or (InvoiceId = 0) then
            exit;

        RecRef.Open(Database::"PTE Manage Invoice Expense");
        RequestParameters[1] := InvoiceId;
        ExpenseJsonArray.ReadFrom(ManageAPICalls.GetResponse('GET', 12, RequestParameters, Client));
        foreach ExpenseJsonToken in ExpenseJsonArray do begin
            RecRef.Init();
            RecRef.Field(ManageInvoiceExpenses.FieldNo("Invoice ID")).Value := InvoiceId;
            RecRef.Field(ManageInvoiceExpenses.FieldNo("Invoice Number")).Value := InvoiceNumber;
            RecRef.Field(ManageInvoiceExpenses.FieldNo("Line No.")).Value := GetExpenceLineNo(InvoiceId);
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo(Type), GetObjectFromParentObject(ExpenseJsonToken.AsObject(), 'type'), 'name');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo(Quantity), ExpenseJsonToken.AsObject(), 'amount');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo(Amount), ExpenseJsonToken.AsObject(), 'invoiceAmount');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo("Work Date"), ExpenseJsonToken.AsObject(), 'date');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo(Employee), GetObjectFromParentObject(ExpenseJsonToken.AsObject(), 'member'), 'identifier');
            PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo("Agreement Id"), GetObjectFromParentObject(ExpenseJsonToken.AsObject(), 'agreement'), 'id');
            GetParentAgreementIdAndType(RecRef, RecRef.Field(ManageInvoiceExpenses.FieldNo("Agreement Id")).Value);
            GetAgreementNumber(RecRef, RecRef.Field(ManageInvoiceExpenses.FieldNo("Agreement Id")).Value);
            CheckAgreementNumber(RecRef, RecRef.Field(ManageInvoiceExpenses.FieldNo("Agreement Number")).Value, RecRef.Field(ManageInvoiceExpenses.FieldNo("Parent Agreement Id")).Value);
            RecRef.Insert();
        end;
        RecRef.Close();
    end;

    local procedure GetProductData(ProductId: Integer; InvoiceNumber: Code[10]; ItemIdentifer: Code[20])
    var
        ManageProduct: Record "PTE Manage Product";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        RecRef: RecordRef;
        ProdcutSalesJsonObject: JsonObject;
        RequestParameters: array[3] of Variant;
    begin
        if ProductId = 0 then
            exit;

        RecRef.Open(Database::"PTE Manage Product");
        RequestParameters[1] := ProductId;
        ProdcutSalesJsonObject.ReadFrom(ManageAPICalls.GetResponse('GET', 9, RequestParameters, Client));
        RecRef.Init();
        RecRef.Field(1).Value := ProductId;
        RecRef.Field(2).Value := InvoiceNumber;
        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageProduct.FieldNo(Description), ProdcutSalesJsonObject, 'customerDescription');
        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageProduct.FieldNo("Agreement Id"), GetObjectFromParentObject(ProdcutSalesJsonObject, 'agreement'), 'id');
        GetParentAgreementIdAndType(RecRef, RecRef.Field(ManageProduct.FieldNo("Agreement Id")).Value);
        GetAgreementNumber(RecRef, RecRef.Field(ManageProduct.FieldNo("Agreement Id")).Value);
        CheckAgreementNumber(RecRef, RecRef.Field(ManageProduct.FieldNo("Agreement Number")).Value, RecRef.Field(ManageProduct.FieldNo("Parent Agreement Id")).Value);
        if ItemIdentifer <> 'SALE0000' then
            GetProductDiscount(RecRef, ItemIdentifer);
        RecRef.Insert();
        RecRef.Close();
    end;

    local procedure GetParentAgreementIdAndType(var RecRef: RecordRef; AgreementId: Integer)
    var
        ManageInvoiceExpenses: Record "PTE Manage Invoice Expense";
        ManageInvoiceTimeEntry: Record "PTE Manage Time Entry";
        ManageProduct: Record "PTE Manage Product";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        PTEJsonManagement: Codeunit "PTE Json Management";
        AgreementJsonObject: JsonObject;
        ParentAgreementdJsonToken: JsonToken;
        RequestParameters: array[3] of Variant;
    begin
        if AgreementId = 0 then
            exit;

        RequestParameters[1] := AgreementId;
        AgreementJsonObject.ReadFrom(ManageAPICalls.GetResponse('GET', 11, RequestParameters, Client));
        case RecRef.RecordId.TableNo of
            Database::"PTE Manage Time Entry":
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceTimeEntry.FieldNo("Agreement Type"), GetObjectFromParentObject(AgreementJsonObject, 'type'), 'name');
            Database::"PTE Manage Invoice Expense":
                PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo("Agreement Type"), GetObjectFromParentObject(AgreementJsonObject, 'type'), 'name');
        end;

        if AgreementJsonObject.SelectToken('parentAgreement', ParentAgreementdJsonToken) then
            if ParentAgreementdJsonToken.AsObject().SelectToken('id', ParentAgreementdJsonToken) then
                case RecRef.RecordId.TableNo of
                    Database::"PTE Manage Product":
                        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageProduct.FieldNo("Parent Agreement Id"), GetObjectFromParentObject(AgreementJsonObject, 'parentAgreement'), 'id');
                    Database::"PTE Manage Time Entry":
                        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceTimeEntry.FieldNo("Parent Agreement Id"), GetObjectFromParentObject(AgreementJsonObject, 'parentAgreement'), 'id');
                    Database::"PTE Manage Invoice Expense":
                        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo("Parent Agreement Id"), GetObjectFromParentObject(AgreementJsonObject, 'parentAgreement'), 'id');
                end;
    end;

    local procedure GetAgreementNumber(var RecRef: RecordRef; AgreementId: Integer)
    var
        ManageProduct: Record "PTE Manage Product";
        ManageTimeEntry: Record "PTE Manage Time Entry";
        ManageInvoiceExpenses: Record "PTE Manage Invoice Expense";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        AgreementJsonObject: JsonObject;
        CustomFieldJsonToken: JsonToken;
        RequestParameters: array[3] of Variant;
    begin
        if AgreementId = 0 then
            exit;

        RequestParameters[1] := AgreementId;
        AgreementJsonObject.ReadFrom(ManageAPICalls.GetResponse('GET', 11, RequestParameters, Client));
        if AgreementJsonObject.SelectToken('customFields', CustomFieldJsonToken) then
            if CustomFieldJsonToken.AsArray().Get(0, CustomFieldJsonToken) then
                case RecRef.RecordId.TableNo of
                    Database::"PTE Manage Product":
                        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageProduct.FieldNo("Agreement Number"), CustomFieldJsonToken.AsObject(), 'value');
                    Database::"PTE Manage Time Entry":
                        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageTimeEntry.FieldNo("Agreement Number"), CustomFieldJsonToken.AsObject(), 'value');
                    Database::"PTE Manage Invoice Expense":
                        PTEJsonManagement.GetValueAndSetToRecFieldNo(RecRef, ManageInvoiceExpenses.FieldNo("Agreement Number"), CustomFieldJsonToken.AsObject(), 'value');
                end;
    end;

    local procedure GetProductDiscount(var Recref: RecordRef; ItemIdentifer: Code[20])
    var
        ManageProduct: Record "PTE Manage Product";
        PTEJsonManagement: Codeunit "PTE Json Management";
        ManageAPICalls: Codeunit "PTE Manage API Calls";
        ProductDiscountJsonArray: JsonArray;
        CustomFieldsJsonToken: JsonToken;
        ProductDiscountJsonToken: JsonToken;
        RequestParameters: array[3] of Variant;
        AgreementId: Integer;
        Description: Text[100];
    begin
        AgreementId := Recref.Field(ManageProduct.FieldNo("Agreement Id")).Value;
        Description := Recref.Field(ManageProduct.FieldNo(Description)).Value;
        if (AgreementId = 0) or (Description = '') or (ItemIdentifer = '') then
            exit;

        RequestParameters[1] := AgreementId;
        RequestParameters[2] := ItemIdentifer;
        RequestParameters[3] := Description;
        if ProductDiscountJsonArray.ReadFrom(ManageAPICalls.GetResponse('GET', 10, RequestParameters, Client)) then
            if ProductDiscountJsonArray.Get(0, ProductDiscountJsonToken) then
                if ProductDiscountJsonToken.AsObject().SelectToken('customFields', CustomFieldsJsonToken) then
                    if CustomFieldsJsonToken.AsArray().Get(0, CustomFieldsJsonToken) then
                        PTEJsonManagement.GetValueAndSetToRecFieldNo(Recref, ManageProduct.FieldNo("Discount %"), CustomFieldsJsonToken.AsObject(), 'value');
    end;

    procedure CloseBatch()
    var
        ManageInvoiceHeader: Record "PTE Manage Invoice Header";
        ManageApiCalls: Codeunit "PTE Manage API Calls";
        RequestParameters: array[3] of Variant;
    begin
        ManageInvoiceHeader.SetRange(Closed, false);
        if ManageInvoiceHeader.FindSet() then
            repeat
                RequestParameters[1] := GetGLEntryArray(ManageInvoiceHeader."GLEntry Ids");
                ManageApiCalls.GetResponse('POST', 13, RequestParameters, Client);
                ManageInvoiceHeader.Closed := true;
                ManageInvoiceHeader.Modify();
            until ManageInvoiceHeader.Next() = 0;
    end;

    local procedure CheckIfInvoiceExists(InvoiceObject: JsonObject): Boolean
    var
        InvoiceHeader: Record "PTE Manage Invoice Header";
        BillingLogId: JsonToken;
    begin
        if InvoiceObject.SelectToken('billingLogId', BillingLogId) then
            exit(InvoiceHeader.Get(BillingLogId.AsValue().AsInteger()));
    end;

    local procedure GetGLEntryArray(GLEntryIds: Text) GLEntryIdsArray: JsonArray
    var
        GLIdInteger: Integer;
        GLEntryId: Text;
    begin
        foreach GLEntryId in GLEntryIds.Split(',') do begin
            Evaluate(GLIdInteger, GLEntryId);
            GLEntryIdsArray.Add(GLIdInteger);
        end;
    end;

    local procedure GetObjectFromParentObject(ParentObject: JsonObject; Path: Text) SelectedObject: JsonObject
    var
        JsonToken: JsonToken;
    begin
        if ParentObject.SelectToken(Path, JsonToken) then
            SelectedObject := JsonToken.AsObject();
    end;

    local procedure GetManageinvoiceLineNo(InvoiceNumber: Code[10]) LineNo: Integer
    var
        ManageInvoiceLine: Record "PTE Manage Invoice Line";
    begin
        ManageInvoiceLine.SetRange("Invoice Number", InvoiceNumber);
        if ManageInvoiceLine.FindLast() then
            LineNo := ManageInvoiceLine."Line No." + 1
        else
            LineNo := 10000;
    end;

    local procedure GetExpenceLineNo(InvoiceId: Integer) LineNo: Integer
    var
        ManageInvoiceExpense: Record "PTE Manage Invoice Expense";
    begin
        ManageInvoiceExpense.SetRange("Invoice ID", InvoiceId);
        if ManageInvoiceExpense.FindLast() then
            LineNo := ManageInvoiceExpense."Line No." + 1
        else
            LineNo := 10000;
    end;

    local procedure ConvertInvoiceTypeTextToEnum(InvoiceObject: JsonObject; var RecRef: RecordRef)
    var
        InvoieHeader: Record "PTE Manage Invoice Header";
        InvoiceTypeFieldRef: FieldRef;
        InvoicType: Enum "PTE Manage Invoice Type";
        InvoiceTypeToken: JsonToken;
    begin
        InvoiceObject.SelectToken('invoiceType', InvoiceTypeToken);
        InvoiceTypeFieldRef := RecRef.Field(InvoieHeader.FieldNo("Invoice Type"));
        InvoiceTypeFieldRef.Value := Enum::"PTE Manage Invoice Type".FromInteger(InvoicType.Ordinals.Get(InvoicType.Names.IndexOf(InvoiceTypeToken.AsValue().AsText())));
    end;

    local procedure ConvertVATToProcent(var RecRef: RecordRef)
    var
        ManageInvoiceHeader: Record "PTE Manage Invoice Header";
        FieldRef: FieldRef;
        VATDecimal: Decimal;
    begin
        FieldRef := RecRef.Field(ManageInvoiceHeader.FieldNo("VAT %"));
        VATDecimal := FieldRef.Value;
        FieldRef.Value := VATDecimal * 100;
    end;

    local procedure InitializeClient()
    var
        ManageAPISetup: Record "PTE Manage API Setup";
        Base64Convert: Codeunit "Base64 Convert";
    begin
        if IsInitialized then
            exit;

        ManageAPISetup.Get();
        Client.DefaultRequestHeaders.Add('Authorization', 'Basic ' + Base64Convert.ToBase64(ManageAPISetup."User Name" + ':' + ManageAPISetup.Password));
        Client.DefaultRequestHeaders.Add('clientid', ManageAPISetup."Client Id");
        IsInitialized := true;
    end;

    procedure GetAgreementNumberForInvoices()
    var
        ManageInvoiceHeader: Record "PTE Manage Invoice Header";
    begin
        ManageInvoiceHeader.SetRange("Agreement Number", '');
        if ManageInvoiceHeader.FindSet() then
            repeat
                ManageInvoiceHeader."Agreement Number" := FindHeaderAgreementNumber(ManageInvoiceHeader."Invoice Number", ManageInvoiceHeader."Invoice Type");
                ManageInvoiceHeader.Modify();
            until ManageInvoiceHeader.Next() = 0;
    end;

    local procedure FindHeaderAgreementNumber(InvoiceNumber: Code[10]; InvoiceType: Enum "PTE Manage Invoice Type"): Text[10]
    var
        ManageTimeEntry: Record "PTE Manage Time Entry";
        ManageProduct: Record "PTE Manage Product";
        ManageInvoiceExpenses: Record "PTE Manage Invoice Expense";
    begin
        if InvoiceType = InvoiceType::Standard then begin
            ManageTimeEntry.SetRange("Invoice Number", InvoiceNumber);
            ManageTimeEntry.SetFilter("Agreement Number", '<>%1', '');
            ManageTimeEntry.SetLoadFields("Agreement Number");
            if ManageTimeEntry.FindLast() then
                exit(ManageTimeEntry."Agreement Number");

            ManageInvoiceExpenses.SetRange("Invoice Number", InvoiceNumber);
            ManageInvoiceExpenses.SetFilter("Agreement Number", '<>%1', '');
            ManageInvoiceExpenses.SetLoadFields("Agreement Number");
            if ManageInvoiceExpenses.FindLast() then
                exit(ManageInvoiceExpenses."Agreement Number");
        end else begin
            ManageProduct.SetRange("Invoice Number", InvoiceNumber);
            ManageProduct.SetFilter("Agreement Number", '<>%1', '');
            ManageProduct.SetLoadFields("Agreement Number");
            if ManageProduct.FindLast() then
                exit(ManageProduct."Agreement Number");
        end;
    end;

    local procedure CheckAgreementNumber(RecRef: RecordRef; AgreementNumber: Code[10]; ParentAgreementId: Integer)
    begin
        if (AgreementNumber = '') and (ParentAgreementId > 0) then
            GetAgreementNumber(RecRef, ParentAgreementId);
    end;

    local procedure CheckIfAgreementIsTimapottur(AgreementType: Text[50]): Boolean
    begin
        if (AgreementType.Trim() = 'Tímapottur') then
            exit(true);
    end;
}