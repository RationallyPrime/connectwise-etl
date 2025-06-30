codeunit 62001 "PTE Manage API Calls"
{
    var
        ManageAPISetup: Record "PTE Manage API Setup";

    procedure GetResponse(Method: Text; Step: Integer; RequestParameters: array[3] of Variant; Client: HttpClient) ResponseText: Text
    var
        PTEHTTP: Codeunit "PTE Http";
        RequestMessage: HttpRequestMessage;
        ResponseMessage: HttpResponseMessage;
    begin
        ManageAPISetup.Get();
        RequestMessage.SetRequestUri(GetWebServiceURL(Step, RequestParameters));
        RequestMessage.Method(Method);
        if (Step = 2) or (Step = 13) then
            AddContentForPostRequest(Step, RequestParameters, RequestMessage);

        Client.Send(RequestMessage, ResponseMessage);
        if ResponseMessage.IsSuccessStatusCode() then
            ResponseMessage.Content.ReadAs(ResponseText)
        else begin
            PTEHTTP.ProcessErrorMessage(ResponseMessage);
            Message(RequestMessage.GetRequestUri);
        end;
    end;

    local procedure GetWebServiceURL(Step: Integer; RequestParameters: array[3] of Variant) WebServiceURL: Text
    var
        TypeHelper: Codeunit "Type Helper";
        UrlStep1Lbl: Label 'finance/accounting/unpostedinvoices?pagesize=1000', Locked = true;
        UrlStep2Lbl: Label 'finance/accounting/export', Locked = true;
        UrlStep3Lbl: Label 'time/entries/', Locked = true;
        UrlStep4Lbl: Label 'time/workRoles/', Locked = true;
        UrlStep5Lbl: Label 'finance/agreements/%1/workroles?conditions=workrole/id=', Locked = true;
        UrlStep6Lbl: Label 'project/tickets/', Locked = true;
        UrlStep7Lbl: Label 'service/tickets/', Locked = true;
        UrlStep8_11Lbl: Label 'finance/agreements/', Locked = true;
        UrlStep9Lbl: Label 'procurement/products/', Locked = true;
        UrlStep10Lbl: Label 'finance/agreements/%1/additions?conditions=product/identifier = "%2" and invoiceDescription= "%3"', Locked = true;
        UrlStep12Lbl: Label 'expense/entries?conditions=invoice/id=', Locked = true;
        UrlStep13Lbl: Label 'finance/accounting/batches', Locked = true;
    begin
        case Step of
            1:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep1Lbl;
            2:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep2Lbl;
            3:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep3Lbl + Format(RequestParameters[1]);
            4:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep4Lbl + Format(RequestParameters[1]);
            5:
                WebServiceURL := ManageAPISetup."Web Service URL" + StrSubstNo(UrlStep5Lbl, RequestParameters[1]) + Format(RequestParameters[2]);
            6:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep6Lbl + Format(RequestParameters[1]);
            7:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep7Lbl + Format(RequestParameters[1]);
            8, 11:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep8_11Lbl + Format(RequestParameters[1]);
            9:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep9Lbl + Format(RequestParameters[1]);
            10:
                WebServiceURL := ManageAPISetup."Web Service URL" + StrSubstNo(UrlStep10Lbl, Format(RequestParameters[1]), TypeHelper.UrlEncode(RequestParameters[2]), TypeHelper.UrlEncode(RequestParameters[3]));
            12:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep12Lbl + Format(RequestParameters[1]);
            13:
                WebServiceURL := ManageAPISetup."Web Service URL" + UrlStep13Lbl;
        end;
    end;

    local procedure AddContentForPostRequest(Step: Integer; RequestParameters: array[3] of Variant; var RequestMessage: HttpRequestMessage)
    var
        ContentHeaders: HttpHeaders;
    begin
        if Step = 2 then
            RequestMessage.Content.WriteFrom(CreateExportRequestJsonObject(RequestParameters[1]))
        else
            RequestMessage.Content.WriteFrom(CreateCloseBatchRequestJsonObject(RequestParameters[1]));
        RequestMessage.Content.GetHeaders(ContentHeaders);
        ContentHeaders.Remove('Content-Type');
        ContentHeaders.Add('Content-Type', 'application/json');
    end;

    local procedure CreateExportRequestJsonObject(BillingLogId: Integer) RequestTxt: Text
    var
        InvoicenumberArray: JsonArray;
        RequestJsonObject: JsonObject;
    begin
        InvoicenumberArray.Add(BillingLogId);
        RequestJsonObject.Add('batchIdentifier', Format(Today, 8, '<Year4><Month,2><Day,2>') + '-' + Format(Time, 6, '<Hour,2><Minute,2><Second,2>'));
        RequestJsonObject.Add('locationId', 0);
        RequestJsonObject.Add('includedInvoiceIds', InvoiceNumberArray);
        RequestJsonObject.Add('thruDate', Format(CalcDate('<CM+1D>'), 10, '<Year4>-<Month,2>-<Day,2>') + 'T00:00:00Z');
        requestJsonObject.Add('summarizeInvoices', 'Detailed');
        RequestJsonObject.Add('exportInvoicesFlag', true);
        RequestJsonObject.WriteTo(RequestTxt);
    end;

    local procedure CreateCloseBatchRequestJsonObject(BatchId: JsonArray) RequestTxt: Text
    var
        RequestJsonObject: JsonObject;
    begin
        RequestJsonObject.Add('batchIdentifier', Format(Today, 8, '<Year4><Month,2><Day,2>') + '-' + Format(Time, 6, '<Hour,2><Minute,2><Second,2>'));
        RequestJsonObject.Add('exportInvoicesFlag', true);
        RequestJsonObject.Add('exportExpensesFlag', true);
        RequestJsonObject.Add('exportProductsFlag', false);
        RequestJsonObject.Add('summarizeExpenses', true);
        RequestJsonObject.Add('processedRecordIds', BatchId);
        RequestJsonObject.WriteTo(RequestTxt);
    end;


}
