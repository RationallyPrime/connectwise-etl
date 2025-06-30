codeunit 62003 "PTE Create Job Journal"
{
    TableNo = "PTE Manage Invoice Header";
    trigger OnRun()
    begin
        ManageHeader.Copy(Rec);
        CreateJobJournalLinesForManageInvoice();
        Rec := ManageHeader;
    end;

    var
        ManageHeader: Record "PTE Manage Invoice Header";

    local procedure CreateJobJournalLinesForManageInvoice()
    var
        ManageAPISetup: Record "PTE Manage API Setup";
        ManageInvoiceLine: Record "PTE Manage Invoice Line";
        ManageInvoiceErrorHand: Codeunit "PTE Manage Invoice Error Hand";
        LineNo: Integer;
    begin
        ManageAPISetup.Get();
        ManageAPISetup.TestField("Job Journal Template Name");
        ManageAPISetup.TestField("Job Journal Batch Name");
        ManageAPISetup.TestField("Time Entry Reason Code");

        ManageInvoiceErrorHand.CheckIfAgreementNumberIsMissing(ManageHeader);

        ManageInvoiceLine.SetRange("Invoice Number", ManageHeader."Invoice Number");
        ManageInvoiceLine.SetFilter("Time Entry Id", '>%1', 0);
        ManageInvoiceLine.SetRange("Job Journal Line No.", 0);
        if ManageInvoiceLine.FindSet() then
            LineNo := GetLastLineNo(ManageAPISetup."Job Journal Template Name", ManageAPISetup."Job Journal Batch Name");
        repeat
            CreateJobJournalLine(ManageInvoiceLine, ManageAPISetup."Job Journal Template Name", ManageAPISetup."Job Journal Batch Name", LineNo, ManageAPISetup."Time Entry Reason Code");
            LineNo += 10000;
        until ManageInvoiceLine.Next() = 0;

        PostInvoiceExpenses(ManageHeader, ManageAPISetup."Job Journal Template Name", ManageAPISetup."Job Journal Batch Name");
    end;

    local procedure CreateJobJournalLine(var ManageInvoiceLine: Record "PTE Manage Invoice Line"; JobJournalTemplateName: Code[10]; JobJournalBatchName: Code[10]; LineNo: Integer; ReasonCode: Code[10])
    var
        JobJournalLine: Record "Job Journal Line";
        ManageTimeEntry: Record "PTE Manage Time Entry";
        ErrorExists: Boolean;
    begin
        if not ManageTimeEntry.Get(ManageInvoiceLine."Time Entry Id", ManageHeader."Invoice Number") then
            exit;

        JobJournalLine.Init();
        JobJournalLine.Validate("Journal Template Name", JobJournalTemplateName);
        JobJournalLine.Validate("Journal Batch Name", JobJournalBatchName);
        JobJournalLine.Validate("Line No.", LineNo);
        JobJournalLine.Validate("Posting Date", ManageInvoiceLine."Document Date");
        JobJournalLine.Validate("Document No.", ManageInvoiceLine."Invoice Number");
        JobJournalLine.Validate("Job No.", GetJobNo(ManageInvoiceLine."Invoice Number", ManageHeader."Agreement Number", ErrorExists));
        JobJournalLine.Validate("Job Task No.", GetJobTaskNo(JobJournalLine."Job No.", ManageInvoiceLine."Invoice Number", ErrorExists));
        JobJournalLine.Validate(Type, JobJournalLine.Type::Resource);
        JobJournalLine.Validate("No.", GetResourceNo(ManageInvoiceLine."Invoice Number", ManageTimeEntry.Employee, JobJournalLine."Job No.", ErrorExists));
        if not ErrorExists and (ManageTimeEntry."Ticket Id" > 0) then begin
            CreateJobRequest(Format(ManageTimeEntry."Ticket Id"), JobJournalLine."Job No.", JobJournalLine."Job Task No.", ManageTimeEntry."Ticket Summary");
            JobJournalLine.Validate("WJobJR Request No.", Format(ManageTimeEntry."Ticket Id"));
        end;
        JobJournalLine.Validate("Reason Code", ReasonCode);
        if ManageTimeEntry.Note <> '' then
            JobJournalLine.Validate("Description", CopyStr(ManageTimeEntry.Note, 1, MaxStrLen(JobJournalLine.Description)))
        else
            JobJournalLine.Validate("Description", CopyStr(ManageInvoiceLine.Memo, 1, MaxStrLen(JobJournalLine.Description)));
        JobJournalLine.Validate("Work Type Code", GetWorkTypeCode(ManageTimeEntry."Work Type", ManageInvoiceLine."Invoice Number", ErrorExists));
        JobJournalLine.Validate("Quantity", ManageInvoiceLine.Quantity * -1);
        if ManageInvoiceLine."Line Amount" = 0 then
            JobJournalLine."WJOB Qty to Invoice" := 0;
        if not ErrorExists then
            if JobJournalLine.Insert() then begin
                ManageInvoiceLine."Job Journal Line No." := JobJournalLine."Line No.";
                ManageInvoiceLine.Modify();
            end;
    end;

    local procedure GetLastLineNo(JobJournalTemplateName: Code[10]; JobJournalBatchName: Code[10]): Integer
    var
        JobJournalLine: Record "Job Journal Line";
    begin
        JobJournalLine.SetRange("Journal Template Name", JobJournalTemplateName);
        JobJournalLine.SetRange("Journal Batch Name", JobJournalBatchName);
        if JobJournalLine.FindLast() then
            exit(JobJournalLine."Line No." + 10000)
        else
            exit(10000);
    end;

    local procedure GetResourceNo(InvoiceNumber: Code[10]; ManageResoureId: Code[50]; JobNoLine: Code[20]; var ErrorExists: Boolean): Code[20]
    var
        Resource: Record "Resource";
        ManageInvoiceError: Codeunit "PTE Manage Invoice Error Hand";
        ErrorLbl: Label 'Manage Employee %1 not found in Resource table.', Comment = '%1 = Manage Resource Id';
    begin
        Resource.SetRange("PTE Manage Resource Id", ManageResoureId);
        if Resource.FindFirst() then begin
            if JobNoLine <> '' then
                exit(Resource."No.");
        end else begin
            ErrorExists := true;
            ManageInvoiceError.CreateManageError(InvoiceNumber, Database::Resource, ManageResoureId, StrSubstNo(ErrorLbl, ManageResoureId));
        end;
    end;

    local procedure GetJobNo(InvoiceNumber: Code[10]; ManageJobNo: Code[10]; var ErrorExists: Boolean): Code[20]
    var
        Job: Record "Job";
        ManageInvoiceError: Codeunit "PTE Manage Invoice Error Hand";
        ErrorLbl: Label 'Manage Job %1 not found in Job table.', Comment = '%1 = Manage Job Code';
    begin
        if Job.Get(ManageJobNo) then
            exit(Job."No.")
        else begin
            ErrorExists := true;
            ManageInvoiceError.CreateManageError(InvoiceNumber, Database::Job, ManageJobNo, StrSubstNo(ErrorLbl, ManageJobNo));
        end;
    end;

    local procedure GetJobTaskNo(JobNo: Code[20]; InvoiceNumber: Code[10]; var ErrorExists: Boolean): Code[20]
    var
        JobTask: Record "Job Task";
        ManageInvoiceError: Codeunit "PTE Manage Invoice Error Hand";
        ErrorLbl: Label 'Job Taks for Job %1 not found in Job Task table.', Comment = '%1 = Job No';
    begin
        JobTask.SetRange("Job No.", JobNo);
        if JobTask.FindFirst() then
            exit(JobTask."Job Task No.")
        else begin
            ErrorExists := true;
            ManageInvoiceError.CreateManageError(InvoiceNumber, Database::"Job Task", '', StrSubstNo(ErrorLbl, JobNo));
        end;
    end;

    local procedure GetWorkTypeCode(ManageWorkTypeName: Text[50]; InvoiceNumber: Code[10]; var ErrorExists: Boolean): Code[10]
    var
        WorkType: Record "Work Type";
        ManageInvoiceError: Codeunit "PTE Manage Invoice Error Hand";
        ErrorLbl: Label 'Manage Work Type %1 not found in Work Type table.', Comment = '%1 = Manage Work Type';
    begin
        WorkType.SetFilter("PTE Manage Work Type Name", '%1', '*' + ManageWorkTypeName + '*');
        if WorkType.FindFirst() then
            exit(WorkType."Code")
        else begin
            ErrorExists := true;
            ManageInvoiceError.CreateManageError(InvoiceNumber, Database::"Work Type", ManageWorkTypeName, StrSubstNo(ErrorLbl, ManageWorkTypeName));
        end;
    end;

    procedure GetAgreementNumber(ManageInvoiceNo: Code[10]): Code[10]
    var
        ManageTimeEntry: Record "PTE Manage Time Entry";
    begin
        ManageTimeEntry.SetRange("Invoice Number", ManageInvoiceNo);
        ManageTimeEntry.SetFilter("Agreement Number", '<>%1', '');
        if ManageTimeEntry.FindFirst() then
            exit(ManageTimeEntry."Agreement Number");
    end;

    local procedure PostInvoiceExpenses(ManageInvoice: Record "PTE Manage Invoice Header"; JobJournalTemplateName: Code[10]; JobJournalBatchName: Code[10])
    var
        ManageExpenses: Record "PTE Manage Invoice Expense";
        JobJournalLine: Record "Job Journal Line";
        LineNo: Integer;
        ErrorExists: Boolean;
    begin
        ManageExpenses.SetRange("Invoice ID", ManageInvoice."Invoice ID");
        ManageExpenses.SetRange("Job Journal Line No.", 0);
        if ManageExpenses.IsEmpty() then
            exit;

        if ManageExpenses.FindSet() then
            LineNo := GetLastLineNo(JobJournalTemplateName, JobJournalBatchName);
        repeat
            JobJournalLine.Init();
            JobJournalLine.Validate("Journal Template Name", JobJournalTemplateName);
            JobJournalLine.Validate("Journal Batch Name", JobJournalBatchName);
            JobJournalLine.Validate("Line No.", LineNo);
            JobJournalLine.Validate("Posting Date", DT2Date(ManageExpenses."Work Date"));
            JobJournalLine.Validate("Document No.", ManageInvoice."Invoice Number");
            JobJournalLine.Validate("Job No.", GetJobNo(ManageInvoice."Invoice Number", ManageInvoice."Agreement Number", ErrorExists));
            JobJournalLine.Validate("Job Task No.", GetJobTaskNo(JobJournalLine."Job No.", ManageInvoice."Invoice Number", ErrorExists));
            JobJournalLine.Validate(Type, JobJournalLine.Type::Resource);
            JobJournalLine.Validate("No.", GetResourceNo(ManageInvoice."Invoice Number", ManageExpenses.Type, JobJournalLine."Job No.", ErrorExists));
            JobJournalLine.Validate("Quantity", ManageExpenses.Quantity);
            if not ErrorExists then
                if JobJournalLine.Insert() then begin
                    ManageExpenses."Job Journal Line No." := JobJournalLine."Line No.";
                    ManageExpenses.Modify();
                    LineNo += 10000;
                end;
        until ManageExpenses.Next() = 0;
    end;

    local procedure CreateJobRequest(JobRequestNo: Code[20]; JobNo: Code[20]; JobTaskNo: Code[20]; JobRequestDescription: Text[100])
    var
        JobRequest: Record "WJOBJR Job Request";
        JobRequestLine: Record "WJOBJR Job Request Line";
    begin

        JobRequest.SetRange("No.", JobRequestNo);
        if JobRequest.IsEmpty() then begin
            JobRequest.Init();
            JobRequest."No." := CopyStr(JobRequestNo, 1, 20);
            JobRequest.Description := CopyStr(JobRequestDescription, 1, 120);
            JobRequest.Validate("Job No.", JobNo);
            JobRequest."Registered Date" := WorkDate();
            JobRequest.Status := JobRequest.Status::"In Process";
            JobRequest."Job Task No." := JobTaskNo;
            JobRequest.Insert();
        end;

        JobRequestLine.Reset();
        JobRequestLine.SetRange("Request No.", JobRequestNo);
        if JobRequestLine.IsEmpty() then begin
            JobRequestLine.Init();
            JobRequestLine.Validate("Request No.", JobRequestNo);
            JobRequestLine."Line No." := 10000;
            JobRequestLine."Job No." := JobNo;
            JobRequestLine."Job Task No." := JobTaskNo;
            JobRequestLine.Status := JobRequestLine.Status::"In process";
            JobRequestLine."Task Description" := CopyStr(JobRequestDescription, 1, 150);
            JobRequestLine.Insert(true);
        end;
    end;
}
