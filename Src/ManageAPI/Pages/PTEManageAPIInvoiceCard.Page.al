page 62005 "PTE Manage API Invoice Card"
{
    ApplicationArea = All;
    Caption = 'Manage API Invoice';
    DataCaptionExpression = Rec."Invoice Number";
    Editable = false;
    PageType = Document;
    SourceTable = "PTE Manage Invoice Header";

    layout
    {
        area(content)
        {
            group(General)
            {
                Caption = 'General';

                field("Invoice Number"; Rec."Invoice Number")
                {
                    ToolTip = '', Locked = true;
                }
                field("Invoice Type"; Rec."Invoice Type")
                {
                    ToolTip = '', Locked = true;
                }
                field("Invoice Date"; DT2Date(Rec."Invoice Date"))
                {
                    Caption = 'Invoice Date';
                    ToolTip = '', Locked = true;
                }
                field("Social Security No."; Rec."Social Security No.")
                {
                    ToolTip = '', Locked = true;
                }
                field("Customer Name"; Rec."Customer Name")
                {
                    ToolTip = '', Locked = true;
                }
                field(Site; Rec.Site)
                {
                    ToolTip = '', Locked = true;
                }
                field("Agreement Number"; Rec."Agreement Number")
                {
                    ToolTip = '', Locked = true;
                }
                field(Project; Rec.Project)
                {
                    ToolTip = '', Locked = true;
                    Visible = false;
                }
                field("Total Imported Amount"; -1 * Rec."Total Amount Imported")
                {
                    ToolTip = '', Locked = true;
                    Caption = 'Total Amount Imported';
                }
                field("Total Amount"; Rec."Total Amount")
                {
                    ToolTip = '', Locked = true;
                }
                field("Total Amount without VAT"; Rec."Total Amount without VAT")
                {
                    ToolTip = '', Locked = true;
                }
                field("VAT %"; Rec."VAT %")
                {
                    ToolTip = '', Locked = true;
                }
            }
            part(InvoiceLines; "PTE Manage Invoice Lines")
            {
                SubPageLink = "Invoice Number" = field("Invoice Number");
            }
        }
    }
    actions
    {
        area(Processing)
        {
            action(ShowExpenses)
            {
                Caption = 'Invoice Expenses';
                Image = ProjectExpense;
                Promoted = true;
                PromotedCategory = Process;
                RunObject = page "PTE Manage Invoice Expenses";
                RunPageLink = "Invoice ID" = field("Invoice ID");
                ToolTip = 'Shows the expenses for the invoice.';
                Visible = StandardVisiblity;
            }
            action(CreateInvoice)
            {
                Caption = 'Create Sales Invoice';
                Image = CreateDocument;
                Promoted = true;
                PromotedCategory = Process;
                ToolTip = 'Create Sales Invoice for current Manage invoice';
                Visible = not StandardVisiblity;
                trigger OnAction()
                var
                    SalesHeader: Record "Sales Header";
                    CreateSalesInvoices: Codeunit "PTE Create Sales Invoices";
                begin
                    CreateSalesInvoices.Run(Rec);
                    if SalesHeader.get(SalesHeader."Document Type"::Invoice, Rec."Sales Invoice No.") then
                        Page.Run(Page::"Sales Invoice", SalesHeader);
                end;
            }
            action(OpenInvoice)
            {
                Caption = 'Open Sales Invoice';
                Image = SalesInvoice;
                Promoted = true;
                PromotedCategory = Process;
                ToolTip = 'Open Sales Invoice for current Manage invoice';
                Visible = not StandardVisiblity;
                trigger OnAction()
                var
                    SalesHeader: Record "Sales Header";
                begin
                    if SalesHeader.Get(SalesHeader."Document Type"::Invoice, Rec."Sales Invoice No.") then
                        Page.Run(Page::"Sales Invoice", SalesHeader);
                end;
            }
            action(CreateJobJournalLinesAction)
            {
                Caption = 'Create Job Journal Lines';
                Image = CreateLinesFromJob;
                Promoted = true;
                PromotedCategory = Process;
                ToolTip = 'Create Job Journal Lines for current Manage invoice';
                Visible = StandardVisiblity;
                trigger OnAction()
                var
                    JobJournalLine: Record "Job Journal Line";
                    CreateJobJournal: Codeunit "PTE Create Job Journal";
                begin
                    CreateJobJournal.Run(Rec);
                    JobJournalLine.SetRange("Document No.", Rec."Invoice Number");
                    if not JobJournalLine.IsEmpty() then
                        Page.Run(Page::"Job Journal", JobJournalLine);
                end;
            }
            action(OpenJobJournal)
            {
                Caption = 'Open Job Journal';
                Image = JobJournal;
                Promoted = true;
                PromotedCategory = Process;
                ToolTip = 'Open Job Journal for current Manage invoice';
                Visible = StandardVisiblity;
                trigger OnAction()
                var
                    JobJournalLine: Record "Job Journal Line";
                begin
                    JobJournalLine.SetRange("Document No.", Rec."Invoice Number");
                    if not JobJournalLine.IsEmpty() then
                        Page.Run(Page::"Job Journal", JobJournalLine);
                end;
            }
        }
    }
    trigger OnAfterGetRecord()
    begin
        if Rec."Invoice Type" = Rec."Invoice Type"::Standard then
            StandardVisiblity := true;
        rec.CalcFields("Total Amount Imported");
    end;

    var
        StandardVisiblity: Boolean;
}