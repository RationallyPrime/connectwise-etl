page 62003 "PTE Manage API Invoice List"
{
    ApplicationArea = All;
    Caption = 'Manage API Invoice List';
    CardPageId = "PTE Manage API Invoice Card";
    PageType = List;
    InsertAllowed = false;
    SourceTable = "PTE Manage Invoice Header";
    SourceTableView = sorting("Invoice Type");
    UsageCategory = Lists;

    layout
    {
        area(content)
        {
            repeater(General)
            {
                field("Invoice Number"; Rec."Invoice Number")
                {
                    ToolTip = '', Locked = true;
                    Style = Attention;
                    StyleExpr = Rec."Data Errors" > 0;
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
                field("Agreement Number"; Rec."Agreement Number")
                {
                    ToolTip = '', Locked = true;
                }
                field(Site; Rec.Site)
                {
                    ToolTip = '', Locked = true;
                }
                field(Project; Rec.Project)
                {
                    ToolTip = '', Locked = true;
                    Visible = false;
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
                field("Total Imported Amount"; -1 * Rec."Total Amount Imported")
                {
                    ToolTip = '', Locked = true;
                    Caption = 'Total Amount Imported';
                }
                field("Data Errors"; Rec."Data Errors")
                {
                    ToolTip = '', Locked = true;
                    Style = Attention;
                    StyleExpr = Rec."Data Errors" > 0;
                }
                field("Sales Invoice No."; Rec."Sales Invoice No.")
                {
                    ToolTip = '', Locked = true;
                }
                field("Job Journal Lines Amount"; Rec."Job Journal Lines Amount")
                {
                    ToolTip = '', Locked = true;
                }
                field("Posted Project Lines Amount"; Rec."Posted Project Lines Amount")
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
            action(FetchInvoices)
            {
                Caption = 'Fetch Invoices';
                Image = GetOrder;
                Promoted = true;
                PromotedCategory = Process;
                PromotedIsBig = true;
                ToolTip = 'Get Invoices from Manage API';
                trigger OnAction()
                var
                    GetManageInvoices: Codeunit "PTE Get Manage Invoices";
                begin
                    GetManageInvoices.GetInvoices();
                    GetManageInvoices.GetAgreementNumberForInvoices();
                end;
            }
            action(PostManageInvoices)
            {
                Caption = 'Post Manage Invoices';
                Image = PostDocument;
                Promoted = true;
                PromotedCategory = Process;
                PromotedIsBig = true;
                ToolTip = 'Create Sales Invoices or Job Journal Lines From Manage Invoices';
                trigger OnAction()
                var
                    ManageInvoiceHeader: Record "PTE Manage Invoice Header";
                    ManageError: Record "PTE Manage Invoice Error Logo";
                    CreateSalesInvoices: Codeunit "PTE Create Sales Invoices";
                    CreateJobJournalLines: Codeunit "PTE Create Job Journal";
                    ManageInvoiceErrorHandling: Codeunit "PTE Manage Invoice Error Hand";
                    Progress: Dialog;
                    ProgressMsg: Label 'Processing......#1######################\', Comment = '#1 Progress Message';
                begin
                    CurrPage.SetSelectionFilter(ManageInvoiceHeader);
                    if ManageInvoiceHeader.FindSet() then
                        Progress.Open(ProgressMsg);
                    repeat
                        ManageError.SetRange("Invoice Number", ManageInvoiceHeader."Invoice Number");
                        ManageError.DeleteAll();
                        if not ManageInvoiceErrorHandling.CheckIfManageInvoiceHeaderHasErrors(ManageInvoiceHeader) then
                            if ManageInvoiceHeader."Invoice Type" = ManageInvoiceHeader."Invoice Type"::Agreement then
                                CreateSalesInvoices.Run(ManageInvoiceHeader)
                            else
                                CreateJobJournalLines.Run(ManageInvoiceHeader);
                        Progress.Update(1, ManageInvoiceHeader."Invoice Number");
                        Sleep(50);
                        Commit();
                    until ManageInvoiceHeader.Next() = 0;
                    Progress.Close();
                end;
            }
            action(CloseBatch)
            {
                Caption = 'Close Invoices In Manage';
                Image = Close;
                Promoted = true;
                PromotedCategory = Process;
                PromotedIsBig = true;
                ToolTip = 'Close Invoices in Manage';
                trigger OnAction()
                var
                    GetManageInvoices: Codeunit "PTE Get Manage Invoices";
                begin
                    GetManageInvoices.CloseBatch();
                end;
            }
            action(GetAgreementNumbers)
            {
                Caption = 'Get Agreement Numbers';
                Image = GetSourceDoc;
                Promoted = true;
                PromotedCategory = Process;
                PromotedIsBig = true;
                trigger OnAction()
                var
                    GetManageInvoices: Codeunit "PTE Get Manage Invoices";
                begin
                    GetManageInvoices.GetAgreementNumberForInvoices();
                end;
            }
        }
    }
    trigger OnAfterGetRecord()
    begin
        Rec.CalcFields("Total Amount Imported");
    end;
}