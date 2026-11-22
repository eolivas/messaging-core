namespace Demo.SalesLeads.Domain;

/// <summary>A lead created when a quote is accepted. Not persisted; discarded on publish failure.</summary>
public sealed class SalesLead
{
    public SalesLead(string leadId, string customerName, string customerEmail, decimal quoteAmount)
    {
        LeadId = leadId;
        CustomerName = customerName;
        CustomerEmail = customerEmail;
        QuoteAmount = quoteAmount;
    }

    public string LeadId { get; }

    public string CustomerName { get; }

    public string CustomerEmail { get; }

    public decimal QuoteAmount { get; }

    public SalesLeadStatus Status { get; private set; } = SalesLeadStatus.Pending;

    public void MarkPublished() => Status = SalesLeadStatus.Published;
}
