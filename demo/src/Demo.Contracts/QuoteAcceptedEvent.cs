namespace Demo.Contracts;

/// <summary>Message published by Sales Leads when a quote is accepted.</summary>
public sealed record QuoteAcceptedEvent(
    string LeadId,
    string CustomerName,
    string CustomerEmail,
    decimal QuoteAmount,
    DateTimeOffset AcceptedAtUtc)
{
    public const string MessageType = "quote-accepted";
}
