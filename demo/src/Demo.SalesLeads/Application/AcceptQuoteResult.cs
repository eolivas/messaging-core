namespace Demo.SalesLeads.Application;

/// <summary>Outcome of accepting one quote.</summary>
public sealed record AcceptQuoteResult(
    bool Succeeded,
    string? CorrelationId,
    string? LeadId,
    string? Error);
