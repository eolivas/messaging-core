namespace Demo.Application.Sessions;

/// <summary>Result of one Quote Accepted trigger as reported by the Sales Leads microservice.</summary>
public sealed record AcceptQuoteOutcome(
    bool Succeeded,
    string? CorrelationId,
    string? LeadId,
    string? Error);
