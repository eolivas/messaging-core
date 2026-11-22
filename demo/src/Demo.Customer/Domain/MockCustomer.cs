namespace Demo.Customer.Domain;

/// <summary>In-memory customer record created from an accepted quote.</summary>
public sealed record MockCustomer(
    string LeadId,
    string Name,
    string Email,
    DateTimeOffset CreatedAtUtc);
