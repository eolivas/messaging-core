namespace Demo.Communications;

/// <summary>An email recorded in memory. Nothing is ever sent over the network.</summary>
public sealed record MockEmail(string LeadId, string To, string Subject, string Body);
