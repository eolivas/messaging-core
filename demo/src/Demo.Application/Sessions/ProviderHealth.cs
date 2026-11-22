namespace Demo.Application.Sessions;

/// <summary>Aggregated readiness of the active provider.</summary>
public sealed record ProviderHealth(bool Ready, string Status);
