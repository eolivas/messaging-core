namespace Demo.Application.Sessions;

/// <summary>Library configuration of the active host, as applied (never defaults).</summary>
public sealed record IntegrationInfo(
    string MessagingMode,
    string ProviderName,
    bool IdempotencyEnabled,
    int RetryMaxAttempts,
    string BackoffStrategy);
