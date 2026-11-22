namespace Demo.Application.Sessions;

public sealed record IntegrationDetailsDto(
    string MessagingMode,
    string ProviderName,
    bool IdempotencyEnabled,
    int RetryMaxAttempts,
    string BackoffStrategy);
