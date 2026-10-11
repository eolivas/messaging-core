namespace Messaging.Configuration;

/// <summary>
/// Uniform retry policy surface the core defines and maps onto the active transport's native
/// retry/redelivery mechanism (Req 6.1, 6.4). Bounds and defaults come from
/// <see cref="ConfigurationBounds"/> so the validator and the model share one source of truth.
/// </summary>
public sealed record RetryPolicyConfig
{
    /// <summary>Maximum delivery attempts before dead-lettering; 1..100, default 5 (Req 6.1).</summary>
    public int MaxAttempts { get; init; } = ConfigurationBounds.DefaultMaxAttempts;

    /// <summary>Backoff shape applied between attempts; default <see cref="BackoffStrategy.ExponentialWithJitter"/> (Req 6.1).</summary>
    public BackoffStrategy Backoff { get; init; } = BackoffStrategy.ExponentialWithJitter;

    /// <summary>Base per-attempt delay in seconds; 1..300, default 5 (Req 6.4).</summary>
    public int PerAttemptDelaySeconds { get; init; } = ConfigurationBounds.DefaultPerAttemptDelaySeconds;
}
