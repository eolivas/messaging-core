namespace Messaging.Configuration;

/// <summary>
/// Duplicate-suppression settings for idempotent consumption (Req 7). Defaults and the retention
/// bound come from <see cref="ConfigurationBounds"/>.
/// </summary>
public sealed record IdempotencyConfig
{
    /// <summary>Whether idempotency checking is active; default enabled (Req 7.2).</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Retention window in seconds; 1..2_592_000 (30 days), default 86_400 (24h) (Req 7.3).</summary>
    public int RetentionSeconds { get; init; } = ConfigurationBounds.DefaultRetentionSeconds;
}
