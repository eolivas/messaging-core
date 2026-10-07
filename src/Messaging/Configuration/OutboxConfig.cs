namespace Messaging.Configuration;

/// <summary>
/// Reliable-publish (transactional outbox) settings (Req 5). Defaults come from
/// <see cref="ConfigurationBounds"/>.
/// </summary>
public sealed record OutboxConfig
{
    /// <summary>Whether outbox-based reliable publishing is active; default enabled (Req 5.1).</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>Dispatch-attempt cap before an entry is marked Failed; default 10 (Req 5.5).</summary>
    public int MaxDispatchAttempts { get; init; } = ConfigurationBounds.DefaultMaxDispatchAttempts;
}
