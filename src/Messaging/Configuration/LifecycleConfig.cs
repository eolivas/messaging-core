namespace Messaging.Configuration;

/// <summary>
/// Host lifecycle settings governing graceful shutdown (Req 10). The timeout bound and default
/// come from <see cref="ConfigurationBounds"/>.
/// </summary>
public sealed record LifecycleConfig
{
    /// <summary>Graceful-drain timeout in seconds on shutdown; 1..300, default 30 (Req 10.6).</summary>
    public int ShutdownTimeoutSeconds { get; init; } = ConfigurationBounds.DefaultShutdownTimeoutSeconds;
}
