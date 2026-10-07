namespace Messaging.Configuration;

/// <summary>
/// Observability settings for structured logging, metrics, and tracing (Req 9).
/// </summary>
public sealed record ObservabilityConfig
{
    /// <summary>Whether distributed tracing spans are emitted; default enabled (Req 9.6).</summary>
    public bool TracingEnabled { get; init; } = true;

    /// <summary>Logical service name stamped on logs, metrics, and spans; default "messaging-core" (Req 9.1).</summary>
    public string ServiceName { get; init; } = "messaging-core";
}
