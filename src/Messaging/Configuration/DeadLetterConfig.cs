namespace Messaging.Configuration;

/// <summary>
/// Dead-letter destination settings. The target is per-mode: each mode binds its own dead-letter
/// destination (Req 8.5). A null target defers to the active provider's native dead-letter target.
/// </summary>
public sealed record DeadLetterConfig
{
    /// <summary>Per-mode dead-letter target; null uses the provider's native default (Req 8.5).</summary>
    public string? Target { get; init; }
}
