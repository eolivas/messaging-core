using Messaging.Abstractions;

namespace Messaging.Configuration;

/// <summary>
/// The validated set of configuration values that selects the <see cref="MessagingMode"/> and
/// supplies mode-independent settings (Req 1, 2, 6, 7, 10). This lives in the Application layer
/// because it is an application concern, not a Domain concept.
/// </summary>
/// <remarks>
/// Each nested section carries its own documented defaults sourced from
/// <see cref="ConfigurationBounds"/>, which is also the single source of truth the
/// <see cref="IConfigurationValidator"/> consumes for range checks (task 5). Provider-specific
/// settings hang off the open <see cref="ProviderSettings"/> extension bag and are validated
/// against each provider's <c>ProviderSettingDescriptor</c> set rather than against fixed fields,
/// so a new mode adds settings without changing this model (Req 2.5, 3.6).
/// </remarks>
public sealed record MessagingConfiguration
{
    /// <summary>The configured messaging mode; null fails startup validation fast (Req 1.2).</summary>
    public required MessagingMode? Mode { get; init; }

    /// <summary>Uniform retry/backoff policy mapped onto the active transport (Req 6).</summary>
    public RetryPolicyConfig Retry { get; init; } = new();

    /// <summary>Transactional outbox settings for reliable publishing (Req 5).</summary>
    public OutboxConfig Outbox { get; init; } = new();

    /// <summary>Idempotent-consumption settings (Req 7).</summary>
    public IdempotencyConfig Idempotency { get; init; } = new();

    /// <summary>Per-mode dead-letter settings (Req 8.5).</summary>
    public DeadLetterConfig DeadLetter { get; init; } = new();

    /// <summary>Logging, metrics, and tracing settings (Req 9).</summary>
    public ObservabilityConfig Observability { get; init; } = new();

    /// <summary>Host lifecycle / graceful-shutdown settings (Req 10).</summary>
    public LifecycleConfig Lifecycle { get; init; } = new();

    /// <summary>
    /// Open extension bag of provider-specific settings validated against the active provider's
    /// declared <c>ProviderSettingDescriptor</c> set (Req 2.5, 3.6).
    /// </summary>
    public IReadOnlyDictionary<string, string> ProviderSettings { get; init; }
        = new Dictionary<string, string>();
}
