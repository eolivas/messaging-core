namespace Messaging.Configuration;

/// <summary>
/// The documented default values and permitted inclusive bounds for every numeric messaging
/// configuration setting (Req 6.1, 6.4, 7.3, 10.6). This is the single source of truth consumed
/// both by the configuration records (for their defaults) and by the
/// <see cref="IConfigurationValidator"/> (for range checks), so bounds are never duplicated as
/// inline literals that could drift apart.
/// </summary>
/// <remarks>
/// Each setting exposes a <see cref="Bounds"/> descriptor carrying its minimum, maximum, and the
/// human-readable permitted-range text the validator surfaces in a
/// <c>ConfigurationError.PermittedValuesOrRange</c>. The <c>Default*</c> constants are the values
/// the configuration records bind when a setting is absent.
/// </remarks>
public static class ConfigurationBounds
{
    /// <summary>Inclusive numeric bounds for a single setting plus its display text.</summary>
    /// <param name="Min">Smallest accepted value (inclusive).</param>
    /// <param name="Max">Largest accepted value (inclusive).</param>
    /// <param name="SettingName">The configuration setting the bound applies to.</param>
    public readonly record struct Bounds(int Min, int Max, string SettingName)
    {
        /// <summary>True when <paramref name="value"/> falls within [<see cref="Min"/>, <see cref="Max"/>].</summary>
        public bool Contains(int value) => value >= Min && value <= Max;

        /// <summary>Human-readable permitted range for a <c>ConfigurationError</c> (e.g. "1..100").</summary>
        public string PermittedRange => $"{Min}..{Max}";
    }

    /// <summary>Retry max-attempts bound: 1–100, default 5 (Req 6.1).</summary>
    public const int DefaultMaxAttempts = 5;
    public static readonly Bounds MaxAttempts = new(1, 100, $"{nameof(MessagingConfiguration.Retry)}.{nameof(RetryPolicyConfig.MaxAttempts)}");

    /// <summary>Retry per-attempt delay bound: 1–300s, default 5 (Req 6.4).</summary>
    public const int DefaultPerAttemptDelaySeconds = 5;
    public static readonly Bounds PerAttemptDelaySeconds = new(1, 300, $"{nameof(MessagingConfiguration.Retry)}.{nameof(RetryPolicyConfig.PerAttemptDelaySeconds)}");

    /// <summary>Outbox dispatch-attempt cap, default 10 (Req 5.5).</summary>
    public const int DefaultMaxDispatchAttempts = 10;
    public static readonly Bounds MaxDispatchAttempts = new(1, 100, $"{nameof(MessagingConfiguration.Outbox)}.{nameof(OutboxConfig.MaxDispatchAttempts)}");

    /// <summary>Idempotency retention bound: 1–2,592,000s (30 days), default 86,400s (24h) (Req 7.3).</summary>
    public const int DefaultRetentionSeconds = 86_400;
    public static readonly Bounds RetentionSeconds = new(1, 2_592_000, $"{nameof(MessagingConfiguration.Idempotency)}.{nameof(IdempotencyConfig.RetentionSeconds)}");

    /// <summary>Graceful-shutdown timeout bound: 1–300s, default 30 (Req 10.6).</summary>
    public const int DefaultShutdownTimeoutSeconds = 30;
    public static readonly Bounds ShutdownTimeoutSeconds = new(1, 300, $"{nameof(MessagingConfiguration.Lifecycle)}.{nameof(LifecycleConfig.ShutdownTimeoutSeconds)}");
}
