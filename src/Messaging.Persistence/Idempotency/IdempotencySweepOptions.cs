namespace Messaging.Persistence.Idempotency;

/// <summary>
/// Settings for the background retention sweep that prunes expired idempotency rows (Req 7.3).
/// The sweep removes rows whose <c>expires_at_utc</c> is in the past so the store does not grow
/// without bound once a key's retention window has elapsed.
/// </summary>
/// <remarks>
/// Defaults are chosen to be safe for a long (default 24h) retention window: sweeping every few
/// minutes keeps the table trimmed without adding meaningful load, since pruning is not
/// time-critical (an expired-but-not-yet-swept key simply lingers briefly and is still correctly
/// treated as expired by the retention-boundary query). Validation is applied by the sweep service
/// when it reads these options.
/// </remarks>
public sealed record IdempotencySweepOptions
{
    /// <summary>The default interval between sweep passes (5 minutes).</summary>
    public static readonly TimeSpan DefaultInterval = TimeSpan.FromMinutes(5);

    /// <summary>The smallest permitted sweep interval, guarding against a hot-loop misconfiguration.</summary>
    public static readonly TimeSpan MinimumInterval = TimeSpan.FromSeconds(1);

    /// <summary>How long to wait between sweep passes; default <see cref="DefaultInterval"/> (Req 7.3).</summary>
    public TimeSpan SweepInterval { get; init; } = DefaultInterval;
}
