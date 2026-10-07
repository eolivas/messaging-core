using System.Diagnostics.Metrics;

namespace Messaging.Observability;

/// <summary>
/// The concrete metrics adapter backing the four monotonic message-lifecycle counters
/// (published, consumed, retry, dead-letter) over the standard .NET metrics API
/// (<see cref="System.Diagnostics.Metrics"/>), which any OpenTelemetry or Prometheus exporter can
/// observe without the core taking a dependency on a specific telemetry vendor (Req 9.5).
/// </summary>
/// <remarks>
/// <para>
/// Each event maps to a <see cref="Counter{T}"/>, which is monotonic by construction: the API only
/// exposes <see cref="Counter{T}.Add(long)"/> and the core only ever adds <c>1</c>, so a counter can
/// never decrease and its value equals the number of recorded events of that type (Req 9.5).
/// </para>
/// <para>
/// This type satisfies both <see cref="IMessagingMetrics"/> and the earlier, minimal
/// <see cref="IDeadLetterMetrics"/> seam so the dead-letter path and the observability pipeline share
/// one dead-letter counter rather than double-counting (DIP/ISP). The counter names follow the design
/// (<c>messaging.published.count</c>, <c>messaging.consumed.count</c>, <c>messaging.retry.count</c>,
/// <c>messaging.deadletter.count</c>).
/// </para>
/// <para>
/// The underlying <see cref="Meter"/> is owned by this instance and released on <see cref="Dispose"/>.
/// Methods here do not swallow exceptions themselves; best-effort isolation (Req 9.8) is applied one
/// level up by <see cref="ObservabilityContext"/> so the swallow policy lives in exactly one place.
/// </para>
/// </remarks>
public sealed class MessagingMetrics : IMessagingMetrics, IDeadLetterMetrics, IDisposable
{
    /// <summary>The meter name used for the messaging-core counters; also the exporter subscription name.</summary>
    public const string MeterName = "Messaging.Core";

    private readonly Meter _meter;
    private readonly Counter<long> _published;
    private readonly Counter<long> _consumed;
    private readonly Counter<long> _retry;
    private readonly Counter<long> _deadLetter;

    /// <summary>
    /// Creates the metrics adapter, constructing the <see cref="Meter"/> and the four monotonic
    /// counters. The optional <paramref name="serviceName"/> (from
    /// <see cref="Configuration.ObservabilityConfig.ServiceName"/>) tags the meter version so counters
    /// from different logical services are distinguishable at the exporter.
    /// </summary>
    /// <param name="serviceName">The logical service name stamped on the meter; defaults to the meter name.</param>
    public MessagingMetrics(string? serviceName = null)
    {
        _meter = new Meter(MeterName, version: serviceName);
        _published = _meter.CreateCounter<long>("messaging.published.count", unit: "{message}", description: "Messages published.");
        _consumed = _meter.CreateCounter<long>("messaging.consumed.count", unit: "{message}", description: "Messages consumed.");
        _retry = _meter.CreateCounter<long>("messaging.retry.count", unit: "{attempt}", description: "Message processing retries.");
        _deadLetter = _meter.CreateCounter<long>("messaging.deadletter.count", unit: "{message}", description: "Messages dead-lettered.");
    }

    /// <inheritdoc />
    public void RecordPublished() => _published.Add(1);

    /// <inheritdoc />
    public void RecordConsumed() => _consumed.Add(1);

    /// <inheritdoc />
    public void RecordRetry() => _retry.Add(1);

    /// <inheritdoc cref="IMessagingMetrics.RecordDeadLetter" />
    public void RecordDeadLetter() => _deadLetter.Add(1);

    /// <summary>Releases the underlying <see cref="Meter"/> and its counters.</summary>
    public void Dispose() => _meter.Dispose();
}
