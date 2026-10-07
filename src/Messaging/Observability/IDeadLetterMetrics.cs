namespace Messaging.Observability;

/// <summary>
/// The Application-layer port for recording the dead-letter metric (Req 8.4). Dead-letter routing
/// emits exactly one metric per dead-lettered message through this seam; the concrete
/// OpenTelemetry-backed counter (<c>messaging.deadletter.count</c>) is wired in the observability
/// task and injected here, so the routing logic never couples to a concrete metrics provider (DIP).
/// </summary>
/// <remarks>
/// <para>
/// This is a small, consumer-driven interface (ISP): it exposes only the single operation the
/// dead-letter path needs. The full counter surface for published/consumed/retry/dead-letter counts
/// (Req 9.5) is introduced with the observability work; this port is the minimal slice that lets
/// task 13.1 emit its one dead-letter metric without waiting on that work, and the later
/// implementation can satisfy this port alongside the others.
/// </para>
/// <para>
/// Implementations MUST be best-effort: a failure to record the metric must not fail the dead-letter
/// routing or the message operation (Req 9.8). The router treats this as fire-and-forget around the
/// routing decision.
/// </para>
/// </remarks>
public interface IDeadLetterMetrics
{
    /// <summary>
    /// Records exactly one dead-letter event on the monotonic dead-letter counter (Req 8.4, 9.5).
    /// </summary>
    void RecordDeadLetter();
}
