namespace Messaging.Observability;

/// <summary>
/// The Application-layer port exposing the four monotonic message-lifecycle counters the core records
/// uniformly across modes: published, consumed, retry, and dead-letter counts (Req 9.5). Each
/// operation adds exactly one to a non-decreasing counter, so the exposed value is always the total
/// number of events of that type observed so far.
/// </summary>
/// <remarks>
/// <para>
/// This is the full counter surface referenced by <see cref="IDeadLetterMetrics"/>: the dead-letter
/// path was given a minimal single-method seam early (task 13.1), and the concrete implementation
/// introduced here satisfies both ports so there is one counter per event type with no double count
/// (DIP/ISP). <see cref="RecordDeadLetter"/> here and <see cref="IDeadLetterMetrics.RecordDeadLetter"/>
/// map to the same underlying dead-letter counter.
/// </para>
/// <para>
/// Implementations MUST be best-effort: a failure to record a metric must not fail the publish or
/// consume operation (Req 9.8). Emission is isolated by <see cref="ObservabilityContext"/>.
/// </para>
/// </remarks>
public interface IMessagingMetrics
{
    /// <summary>Records exactly one published-message event on the monotonic published counter (Req 9.5).</summary>
    void RecordPublished();

    /// <summary>Records exactly one consumed-message event on the monotonic consumed counter (Req 9.5).</summary>
    void RecordConsumed();

    /// <summary>Records exactly one retry event on the monotonic retry counter (Req 9.5).</summary>
    void RecordRetry();

    /// <summary>Records exactly one dead-letter event on the monotonic dead-letter counter (Req 9.5, 8.4).</summary>
    void RecordDeadLetter();
}
