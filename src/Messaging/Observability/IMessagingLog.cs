namespace Messaging.Observability;

/// <summary>
/// The Application-layer port for the structured log entry emitted on every message publish, consume,
/// retry, and dead-letter event (Req 9.1, 9.2). It is a small, consumer-driven seam (ISP/DIP),
/// mirroring the dead-letter-specific <see cref="IDeadLetterLog"/>: the observability pipeline calls
/// this and a concrete structured-logging adapter (wired in Infrastructure) satisfies it, so the core
/// never couples to a concrete logger.
/// </summary>
/// <remarks>
/// <para>
/// The entry carries its fields as typed parameters rather than a pre-formatted message string,
/// keeping it genuinely structured: the event type, correlation id, and UTC timestamp are discrete
/// fields the operator can filter on (Req 9.1), and the correlation id ties the entry to its trace
/// span and to the rest of the logical operation (Req 9.2, 9.4).
/// </para>
/// <para>
/// Callers must never pass raw payload bytes or secrets here. Sensitive payload values are replaced
/// with the fixed redaction marker on the payload path by <see cref="PayloadRedactor"/> before any
/// log call (Req 9.7, code-conventions security). This port deliberately exposes no payload parameter
/// so there is no seam through which an un-redacted payload could reach a log entry.
/// </para>
/// <para>
/// Implementations MUST be best-effort: a logging failure must not fail the publish or consume
/// operation (Req 9.8). The <see cref="ObservabilityContext"/> isolates each emit so a throwing logger
/// never changes the message outcome.
/// </para>
/// </remarks>
public interface IMessagingLog
{
    /// <summary>
    /// Emits one structured log entry for a message-lifecycle event, carrying the event type, the
    /// correlation id, and the UTC timestamp as discrete fields (Req 9.1, 9.2).
    /// </summary>
    /// <param name="eventType">The lifecycle event being recorded (publish/consume/retry/dead-letter).</param>
    /// <param name="correlationId">The correlation id carried unchanged from the envelope (Req 9.2, 9.4).</param>
    /// <param name="timestampUtc">The UTC timestamp of the event (Req 9.1).</param>
    void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc);
}
