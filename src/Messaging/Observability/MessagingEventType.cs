namespace Messaging.Observability;

/// <summary>
/// The classification stamped on every structured observability record as the <c>event type</c>
/// field (Req 9.1). The four values are exactly the message-lifecycle events the core instruments
/// uniformly across modes — publish, consume, retry, and dead-letter — so operators can filter,
/// count, and correlate by a stable, discrete field rather than by parsing a free-text message.
/// </summary>
/// <remarks>
/// Keeping the event type a closed enum (rather than a raw string at the call site) means the
/// publish/consume/retry/dead-letter paths cannot drift into inconsistent spellings, and the metric
/// counters (<see cref="IMessagingMetrics"/>) line up one-to-one with the log event types.
/// </remarks>
public enum MessagingEventType
{
    /// <summary>A message was published through the active provider (Req 9.1).</summary>
    Publish,

    /// <summary>A message was delivered to and processed by a consumer (Req 9.1).</summary>
    Consume,

    /// <summary>A failed message was retried under the retry policy (Req 9.1).</summary>
    Retry,

    /// <summary>A message was routed to the dead-letter sink (Req 9.1).</summary>
    DeadLetter,
}
