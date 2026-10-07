namespace Messaging.Resilience;

/// <summary>
/// The terminal disposition of routing a message to the dead-letter sink (Req 8.1, 8.6). It tells the
/// active processing pipeline whether to acknowledge (remove) the source message or to retain it for
/// a later attempt.
/// </summary>
/// <remarks>
/// The distinction is the whole point of the dead-letter contract: a message leaves the pipeline
/// <em>only</em> once it is safely in the sink (Req 8.1). If the sink itself fails, the message must
/// stay in the pipeline and remain unacknowledged so it is not silently lost (Req 8.6). Modeling this
/// as an explicit result — rather than a bare success/throw — lets the transport-specific consume
/// loop make the ack decision without reaching into the routing internals.
/// </remarks>
public enum DeadLetterRoutingResult
{
    /// <summary>
    /// The message was accepted by the dead-letter sink. The pipeline MUST acknowledge the source so
    /// the message is removed from active processing (Req 8.1).
    /// </summary>
    RoutedAndAcknowledged,

    /// <summary>
    /// Routing to the dead-letter sink failed. The pipeline MUST retain the message and MUST NOT
    /// acknowledge the source; the routing failure has been logged (Req 8.6).
    /// </summary>
    RetainedUnacknowledged,
}
