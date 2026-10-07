namespace Messaging.Observability;

/// <summary>
/// The Application-layer port for the two structured log entries the dead-letter path emits (Req 8.4,
/// 8.6). It is defined here as a minimal, consumer-driven seam (ISP) so dead-letter routing can emit
/// structured entries without coupling to a concrete logger; the OpenTelemetry/structured-logging
/// implementation is wired in the observability task and injected here (DIP).
/// </summary>
/// <remarks>
/// <para>
/// Both operations carry their fields as typed parameters rather than a pre-formatted message string,
/// keeping the entries genuinely structured (correlation id, failure reason, attempt count are
/// discrete fields, not interpolated text). The failure reason passed here is the exception message
/// classified by the pipeline; callers must not pass raw payload bytes or secrets — those are
/// redacted on the payload path, which the dead-letter record never logs (code-conventions security,
/// Req 9.7).
/// </para>
/// <para>
/// Implementations MUST be best-effort: a logging failure must not fail dead-letter routing or the
/// message operation (Req 9.8).
/// </para>
/// </remarks>
public interface IDeadLetterLog
{
    /// <summary>
    /// Emits exactly one structured log entry recording that a message was dead-lettered, containing
    /// the correlation id, failure reason, and processing attempt count (Req 8.4).
    /// </summary>
    /// <param name="correlationId">The correlation id carried from the original envelope.</param>
    /// <param name="failureReason">The reason the message could not be processed.</param>
    /// <param name="attemptCount">The total number of processing attempts made before dead-lettering.</param>
    void DeadLettered(string correlationId, string failureReason, int attemptCount);

    /// <summary>
    /// Emits a structured log entry indicating that routing a message to the dead-letter sink failed,
    /// so the message is retained in the active pipeline and left unacknowledged (Req 8.6).
    /// </summary>
    /// <param name="correlationId">The correlation id carried from the original envelope.</param>
    /// <param name="failureReason">The original processing failure reason that triggered dead-lettering.</param>
    /// <param name="attemptCount">The total number of processing attempts made before dead-lettering.</param>
    /// <param name="routingError">The error raised while sending to the dead-letter sink.</param>
    void DeadLetterRoutingFailed(
        string correlationId,
        string failureReason,
        int attemptCount,
        Exception routingError);
}
