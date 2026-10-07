using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.Observability;

namespace Messaging.Resilience;

/// <summary>
/// The Application-layer orchestration that routes a message to the dead-letter sink and owns the
/// acknowledge / retain decision, the single dead-letter metric, and the single structured log entry
/// (Req 8.1, 8.2, 8.4, 8.6). Both dead-letter triggers funnel through here so the routing contract is
/// defined once:
/// </summary>
/// <remarks>
/// <list type="bullet">
///   <item><description><b>Retry exhaustion / non-transient</b> (Req 6.5, 6.6, 8.1): the
///   <see cref="RetryExecutor"/> delegates to this router once a message reaches a terminal failure.</description></item>
///   <item><description><b>Poison / unbindable payload</b> (Req 8.3): an unbindable payload is routed
///   here directly with zero retry attempts (attempt count 1), via <see cref="RouteForContract"/>.</description></item>
/// </list>
/// <para>
/// For every dead-letter it builds a <see cref="DeadLetterMessage"/> that preserves the complete
/// original envelope byte-for-byte together with the correlation id, idempotency key, failure reason,
/// total attempt count, and the UTC dead-letter timestamp (Req 8.2, 6.6). The envelope is carried by
/// reference into the immutable record, so the preserved payload is the exact original bytes.
/// </para>
/// <para>
/// Routing outcome and pipeline effect:
/// </para>
/// <list type="number">
///   <item><description><b>Sink accepts</b> → emit exactly one metric and one structured log entry
///   (correlation id, failure reason, attempt count — Req 8.4), then return
///   <see cref="DeadLetterRoutingResult.RoutedAndAcknowledged"/> so the pipeline acknowledges and
///   removes the message from active processing (Req 8.1).</description></item>
///   <item><description><b>Sink fails</b> → log the routing failure, do <em>not</em> acknowledge, and
///   return <see cref="DeadLetterRoutingResult.RetainedUnacknowledged"/> so the message stays in the
///   pipeline for a later attempt (Req 8.6). The dead-letter metric is <em>not</em> emitted, because
///   no dead-letter actually completed.</description></item>
/// </list>
/// <para>
/// Metric and log emission are best-effort: an observability failure never changes the routing
/// outcome or fails the message operation (Req 9.8). SRP: this type owns only the routing contract;
/// the sink destination is <see cref="IDeadLetterSink"/>, the metric is <see cref="IDeadLetterMetrics"/>,
/// and the structured entries are <see cref="IDeadLetterLog"/> — the router composes them.
/// </para>
/// </remarks>
public sealed class DeadLetterRouter
{
    private readonly IDeadLetterSink _deadLetterSink;
    private readonly IDeadLetterMetrics _metrics;
    private readonly IDeadLetterLog _log;

    /// <summary>
    /// Creates the dead-letter router.
    /// </summary>
    /// <param name="deadLetterSink">The destination for dead-lettered messages (Req 8.1).</param>
    /// <param name="metrics">The dead-letter metric counter (Req 8.4).</param>
    /// <param name="log">The structured log for dead-letter and routing-failure entries (Req 8.4, 8.6).</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public DeadLetterRouter(
        IDeadLetterSink deadLetterSink,
        IDeadLetterMetrics metrics,
        IDeadLetterLog log)
    {
        _deadLetterSink = deadLetterSink ?? throw new ArgumentNullException(nameof(deadLetterSink));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _log = log ?? throw new ArgumentNullException(nameof(log));
    }

    /// <summary>
    /// Routes a poison / unbindable payload to the dead-letter sink without any retry (Req 8.3). The
    /// attempt count is 1: the single bind attempt that failed, with no reprocessing.
    /// </summary>
    /// <param name="envelope">The original envelope, preserved byte-for-byte (Req 8.2).</param>
    /// <param name="failureReason">The reason the payload could not be bound to a known contract.</param>
    /// <param name="idempotencyKey">The idempotency key, when one was derived; otherwise null (Req 6.6).</param>
    /// <param name="cancellationToken">Token to cancel the sink send.</param>
    /// <returns>Whether the message was routed (acknowledge) or retained (do not acknowledge).</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> or <paramref name="failureReason"/> is null.</exception>
    public Task<DeadLetterRoutingResult> RouteForContract(
        MessageEnvelope envelope,
        string failureReason,
        string? idempotencyKey,
        CancellationToken cancellationToken)
        => RouteAsync(envelope, failureReason, idempotencyKey, attemptCount: 1, cancellationToken);

    /// <summary>
    /// Routes a message to the dead-letter sink, builds the <see cref="DeadLetterMessage"/> preserving
    /// the original envelope and context, acknowledges on success, and retains + logs without
    /// acknowledging on sink failure (Req 8.1, 8.2, 8.4, 8.6).
    /// </summary>
    /// <param name="envelope">The original envelope, preserved byte-for-byte (Req 8.2).</param>
    /// <param name="failureReason">The reason the message could not be processed (Req 8.2).</param>
    /// <param name="idempotencyKey">The idempotency key, when one was derived; otherwise null (Req 6.6).</param>
    /// <param name="attemptCount">The total number of processing attempts made before dead-lettering (Req 8.2).</param>
    /// <param name="cancellationToken">Token to cancel the sink send.</param>
    /// <returns>Whether the message was routed (acknowledge) or retained (do not acknowledge).</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> or <paramref name="failureReason"/> is null.</exception>
    public async Task<DeadLetterRoutingResult> RouteAsync(
        MessageEnvelope envelope,
        string failureReason,
        string? idempotencyKey,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(failureReason);

        // Preserve the complete original envelope byte-for-byte plus the full dead-letter context
        // (correlation id, idempotency key, failure reason, attempt count, UTC timestamp) — Req 8.2, 6.6.
        var deadLetter = new DeadLetterMessage(
            Envelope: envelope,
            CorrelationId: envelope.CorrelationId,
            IdempotencyKey: idempotencyKey,
            FailureReason: failureReason,
            AttemptCount: attemptCount,
            DeadLetteredAtUtc: DateTimeOffset.UtcNow);

        try
        {
            await _deadLetterSink.SendAsync(deadLetter, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception routingError) when (routingError is not OperationCanceledException)
        {
            // Sink failed: keep the message in the active pipeline, log the routing failure, and do
            // NOT acknowledge the source (Req 8.6). The dead-letter metric is deliberately not emitted
            // here — no dead-letter actually completed.
            SafeLogRoutingFailed(envelope.CorrelationId, failureReason, attemptCount, routingError);
            return DeadLetterRoutingResult.RetainedUnacknowledged;
        }

        // Sink accepted: emit exactly one metric and one structured log entry (Req 8.4), then signal
        // the pipeline to acknowledge and remove the message from active processing (Req 8.1).
        SafeRecordMetric();
        SafeLogDeadLettered(envelope.CorrelationId, failureReason, attemptCount);

        return DeadLetterRoutingResult.RoutedAndAcknowledged;
    }

    // Observability is best-effort: a metric or log failure must never change the routing outcome or
    // fail the message operation (Req 9.8). Each emit is isolated so one failing does not suppress the
    // other.

    private void SafeRecordMetric()
    {
        try
        {
            _metrics.RecordDeadLetter();
        }
        catch
        {
            // Swallow: metrics are best-effort around the message path (Req 9.8).
        }
    }

    private void SafeLogDeadLettered(string correlationId, string failureReason, int attemptCount)
    {
        try
        {
            _log.DeadLettered(correlationId, failureReason, attemptCount);
        }
        catch
        {
            // Swallow: logging is best-effort around the message path (Req 9.8).
        }
    }

    private void SafeLogRoutingFailed(
        string correlationId,
        string failureReason,
        int attemptCount,
        Exception routingError)
    {
        try
        {
            _log.DeadLetterRoutingFailed(correlationId, failureReason, attemptCount, routingError);
        }
        catch
        {
            // Swallow: logging is best-effort around the message path (Req 9.8).
        }
    }
}
