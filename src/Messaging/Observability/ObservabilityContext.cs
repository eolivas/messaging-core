using System.Diagnostics;

namespace Messaging.Observability;

/// <summary>
/// The single Application-layer facade that applies the uniform observability policy around every
/// message operation (Req 9). It composes the structured log (<see cref="IMessagingLog"/>), the
/// monotonic counters (<see cref="IMessagingMetrics"/>), the trace spans
/// (<see cref="MessagingTraceSource"/>), and the redactor (<see cref="PayloadRedactor"/>) so the
/// publish/consume/retry/dead-letter paths wire in one dependency rather than four, and the
/// best-effort guarantee (Req 9.8) lives in exactly one place.
/// </summary>
/// <remarks>
/// <para>
/// <b>Best-effort by construction (Req 9.8).</b> Every emit — a log entry, a counter increment, or
/// starting a span — is wrapped so a failure is swallowed and never propagates into the publish or
/// consume operation. Each emit is isolated from the others, so one throwing sink does not suppress
/// the rest. This is the one component that owns the swallow policy; the individual sinks stay simple
/// and throw normally.
/// </para>
/// <para>
/// <b>What each hook emits.</b> Publish and consume emit a structured log entry (event type,
/// correlation id, UTC timestamp — Req 9.1, 9.2), bump their counter (Req 9.5), and open a trace span
/// linked across the boundary via the correlation id carried in headers (Req 9.4, 9.6). Retry and
/// dead-letter emit their log entry and counter (Req 9.1, 9.5); the dead-letter structured entry with
/// its failure reason and attempt count is owned by the dead-letter path's own <see cref="IDeadLetterLog"/>
/// (task 13.1), so this hook adds only the lifecycle event entry and the count, avoiding a double log.
/// </para>
/// <para>
/// <b>Redaction (Req 9.7).</b> The core never logs raw payload bytes, and the log port exposes no
/// payload field, so no secret can reach a log entry through this facade. <see cref="Redactor"/> is
/// exposed for callers (and adapters) that build structured key/value fields to redact them before
/// logging. The returned trace headers from <see cref="OnPublish"/> carry only the correlation id and
/// the W3C trace context, never payload data.
/// </para>
/// <para>
/// SRP: this type owns composition and the best-effort policy only. Formatting lives in the log sink,
/// counting in the metrics sink, span lifetime in the trace source, and redaction in the redactor.
/// </para>
/// </remarks>
public sealed class ObservabilityContext
{
    private readonly IMessagingLog _log;
    private readonly IMessagingMetrics _metrics;
    private readonly MessagingTraceSource _traceSource;

    /// <summary>
    /// Creates the observability context.
    /// </summary>
    /// <param name="log">The structured log sink (Req 9.1).</param>
    /// <param name="metrics">The monotonic counters (Req 9.5).</param>
    /// <param name="traceSource">The trace-span source linking publish and consume (Req 9.6).</param>
    /// <param name="redactor">The payload redactor exposed to callers that log structured fields (Req 9.7).</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public ObservabilityContext(
        IMessagingLog log,
        IMessagingMetrics metrics,
        MessagingTraceSource traceSource,
        PayloadRedactor redactor)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _traceSource = traceSource ?? throw new ArgumentNullException(nameof(traceSource));
        Redactor = redactor ?? throw new ArgumentNullException(nameof(redactor));
    }

    /// <summary>The redactor callers use to scrub sensitive values before logging structured fields (Req 9.7).</summary>
    public PayloadRedactor Redactor { get; }

    /// <summary>
    /// Observes a publish: emits the publish log entry and counter and starts the publish trace span,
    /// returning the trace/correlation headers to merge into the outgoing envelope so the consume span
    /// links back (Req 9.1, 9.5, 9.6). All emission is best-effort (Req 9.8).
    /// </summary>
    /// <param name="correlationId">The envelope correlation id (Req 9.2, 9.4).</param>
    /// <param name="traceHeaders">
    /// The headers (correlation id and, when sampled, the W3C <c>traceparent</c>) to add to the
    /// outgoing envelope. Always populated with at least the correlation id, even if a span or sink
    /// fails.
    /// </param>
    /// <returns>
    /// The started publish <see cref="Activity"/> for the caller to dispose when the publish completes,
    /// or <see langword="null"/> when tracing is disabled, unsampled, or the span could not start.
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="correlationId"/> is null.</exception>
    public Activity? OnPublish(string correlationId, out IReadOnlyDictionary<string, string> traceHeaders)
    {
        ArgumentNullException.ThrowIfNull(correlationId);

        SafeLog(MessagingEventType.Publish, correlationId);
        Safe(_metrics.RecordPublished);
        return SafeStartPublishSpan(correlationId, out traceHeaders);
    }

    /// <summary>
    /// Observes a consume: emits the consume log entry and counter and starts the consume trace span
    /// linked to the publish span via the headers (Req 9.1, 9.5, 9.6). All emission is best-effort
    /// (Req 9.8).
    /// </summary>
    /// <param name="correlationId">The envelope correlation id (Req 9.2, 9.4).</param>
    /// <param name="headers">The received envelope headers carrying the propagated trace context.</param>
    /// <returns>
    /// The started consume <see cref="Activity"/> for the caller to dispose when processing completes,
    /// or <see langword="null"/> when tracing is disabled, unsampled, or the span could not start.
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="correlationId"/> or <paramref name="headers"/> is null.</exception>
    public Activity? OnConsume(string correlationId, IReadOnlyDictionary<string, string> headers)
    {
        ArgumentNullException.ThrowIfNull(correlationId);
        ArgumentNullException.ThrowIfNull(headers);

        SafeLog(MessagingEventType.Consume, correlationId);
        Safe(_metrics.RecordConsumed);
        return SafeStartConsumeSpan(headers);
    }

    /// <summary>
    /// Observes a retry attempt: emits the retry log entry and bumps the retry counter (Req 9.1, 9.5).
    /// Best-effort (Req 9.8).
    /// </summary>
    /// <param name="correlationId">The envelope correlation id (Req 9.2).</param>
    /// <exception cref="ArgumentNullException">If <paramref name="correlationId"/> is null.</exception>
    public void OnRetry(string correlationId)
    {
        ArgumentNullException.ThrowIfNull(correlationId);

        SafeLog(MessagingEventType.Retry, correlationId);
        Safe(_metrics.RecordRetry);
    }

    /// <summary>
    /// Observes a dead-letter: emits the dead-letter lifecycle log entry and bumps the dead-letter
    /// counter (Req 9.1, 9.5). The dead-letter path's detailed entry (failure reason, attempt count)
    /// is emitted separately by <see cref="IDeadLetterLog"/>, so this adds only the lifecycle event and
    /// the count. Best-effort (Req 9.8).
    /// </summary>
    /// <param name="correlationId">The envelope correlation id (Req 9.2).</param>
    /// <exception cref="ArgumentNullException">If <paramref name="correlationId"/> is null.</exception>
    public void OnDeadLetter(string correlationId)
    {
        ArgumentNullException.ThrowIfNull(correlationId);

        SafeLog(MessagingEventType.DeadLetter, correlationId);
        Safe(_metrics.RecordDeadLetter);
    }

    // --- best-effort isolation (Req 9.8): a throwing sink never fails the message operation ---

    private void SafeLog(MessagingEventType eventType, string correlationId)
        => Safe(() => _log.MessageEvent(eventType, correlationId, DateTimeOffset.UtcNow));

    private Activity? SafeStartPublishSpan(string correlationId, out IReadOnlyDictionary<string, string> traceHeaders)
    {
        try
        {
            return _traceSource.StartPublishSpan(correlationId, out traceHeaders);
        }
        catch
        {
            // Tracing is best-effort (Req 9.8). Still hand back the correlation id so propagation and
            // correlation survive a tracing failure.
            traceHeaders = new Dictionary<string, string>(capacity: 1)
            {
                [MessagingTraceSource.CorrelationIdHeader] = correlationId,
            };
            return null;
        }
    }

    private Activity? SafeStartConsumeSpan(IReadOnlyDictionary<string, string> headers)
    {
        try
        {
            return _traceSource.StartConsumeSpan(headers);
        }
        catch
        {
            // Tracing is best-effort (Req 9.8).
            return null;
        }
    }

    private static void Safe(Action emit)
    {
        try
        {
            emit();
        }
        catch
        {
            // Swallow: logging/metrics are best-effort around the message path (Req 9.8).
        }
    }
}
