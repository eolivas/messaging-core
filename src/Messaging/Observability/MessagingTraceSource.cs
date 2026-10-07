using System.Diagnostics;

namespace Messaging.Observability;

/// <summary>
/// Owns the distributed-tracing spans that link a publish operation to its matching consume operation
/// through the correlation id carried in message headers (Req 9.6). It wraps a single
/// <see cref="ActivitySource"/> (the standard .NET tracing primitive any OpenTelemetry exporter can
/// listen to) so the core emits spans without depending on a specific tracing vendor.
/// </summary>
/// <remarks>
/// <para>
/// <b>Linking across the publish/consume boundary.</b> Publish and consume run in different processes,
/// so the two spans cannot share an in-memory parent. Instead the publish span's trace context is
/// serialized into the envelope headers (<see cref="TraceParentHeader"/> in W3C <c>traceparent</c>
/// form, plus <see cref="CorrelationIdHeader"/>) by <see cref="StartPublishSpan"/>; on the other side
/// <see cref="StartConsumeSpan"/> reads those headers and starts the consume span as a child of the
/// propagated context. The correlation id travels unchanged in the same headers (Req 9.4), so even
/// where a transport drops trace headers the two spans remain correlatable by correlation id.
/// </para>
/// <para>
/// Both methods return the started <see cref="Activity"/> (or <see langword="null"/> when no listener
/// is sampling, which is the normal no-op case). The caller owns the returned activity's lifetime and
/// disposes it to close the span; the caller also writes the header entries from
/// <see cref="StartPublishSpan"/>'s out parameter into the outgoing envelope. Starting a span never
/// throws into the message path here — best-effort isolation (Req 9.8) is applied by
/// <see cref="ObservabilityContext"/>.
/// </para>
/// </remarks>
public sealed class MessagingTraceSource : IDisposable
{
    /// <summary>The <see cref="ActivitySource"/> name; also the name an exporter subscribes to.</summary>
    public const string ActivitySourceName = "Messaging.Core";

    /// <summary>Header key carrying the W3C <c>traceparent</c> that links publish and consume spans (Req 9.6).</summary>
    public const string TraceParentHeader = "traceparent";

    /// <summary>Header key carrying the correlation id propagated unchanged end-to-end (Req 9.4, 9.6).</summary>
    public const string CorrelationIdHeader = "x-correlation-id";

    private readonly ActivitySource _activitySource;
    private readonly bool _tracingEnabled;

    /// <summary>
    /// Creates the trace source.
    /// </summary>
    /// <param name="tracingEnabled">
    /// Whether spans are emitted; sourced from <see cref="Configuration.ObservabilityConfig.TracingEnabled"/>.
    /// When disabled, both start methods return <see langword="null"/> and inject no headers.
    /// </param>
    /// <param name="serviceName">
    /// The logical service name stamped as the activity-source version
    /// (<see cref="Configuration.ObservabilityConfig.ServiceName"/>).
    /// </param>
    public MessagingTraceSource(bool tracingEnabled = true, string? serviceName = null)
    {
        _tracingEnabled = tracingEnabled;
        _activitySource = new ActivitySource(ActivitySourceName, version: serviceName);
    }

    /// <summary>
    /// Starts the publish span and produces the header entries that carry its trace context and
    /// correlation id to the consumer (Req 9.6). The caller merges <paramref name="traceHeaders"/> into
    /// the outgoing envelope headers so the consume span can link back.
    /// </summary>
    /// <param name="correlationId">The envelope correlation id, stamped on the span and propagated (Req 9.2, 9.4).</param>
    /// <param name="traceHeaders">
    /// The header entries to add to the outgoing envelope: the W3C <c>traceparent</c> and the
    /// correlation id. Always populated with at least the correlation id, even when no span is sampled,
    /// so correlation survives regardless of tracing.
    /// </param>
    /// <returns>The started publish <see cref="Activity"/>, or <see langword="null"/> when tracing is disabled or unsampled.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="correlationId"/> is null.</exception>
    public Activity? StartPublishSpan(string correlationId, out IReadOnlyDictionary<string, string> traceHeaders)
    {
        ArgumentNullException.ThrowIfNull(correlationId);

        var headers = new Dictionary<string, string>(capacity: 2)
        {
            [CorrelationIdHeader] = correlationId,
        };

        Activity? activity = null;
        if (_tracingEnabled)
        {
            activity = _activitySource.StartActivity("messaging.publish", ActivityKind.Producer);
            if (activity is not null)
            {
                activity.SetTag("messaging.correlation_id", correlationId);
                // Serialize this span's context so the remote consume span becomes its child (Req 9.6).
                headers[TraceParentHeader] = activity.Id!;
            }
        }

        traceHeaders = headers;
        return activity;
    }

    /// <summary>
    /// Starts the consume span, linking it to the publish span by extracting the propagated trace
    /// context and correlation id from <paramref name="headers"/> (Req 9.6). When the trace context is
    /// present the consume span is created as a child of it; the correlation id is read from the same
    /// headers and stamped on the span so the two operations correlate even without trace headers.
    /// </summary>
    /// <param name="headers">The received envelope headers carrying the propagated context.</param>
    /// <returns>The started consume <see cref="Activity"/>, or <see langword="null"/> when tracing is disabled or unsampled.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="headers"/> is null.</exception>
    public Activity? StartConsumeSpan(IReadOnlyDictionary<string, string> headers)
    {
        ArgumentNullException.ThrowIfNull(headers);

        if (!_tracingEnabled)
        {
            return null;
        }

        headers.TryGetValue(CorrelationIdHeader, out var correlationId);

        Activity? activity;
        if (headers.TryGetValue(TraceParentHeader, out var traceParent) &&
            ActivityContext.TryParse(traceParent, traceState: null, out var parentContext))
        {
            // Link to the publish span's context so the logical operation is one traceable flow.
            activity = _activitySource.StartActivity("messaging.consume", ActivityKind.Consumer, parentContext);
        }
        else
        {
            activity = _activitySource.StartActivity("messaging.consume", ActivityKind.Consumer);
        }

        if (activity is not null && correlationId is not null)
        {
            activity.SetTag("messaging.correlation_id", correlationId);
        }

        return activity;
    }

    /// <summary>Releases the underlying <see cref="ActivitySource"/>.</summary>
    public void Dispose() => _activitySource.Dispose();
}
