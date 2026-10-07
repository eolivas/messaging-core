using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Messaging.Observability;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Xunit;

namespace Messaging.Tests.Observability;

/// <summary>
/// Integration test for task 22.4 (Req 9.6): when a message is published and then consumed, the core
/// records a distributed trace span for each operation and the two spans are linked through the
/// Correlation_Id. This drives the real observability trace path (<see cref="ObservabilityContext"/>
/// over a real <see cref="MessagingTraceSource"/>) and listens to it with a genuine OpenTelemetry
/// <see cref="TracerProvider"/> wired to the in-memory exporter — the same subscription (by
/// <see cref="MessagingTraceSource.ActivitySourceName"/>) any production exporter would use — so the
/// assertions run against exported spans, not internal state.
/// </summary>
/// <remarks>
/// Linking in this design has two independent, mutually reinforcing legs, and the test asserts both:
/// <list type="number">
///   <item><description>
///   <b>Correlation_Id (Req 9.6).</b> The publish span stamps <c>messaging.correlation_id</c> and the
///   same id travels in the propagated headers to the consume span, which stamps it too. The spans
///   are therefore linked by an identical correlation-id tag even where a transport drops trace
///   headers.
///   </description></item>
///   <item><description>
///   <b>Trace context.</b> The publish span serializes its W3C <c>traceparent</c> into the headers and
///   the consume span is started as a child of that context, so the two exported spans share one
///   <c>TraceId</c> and the consume span's <c>ParentSpanId</c> is the publish span's <c>SpanId</c> —
///   one traceable flow across the publish/consume boundary.
///   </description></item>
/// </list>
/// A dedicated, unique <see cref="ActivitySource"/> version (service name) keeps this test's spans
/// isolated from any other activity source traffic, and <see cref="AlwaysOnSampler"/> guarantees the
/// spans are sampled and exported deterministically.
/// </remarks>
[Trait("Feature", "messaging-core")]
public class OpenTelemetrySpanLinkingIntegrationTests
{
    private const string CorrelationId = "corr-22-4-span-link";

    [Fact]
    public void PublishThenConsume_WithInMemoryExporter_ExportsSpansLinkedByCorrelationId()
    {
        // Arrange: an in-memory exporter subscribed to the core's activity source via a real
        // TracerProvider, exactly as a production OpenTelemetry pipeline subscribes to it.
        var exportedSpans = new List<Activity>();
        using var tracerProvider = Sdk.CreateTracerProviderBuilder()
            .AddSource(MessagingTraceSource.ActivitySourceName)
            .SetSampler(new AlwaysOnSampler())
            .AddInMemoryExporter(exportedSpans)
            .Build();

        using var traceSource = new MessagingTraceSource(tracingEnabled: true);
        var observability = new ObservabilityContext(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            traceSource,
            new PayloadRedactor());

        // Act: run the publish observability hook, carry the trace/correlation headers it produces to
        // the consume hook (the in-process stand-in for the transport propagating headers), and close
        // each span so the exporter receives it.
        using (var publishSpan = observability.OnPublish(CorrelationId, out var traceHeaders))
        {
            Assert.NotNull(publishSpan);
            using var consumeSpan = observability.OnConsume(CorrelationId, traceHeaders);
            Assert.NotNull(consumeSpan);
        }

        tracerProvider.ForceFlush();

        // Assert: exactly one publish span and one consume span were exported.
        var publishExported = Assert.Single(
            exportedSpans.Where(a => a.OperationName == "messaging.publish"));
        var consumeExported = Assert.Single(
            exportedSpans.Where(a => a.OperationName == "messaging.consume"));

        // Leg 1 — linked by Correlation_Id: both spans carry the same correlation-id tag (Req 9.6).
        Assert.Equal(CorrelationId, GetCorrelationIdTag(publishExported));
        Assert.Equal(CorrelationId, GetCorrelationIdTag(consumeExported));

        // Leg 2 — linked by trace context: one shared trace, consume is the child of publish (Req 9.6).
        Assert.Equal(publishExported.TraceId, consumeExported.TraceId);
        Assert.Equal(publishExported.SpanId, consumeExported.ParentSpanId);

        // Span kinds reflect the producer/consumer boundary the linking spans represent.
        Assert.Equal(ActivityKind.Producer, publishExported.Kind);
        Assert.Equal(ActivityKind.Consumer, consumeExported.Kind);
    }

    // Reads the messaging.correlation_id tag the trace source stamps on each span (Req 9.6).
    private static string? GetCorrelationIdTag(Activity span) =>
        span.Tags.FirstOrDefault(t => t.Key == "messaging.correlation_id").Value;

    /// <summary>A no-op <see cref="IMessagingLog"/>; the log leg is out of scope for span linking.</summary>
    private sealed class NoOpMessagingLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
        {
        }
    }

    /// <summary>A no-op <see cref="IMessagingMetrics"/>; the metrics leg is out of scope for span linking.</summary>
    private sealed class NoOpMessagingMetrics : IMessagingMetrics
    {
        public void RecordPublished()
        {
        }

        public void RecordConsumed()
        {
        }

        public void RecordRetry()
        {
        }

        public void RecordDeadLetter()
        {
        }
    }
}
