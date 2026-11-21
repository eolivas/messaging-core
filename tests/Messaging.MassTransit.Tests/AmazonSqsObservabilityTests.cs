using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Messaging.Observability;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Unit tests for SNS/SQS observability: publish and consume events through
/// <see cref="ObservabilityContext"/>, bus-state and topology logs, trace linking, metrics labels, and
/// best-effort suppression. Requirements 15.1 to 15.7. No containers.
/// </summary>
[Trait("Feature", "sns-sqs-provider")]
public class AmazonSqsObservabilityTests
{
    private static ObservabilityContext NewContext(
        IMessagingLog? log = null, IMessagingMetrics? metrics = null, bool tracing = true) =>
        new(
            log ?? new NoOpMessagingLog(),
            metrics ?? new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracing),
            new PayloadRedactor());

    // ----- 15.1 / 15.2: publish and consume events -----------------------------------------------

    [Fact]
    public void OnPublish_RecordsPublishEventWithCorrelationIdAndUtcTimestamp()
    {
        var log = new RecordingLog();
        var before = DateTimeOffset.UtcNow;

        using var activity = NewContext(log).OnPublish("corr-pub-1", out var headers);

        var entry = Assert.Single(log.Events);
        Assert.Equal(MessagingEventType.Publish, entry.Type);
        Assert.Equal("corr-pub-1", entry.CorrelationId);
        Assert.Equal(TimeSpan.Zero, entry.Timestamp.Offset);
        Assert.InRange(entry.Timestamp, before, DateTimeOffset.UtcNow);
        Assert.Equal("corr-pub-1", headers[MessagingTraceSource.CorrelationIdHeader]);
    }

    [Fact]
    public void OnConsume_RecordsConsumeEventWithCorrelationIdAndUtcTimestamp()
    {
        var log = new RecordingLog();
        var headers = new Dictionary<string, string> { [MessagingTraceSource.CorrelationIdHeader] = "corr-con-1" };

        using var activity = NewContext(log).OnConsume("corr-con-1", headers);

        var entry = Assert.Single(log.Events);
        Assert.Equal(MessagingEventType.Consume, entry.Type);
        Assert.Equal("corr-con-1", entry.CorrelationId);
        Assert.Equal(TimeSpan.Zero, entry.Timestamp.Offset);
    }

    // ----- 15.3: bus-state logs -----------------------------------------------------------------

    [Fact]
    public async Task StartAndStop_LogBusStateTransitions()
    {
        var logger = new CapturingLogger<AmazonSqsBusAdapter>();
        var settings = new AmazonSqsProviderSettings { Region = "us-east-1", EndpointName = "orders" };
        var client = new FakeSnsSqsTopologyClient();
        var adapter = new AmazonSqsBusAdapter(
            new FakeBusControl(),
            settings,
            MessagingMode.PublishSubscribe,
            MassTransitRole.Publisher,
            Array.Empty<ContractRegistration>(),
            new SnsSqsTopologyManager(client, settings, NullLogger<SnsSqsTopologyManager>.Instance),
            client,
            NewContext(tracing: false),
            new AmazonSqsMetrics(),
            new InFlightTracker(),
            30,
            logger);

        await adapter.StartBusAsync(CancellationToken.None);
        await adapter.StopBusAsync(CancellationToken.None);

        var states = logger.Entries.Select(e => e.Fields.GetValueOrDefault("BusState")).ToArray();
        Assert.Equal(new object?[] { "started", "stopped" }, states);
    }

    // ----- 15.4: topology logs ------------------------------------------------------------------

    [Fact]
    public async Task Topology_LogsKindNameAndOutcomeForCreatedAndExistingResources()
    {
        var logger = new CapturingLogger<SnsSqsTopologyManager>();
        var client = new FakeSnsSqsTopologyClient();
        var manager = new SnsSqsTopologyManager(
            client, new AmazonSqsProviderSettings { Region = "us-east-1", EndpointName = "orders" }, logger);
        var plan = new TopologyPlan(
            new[] { new TopicSpec("orders-topic", false, null) },
            new[] { new QueueSpec("orders", false, null, null, 30, 60, false, null) },
            Array.Empty<SubscriptionSpec>());

        await manager.ApplyAsync(plan, true, CancellationToken.None);
        await manager.ApplyAsync(plan, true, CancellationToken.None);

        var rows = logger.Entries
            .Where(e => e.Fields.ContainsKey("ResourceKind"))
            .Select(e => (Kind: e.Fields["ResourceKind"]?.ToString(),
                          Name: e.Fields["ResourceName"]?.ToString(),
                          Outcome: e.Fields["Outcome"]?.ToString()))
            .ToList();
        Assert.Contains(rows, r => r.Name == "orders-topic" && r.Outcome == "Created" && r.Kind is not null);
        Assert.Contains(rows, r => r.Name == "orders" && r.Outcome == "Created" && r.Kind is not null);
        Assert.Contains(rows, r => r.Name == "orders-topic" && r.Outcome == "Exists");
        Assert.Contains(rows, r => r.Name == "orders" && r.Outcome == "Exists");
    }

    [Fact]
    public async Task Topology_VerifyOnlyMissingResource_LogsMissingOutcome()
    {
        var logger = new CapturingLogger<SnsSqsTopologyManager>();
        var manager = new SnsSqsTopologyManager(
            new FakeSnsSqsTopologyClient(),
            new AmazonSqsProviderSettings { Region = "us-east-1", EndpointName = "orders" },
            logger);
        var plan = new TopologyPlan(
            Array.Empty<TopicSpec>(),
            new[] { new QueueSpec("ghost", false, null, null, 30, 60, false, null) },
            Array.Empty<SubscriptionSpec>());

        await Assert.ThrowsAnyAsync<Exception>(
            () => manager.ApplyAsync(plan, false, CancellationToken.None));

        Assert.Contains(
            logger.Entries,
            e => Equals(e.Fields.GetValueOrDefault("ResourceName"), "ghost")
                && Equals(e.Fields.GetValueOrDefault("Outcome"), "Missing"));
    }

    // ----- 15.5: trace link ---------------------------------------------------------------------

    [Fact]
    public void ConsumeTrace_LinksToPublishTrace()
    {
        using var capture = new ActivityCapture();
        var context = NewContext();

        using var publish = context.OnPublish("corr-trace-1", out var headers);
        Assert.NotNull(publish);
        Assert.True(headers.ContainsKey(MessagingTraceSource.TraceParentHeader));

        using var consume = context.OnConsume("corr-trace-1", headers);

        Assert.NotNull(consume);
        Assert.Equal(publish!.TraceId, consume!.TraceId);
        Assert.Equal(publish.SpanId, consume.ParentSpanId);
    }

    [Fact]
    public void FanOutConsumes_AllLinkToTheSamePublishTrace()
    {
        using var capture = new ActivityCapture();
        var context = NewContext();
        using var publish = context.OnPublish("corr-fan-1", out var headers);

        var consumes = Enumerable.Range(0, 3).Select(_ => context.OnConsume("corr-fan-1", headers)).ToList();

        try
        {
            Assert.All(consumes, c =>
            {
                Assert.NotNull(c);
                Assert.Equal(publish!.TraceId, c!.TraceId);
                Assert.Equal(publish.SpanId, c.ParentSpanId);
            });
            Assert.Equal(3, consumes.Select(c => c!.SpanId).Distinct().Count());
        }
        finally
        {
            consumes.ForEach(c => c?.Dispose());
        }
    }

    // ----- 15.6: metrics ------------------------------------------------------------------------

    [Fact]
    public void Metrics_CountByTopicOrQueueNameAndNeverLabelWithCorrelationId()
    {
        var topic = "topic-" + Guid.NewGuid().ToString("N");
        var queue = "queue-" + Guid.NewGuid().ToString("N");
        var measurements = new ConcurrentBag<(string Instrument, long Value, Dictionary<string, object?> Tags)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == AmazonSqsMetrics.MeterName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            measurements.Add((instrument.Name, value, tags.ToArray().ToDictionary(t => t.Key, t => t.Value))));
        listener.Start();

        using var metrics = new AmazonSqsMetrics();
        metrics.RecordPublished(topic);
        metrics.RecordConsumed(queue);
        metrics.RecordDeadLettered(queue);
        metrics.RecordFailed(queue);

        var mine = measurements
            .Where(m => m.Tags.Values.Any(v => Equals(v, topic) || Equals(v, queue)))
            .ToDictionary(m => m.Instrument);
        Assert.Equal(topic, mine["messaging.amazonsqs.published.count"].Tags[AmazonSqsMetrics.TopicTag]);
        Assert.Equal(queue, mine["messaging.amazonsqs.consumed.count"].Tags[AmazonSqsMetrics.QueueTag]);
        Assert.Equal(queue, mine["messaging.amazonsqs.deadletter.count"].Tags[AmazonSqsMetrics.QueueTag]);
        Assert.Equal(queue, mine["messaging.amazonsqs.failed.count"].Tags[AmazonSqsMetrics.QueueTag]);
        Assert.All(mine.Values, m =>
        {
            Assert.Equal(1, m.Value);
            Assert.All(m.Tags.Keys, k =>
                Assert.True(k is AmazonSqsMetrics.TopicTag or AmazonSqsMetrics.QueueTag, $"Unexpected tag '{k}'."));
        });
    }

    // ----- 15.7: best-effort suppression --------------------------------------------------------

    [Fact]
    public void ThrowingSinks_AreSuppressedAndDoNotFailPublishOrConsume()
    {
        var context = NewContext(new ThrowingLog(), new ThrowingMetrics());

        var publishException = Record.Exception(() =>
        {
            using var a = context.OnPublish("corr-throw", out var headers);
            Assert.Equal("corr-throw", headers[MessagingTraceSource.CorrelationIdHeader]);
        });
        var consumeException = Record.Exception(() =>
        {
            using var a = context.OnConsume(
                "corr-throw", new Dictionary<string, string>());
        });

        Assert.Null(publishException);
        Assert.Null(consumeException);
    }

    [Fact]
    public void ThrowingLog_DoesNotSuppressMetricEmission()
    {
        var metrics = new CountingMetrics();

        using var a = NewContext(new ThrowingLog(), metrics).OnPublish("corr-iso", out _);

        Assert.Equal(1, metrics.Published);
    }

    [Fact]
    public void MetricsEmission_NeverThrows()
    {
        var metrics = new AmazonSqsMetrics();
        metrics.Dispose();

        var ex = Record.Exception(() =>
        {
            metrics.RecordPublished("t");
            metrics.RecordConsumed("q");
            metrics.RecordDeadLettered("q");
            metrics.RecordFailed("q");
        });

        Assert.Null(ex);
    }

    // ----- test doubles -------------------------------------------------------------------------

    private sealed class RecordingLog : IMessagingLog
    {
        public List<(MessagingEventType Type, string CorrelationId, DateTimeOffset Timestamp)> Events { get; } = new();

        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc) =>
            Events.Add((eventType, correlationId, timestampUtc));
    }

    private sealed class ThrowingLog : IMessagingLog
    {
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc) =>
            throw new InvalidOperationException("log sink down");
    }

    private sealed class ThrowingMetrics : IMessagingMetrics
    {
        public void RecordPublished() => throw new InvalidOperationException("metrics down");

        public void RecordConsumed() => throw new InvalidOperationException("metrics down");

        public void RecordRetry() => throw new InvalidOperationException("metrics down");

        public void RecordDeadLetter() => throw new InvalidOperationException("metrics down");
    }

    private sealed class CountingMetrics : IMessagingMetrics
    {
        public int Published { get; private set; }

        public void RecordPublished() => Published++;

        public void RecordConsumed() { }

        public void RecordRetry() { }

        public void RecordDeadLetter() { }
    }

    /// <summary>Samples every activity from the messaging source so spans are created in the test.</summary>
    private sealed class ActivityCapture : IDisposable
    {
        private readonly ActivityListener _listener = new()
        {
            ShouldListenTo = s => s.Name == MessagingTraceSource.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };

        public ActivityCapture() => ActivitySource.AddActivityListener(_listener);

        public void Dispose() => _listener.Dispose();
    }

    private sealed record LogEntry(string Message, IReadOnlyDictionary<string, object?> Fields);

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public ConcurrentQueue<LogEntry> Queue { get; } = new();

        public IReadOnlyList<LogEntry> Entries => Queue.ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var fields = new Dictionary<string, object?>();
            if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
            {
                foreach (var p in pairs)
                {
                    fields[p.Key] = p.Value;
                }
            }

            Queue.Enqueue(new LogEntry(formatter(state, exception), fields));
        }
    }
}
