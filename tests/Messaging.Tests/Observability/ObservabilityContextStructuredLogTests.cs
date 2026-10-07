using System;
using System.Collections.Generic;
using Messaging.Observability;
using Xunit;

namespace Messaging.Tests.Observability;

// Structured-log field unit tests for Task 15.5 (Req 9.1): when a publish, consume, retry, or
// dead-letter event is observed, the ObservabilityContext emits EXACTLY ONE structured log entry
// through IMessagingLog whose discrete fields are present and correct — the event type matching the
// operation, the correlation id carried unchanged, and a UTC timestamp. A capturing fake log records
// every field so each test asserts on the exact values. Tracing is disabled so no span is started and
// the log entry is the only observable emission under test.
public class ObservabilityContextStructuredLogTests
{
    private const string CorrelationId = "corr-15-5";

    [Fact]
    public void OnPublish_Always_EmitsSingleStructuredEntryWithPublishEventType()
    {
        // Arrange
        var log = new CapturingMessagingLog();
        var context = BuildContext(log);
        var before = DateTimeOffset.UtcNow;

        // Act
        context.OnPublish(CorrelationId, out _);
        var after = DateTimeOffset.UtcNow;

        // Assert
        var entry = Assert.Single(log.Entries);
        AssertEntry(entry, MessagingEventType.Publish, before, after);
    }

    [Fact]
    public void OnConsume_Always_EmitsSingleStructuredEntryWithConsumeEventType()
    {
        // Arrange
        var log = new CapturingMessagingLog();
        var context = BuildContext(log);
        var headers = new Dictionary<string, string>();
        var before = DateTimeOffset.UtcNow;

        // Act
        context.OnConsume(CorrelationId, headers);
        var after = DateTimeOffset.UtcNow;

        // Assert
        var entry = Assert.Single(log.Entries);
        AssertEntry(entry, MessagingEventType.Consume, before, after);
    }

    [Fact]
    public void OnRetry_Always_EmitsSingleStructuredEntryWithRetryEventType()
    {
        // Arrange
        var log = new CapturingMessagingLog();
        var context = BuildContext(log);
        var before = DateTimeOffset.UtcNow;

        // Act
        context.OnRetry(CorrelationId);
        var after = DateTimeOffset.UtcNow;

        // Assert
        var entry = Assert.Single(log.Entries);
        AssertEntry(entry, MessagingEventType.Retry, before, after);
    }

    [Fact]
    public void OnDeadLetter_Always_EmitsSingleStructuredEntryWithDeadLetterEventType()
    {
        // Arrange
        var log = new CapturingMessagingLog();
        var context = BuildContext(log);
        var before = DateTimeOffset.UtcNow;

        // Act
        context.OnDeadLetter(CorrelationId);
        var after = DateTimeOffset.UtcNow;

        // Assert
        var entry = Assert.Single(log.Entries);
        AssertEntry(entry, MessagingEventType.DeadLetter, before, after);
    }

    // Asserts the captured entry carries all three structured fields correctly (Req 9.1): the expected
    // event type, the correlation id unchanged, and a UTC timestamp (zero offset) within the window the
    // operation ran.
    private static void AssertEntry(
        CapturingMessagingLog.Entry entry,
        MessagingEventType expectedEventType,
        DateTimeOffset before,
        DateTimeOffset after)
    {
        Assert.Equal(expectedEventType, entry.EventType);
        Assert.Equal(CorrelationId, entry.CorrelationId);
        Assert.Equal(TimeSpan.Zero, entry.TimestampUtc.Offset);
        Assert.InRange(entry.TimestampUtc, before, after);
    }

    // Builds a context with tracing disabled so OnPublish/OnConsume start no span and the structured
    // log entry is the sole emission under observation. Metrics are a no-op; the redactor is real.
    private static ObservabilityContext BuildContext(IMessagingLog log) => new(
        log,
        new NoOpMessagingMetrics(),
        new MessagingTraceSource(tracingEnabled: false),
        new PayloadRedactor());

    /// <summary>
    /// A capturing <see cref="IMessagingLog"/> double that records the exact field values of every
    /// structured entry, in order, so a test can assert the event type, correlation id, and UTC
    /// timestamp are present and correct and that exactly one entry was emitted (Req 9.1).
    /// </summary>
    private sealed class CapturingMessagingLog : IMessagingLog
    {
        private readonly List<Entry> _entries = new();

        /// <summary>Every captured structured entry, in emission order.</summary>
        public IReadOnlyList<Entry> Entries => _entries;

        /// <inheritdoc />
        public void MessageEvent(MessagingEventType eventType, string correlationId, DateTimeOffset timestampUtc)
            => _entries.Add(new Entry(eventType, correlationId, timestampUtc));

        /// <summary>A captured structured log entry with its three discrete fields.</summary>
        internal sealed record Entry(
            MessagingEventType EventType,
            string CorrelationId,
            DateTimeOffset TimestampUtc);
    }

    /// <summary>A no-op <see cref="IMessagingMetrics"/> double; counter behavior is out of scope here.</summary>
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
