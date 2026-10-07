using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.Resilience;
using Xunit;

namespace Messaging.Tests.Resilience;

// Dead-letter emission unit tests for Task 13.5 (Req 8.4): when a message is dead-lettered, the
// router emits EXACTLY ONE metric and EXACTLY ONE structured log entry carrying the correlation id,
// failure reason, and processing attempt count. These are deterministic example-based tests; the
// preservation and routing-failure behaviors are covered by the Task 13.2/13.4 property tests.
public class DeadLetterRouterEmissionTests
{
    private const string CorrelationId = "corr-13-5";
    private const string FailureReason = "downstream unavailable after retries";
    private const int AttemptCount = 5;

    [Fact]
    public async Task RouteAsync_SuccessfulDeadLetter_RecordsMetricExactlyOnce()
    {
        // Arrange
        var metrics = new RecordingDeadLetterMetrics();
        var router = new DeadLetterRouter(new RecordingDeadLetterSink(), metrics, new RecordingDeadLetterLog());

        // Act
        var result = await router.RouteAsync(
            BuildEnvelope(CorrelationId),
            FailureReason,
            idempotencyKey: "idem-key",
            AttemptCount,
            CancellationToken.None);

        // Assert
        Assert.Equal(DeadLetterRoutingResult.RoutedAndAcknowledged, result);
        Assert.Equal(1, metrics.RecordDeadLetterCount);
    }

    [Fact]
    public async Task RouteAsync_SuccessfulDeadLetter_EmitsExactlyOneDeadLetteredLogEntry()
    {
        // Arrange
        var log = new RecordingDeadLetterLog();
        var router = new DeadLetterRouter(new RecordingDeadLetterSink(), new RecordingDeadLetterMetrics(), log);

        // Act
        await router.RouteAsync(
            BuildEnvelope(CorrelationId),
            FailureReason,
            idempotencyKey: "idem-key",
            AttemptCount,
            CancellationToken.None);

        // Assert: exactly one dead-lettered entry, and no routing-failure entry on the success path.
        Assert.Single(log.DeadLetteredEntries);
        Assert.Empty(log.RoutingFailedEntries);
    }

    [Fact]
    public async Task RouteAsync_SuccessfulDeadLetter_LogEntryCarriesCorrelationIdReasonAndAttemptCount()
    {
        // Arrange
        var log = new RecordingDeadLetterLog();
        var router = new DeadLetterRouter(new RecordingDeadLetterSink(), new RecordingDeadLetterMetrics(), log);

        // Act
        await router.RouteAsync(
            BuildEnvelope(CorrelationId),
            FailureReason,
            idempotencyKey: "idem-key",
            AttemptCount,
            CancellationToken.None);

        // Assert: the structured entry captured the exact field values (Req 8.4).
        var entry = Assert.Single(log.DeadLetteredEntries);
        Assert.Equal(CorrelationId, entry.CorrelationId);
        Assert.Equal(FailureReason, entry.FailureReason);
        Assert.Equal(AttemptCount, entry.AttemptCount);
    }

    [Fact]
    public async Task RouteForContract_PoisonPayload_EmitsSingleMetricAndLogWithAttemptCountOne()
    {
        // Arrange: an unbindable/poison payload routes with no retry, so the attempt count is 1.
        var metrics = new RecordingDeadLetterMetrics();
        var log = new RecordingDeadLetterLog();
        var router = new DeadLetterRouter(new RecordingDeadLetterSink(), metrics, log);

        // Act
        await router.RouteForContract(
            BuildEnvelope(CorrelationId),
            failureReason: "unknown contract",
            idempotencyKey: null,
            CancellationToken.None);

        // Assert
        Assert.Equal(1, metrics.RecordDeadLetterCount);
        var entry = Assert.Single(log.DeadLetteredEntries);
        Assert.Equal(CorrelationId, entry.CorrelationId);
        Assert.Equal("unknown contract", entry.FailureReason);
        Assert.Equal(1, entry.AttemptCount);
    }

    [Fact]
    public async Task RouteAsync_SinkFails_DoesNotRecordMetricAndEmitsOnlyRoutingFailureLog()
    {
        // Arrange: a sink that throws means no dead-letter actually completed, so the metric and the
        // dead-lettered log entry MUST NOT be emitted — only the routing-failure entry (Req 8.6).
        var metrics = new RecordingDeadLetterMetrics();
        var log = new RecordingDeadLetterLog();
        var router = new DeadLetterRouter(new ThrowingDeadLetterSink(), metrics, log);

        // Act
        var result = await router.RouteAsync(
            BuildEnvelope(CorrelationId),
            FailureReason,
            idempotencyKey: "idem-key",
            AttemptCount,
            CancellationToken.None);

        // Assert
        Assert.Equal(DeadLetterRoutingResult.RetainedUnacknowledged, result);
        Assert.Equal(0, metrics.RecordDeadLetterCount);
        Assert.Empty(log.DeadLetteredEntries);
        Assert.Single(log.RoutingFailedEntries);
    }

    private static MessageEnvelope BuildEnvelope(string correlationId) => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = "orders.created",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }),
        ContractVersion = 1,
    };

    /// <summary>A dead-letter sink that always fails, to exercise the routing-failure path (Req 8.6).</summary>
    private sealed class ThrowingDeadLetterSink : IDeadLetterSink
    {
        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("sink unavailable"));
    }
}
