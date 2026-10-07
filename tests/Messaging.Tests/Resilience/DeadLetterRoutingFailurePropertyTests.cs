using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.Observability;
using Messaging.Resilience;

namespace Messaging.Tests.Resilience;

// Feature: messaging-core, Property 20: Dead-letter routing failure keeps the message unacknowledged.
// Validates: Requirements 8.6.
//
// For any generated envelope, failure reason, attempt count, and idempotency key:
//   SINK FAILS    — the DeadLetterRouter returns RetainedUnacknowledged (the message stays in the
//                   pipeline, not acknowledged), the routing-failure structured log is emitted
//                   EXACTLY ONCE, and the dead-letter metric is NOT emitted (no dead-letter actually
//                   completed) — Req 8.6.
//   SINK SUCCEEDS — the router returns RoutedAndAcknowledged (the complementary path, confirming the
//                   failure disposition is the sink-failure case and not the default).
//
// The sink failure is an ordinary MessagingFailureException. OperationCanceledException is
// deliberately NOT generated here: the router does not catch cancellation (it propagates), so it is
// out of scope for this routing-failure property.
[Trait("Feature", "messaging-core")]
[Trait("Property", "20")]
public class DeadLetterRoutingFailurePropertyTests
{
    /// <summary>
    /// A generated routing scenario: the full dead-letter context plus whether the sink fails. A
    /// single property drives both the sink-failure path (the focus) and the sink-success path (the
    /// complement) so the disposition is proven to hinge on the sink outcome.
    /// </summary>
    public sealed record RoutingScenario(
        string CorrelationId,
        string FailureReason,
        int AttemptCount,
        string? IdempotencyKey,
        byte[] Payload,
        bool SinkFails);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(DeadLetterRoutingFailurePropertyTests) })]
    public Property RouteAsync_SinkFailure_RetainsUnacknowledgedWithoutMetric(RoutingScenario scenario)
    {
        // Arrange: a sink that throws when the scenario demands failure, and recording observability
        // doubles so we can assert exactly what the router emitted.
        var sinkError = new TransportConnectionException("dead-letter sink unavailable");
        var sink = new ConfigurableDeadLetterSink(scenario.SinkFails, sinkError);
        var metrics = new RecordingDeadLetterMetrics();
        var log = new RecordingDeadLetterLog();
        var router = new DeadLetterRouter(sink, metrics, log);

        var envelope = BuildEnvelope(scenario.CorrelationId, scenario.Payload);

        // Act.
        var result = router
            .RouteAsync(envelope, scenario.FailureReason, scenario.IdempotencyKey, scenario.AttemptCount, CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        // Assert.
        bool holds;
        if (scenario.SinkFails)
        {
            holds =
                result == DeadLetterRoutingResult.RetainedUnacknowledged   // message retained, not acked (Req 8.6)
                && log.RoutingFailedCount == 1                             // routing-failure log emitted exactly once (Req 8.6)
                && ReferenceEquals(log.LastRoutingError, sinkError)        // the actual sink error is logged
                && log.LastRoutingFailedCorrelationId == scenario.CorrelationId
                && log.LastRoutingFailedFailureReason == scenario.FailureReason
                && log.LastRoutingFailedAttemptCount == scenario.AttemptCount
                && metrics.RecordedCount == 0                              // metric NOT emitted — nothing completed (Req 8.6)
                && log.DeadLetteredCount == 0;                            // success log NOT emitted
        }
        else
        {
            holds =
                result == DeadLetterRoutingResult.RoutedAndAcknowledged    // complement: sink accepted
                && log.RoutingFailedCount == 0                             // no routing-failure log on success
                && metrics.RecordedCount == 1                             // one metric on the completed dead-letter
                && log.DeadLetteredCount == 1;
        }

        return holds
            .ToProperty()
            .Label(
                $"sinkFails={scenario.SinkFails}, attempts={scenario.AttemptCount} => " +
                $"result={result}, routingFailedLogs={log.RoutingFailedCount}, " +
                $"deadLetteredLogs={log.DeadLetteredCount}, metrics={metrics.RecordedCount}")
            .Classify(scenario.SinkFails, "sink fails (retain path)")
            .Classify(!scenario.SinkFails, "sink succeeds (acknowledge path)")
            .Classify(scenario.IdempotencyKey is null, "no idempotency key");
    }

    private static MessageEnvelope BuildEnvelope(string correlationId, byte[] payload) => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = "orders.created",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new ReadOnlyMemory<byte>(payload),
        ContractVersion = 1,
    };

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<RoutingScenario> RoutingScenarioArbitrary() => Arb.From(RoutingScenarioGen());

    /// <summary>
    /// Generates a routing scenario across non-empty correlation ids / failure reasons, a positive
    /// attempt count, an optionally-null idempotency key, an arbitrary payload, and both sink
    /// outcomes, so the single property exercises the retain-unacknowledged path and the
    /// acknowledge complement over the whole input space.
    /// </summary>
    private static Gen<RoutingScenario> RoutingScenarioGen() =>
        from correlationId in NonEmptyIdentifier()
        from failureReason in NonEmptyIdentifier()
        from attemptCount in Gen.Choose(1, 100)
        from hasKey in Gen.Elements(true, false)
        from key in NonEmptyIdentifier()
        from payload in Gen.ArrayOf(Gen.Choose(0, 255).Select(b => (byte)b))
        from sinkFails in Gen.Elements(true, false)
        select new RoutingScenario(
            correlationId,
            failureReason,
            attemptCount,
            hasKey ? key : null,
            payload,
            sinkFails);

    /// <summary>A non-empty, non-whitespace token for correlation ids, failure reasons, and keys.</summary>
    private static Gen<string> NonEmptyIdentifier() =>
        from chars in Gen.NonEmptyListOf(Gen.Elements("abcdefghijklmnopqrstuvwxyz0123456789-".ToCharArray()))
        select new string(chars.ToArray());

    // ----- Recording observability doubles ----------------------------------------------------

    /// <summary>An <see cref="IDeadLetterSink"/> that either accepts or throws, per the scenario.</summary>
    private sealed class ConfigurableDeadLetterSink : IDeadLetterSink
    {
        private readonly bool _fails;
        private readonly Exception _error;

        public ConfigurableDeadLetterSink(bool fails, Exception error)
        {
            _fails = fails;
            _error = error;
        }

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
            => _fails ? Task.FromException(_error) : Task.CompletedTask;
    }

    /// <summary>Records how many times the dead-letter metric was emitted.</summary>
    private sealed class RecordingDeadLetterMetrics : IDeadLetterMetrics
    {
        public int RecordedCount { get; private set; }

        public void RecordDeadLetter() => RecordedCount++;
    }

    /// <summary>Records the dead-lettered and routing-failed structured log entries and their fields.</summary>
    private sealed class RecordingDeadLetterLog : IDeadLetterLog
    {
        public int DeadLetteredCount { get; private set; }
        public int RoutingFailedCount { get; private set; }
        public Exception? LastRoutingError { get; private set; }
        public string? LastRoutingFailedCorrelationId { get; private set; }
        public string? LastRoutingFailedFailureReason { get; private set; }
        public int LastRoutingFailedAttemptCount { get; private set; }

        public void DeadLettered(string correlationId, string failureReason, int attemptCount)
            => DeadLetteredCount++;

        public void DeadLetterRoutingFailed(
            string correlationId,
            string failureReason,
            int attemptCount,
            Exception routingError)
        {
            RoutingFailedCount++;
            LastRoutingError = routingError;
            LastRoutingFailedCorrelationId = correlationId;
            LastRoutingFailedFailureReason = failureReason;
            LastRoutingFailedAttemptCount = attemptCount;
        }
    }
}
