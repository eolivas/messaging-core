using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;
using Messaging.Resilience;

namespace Messaging.Tests.Resilience;

// Feature: messaging-core, Property 19: Unbindable payloads are poison and skip retry.
// Validates: Requirement 8.3.
//
// An unbindable / poison payload is routed to the dead-letter sink WITHOUT applying the retry
// policy. The poison entry point is DeadLetterRouter.RouteForContract, which routes with attempt
// count = 1 and never invokes the processing/retry loop. For any generated envelope and any
// max-attempts configuration (1..100), routing a poison payload results in:
//   - exactly one DeadLetterMessage delivered to the sink (routed once);
//   - that message records AttemptCount == 1 (the single failed bind attempt, no reprocessing);
//   - the attempt count is 1 REGARDLESS of the configured RetryPolicyConfig.MaxAttempts, proving the
//     retry policy is not consulted on the poison path;
//   - the original envelope is preserved by reference (byte-for-byte);
//   - the pipeline is told to acknowledge (RoutedAndAcknowledged).
//
// The configured retry policy is generated with varied MaxAttempts and handed to the scenario purely
// to demonstrate it has no influence: the poison route produces AttemptCount == 1 every time. Because
// RouteForContract performs a single sink send and no delay/backoff, no retry wait can occur.
[Trait("Feature", "messaging-core")]
[Trait("Property", "19")]
public class DeadLetterRouterPoisonSkipsRetryPropertyTests
{
    /// <summary>
    /// A generated scenario: the envelope to poison-route and the retry policy that is in effect but
    /// must be ignored by the poison path.
    /// </summary>
    public sealed record PoisonScenario(MessageEnvelope Envelope, RetryPolicyConfig Policy);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(DeadLetterRouterPoisonSkipsRetryPropertyTests) })]
    public Property RouteForContract_PoisonPayload_SkipsRetryAndRecordsSingleAttempt(PoisonScenario scenario)
    {
        // Arrange: a capturing sink wrapped in the router with no-op observability. The retry policy
        // is held only to prove it is never consulted on the poison path.
        var sink = new RecordingDeadLetterSink();
        var router = new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());

        const string failureReason = "payload could not be bound to a known contract";
        const string idempotencyKey = "idem-key";

        // Act: route the unbindable payload as poison.
        DeadLetterRoutingResult result = router
            .RouteForContract(scenario.Envelope, failureReason, idempotencyKey, CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        // Assert: routed exactly once, attempt count fixed at 1 (retry policy not applied), envelope
        // preserved. The attempt count is 1 for EVERY generated MaxAttempts in 1..100 — it does not
        // scale with the policy — which is exactly what "skip retry" means for the poison path (Req 8.3).
        DeadLetterMessage? routed = sink.Sent.SingleOrDefault();

        bool holds =
            result == DeadLetterRoutingResult.RoutedAndAcknowledged
            && sink.Sent.Count == 1
            && routed is not null
            && routed.AttemptCount == 1
            && ReferenceEquals(routed.Envelope, scenario.Envelope)   // envelope preserved byte-for-byte (Req 8.2/8.3)
            && routed.FailureReason == failureReason
            && routed.IdempotencyKey == idempotencyKey
            && routed.CorrelationId == scenario.Envelope.CorrelationId;

        return holds
            .ToProperty()
            .Label(
                $"maxAttempts={scenario.Policy.MaxAttempts}, backoff={scenario.Policy.Backoff} => " +
                $"result={result}, routed={sink.Sent.Count}, attemptCount={routed?.AttemptCount}")
            .Classify(scenario.Policy.MaxAttempts == 1, "max attempts = 1")
            .Classify(scenario.Policy.MaxAttempts > 1, "max attempts > 1 (retry policy clearly skipped)");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<PoisonScenario> PoisonScenarioArbitrary() => Arb.From(PoisonScenarioGen());

    /// <summary>
    /// Generates a valid envelope paired with a retry policy whose <see cref="RetryPolicyConfig.MaxAttempts"/>
    /// spans the full valid range (1..100, per <see cref="ConfigurationBounds.MaxAttempts"/>) and every
    /// backoff strategy, so the property shows the poison path's attempt count stays fixed at 1 no
    /// matter how the retry policy is configured.
    /// </summary>
    private static Gen<PoisonScenario> PoisonScenarioGen() =>
        from envelope in EnvelopeGen()
        from maxAttempts in Gen.Choose(ConfigurationBounds.MaxAttempts.Min, ConfigurationBounds.MaxAttempts.Max)
        from backoff in Gen.Elements(
            BackoffStrategy.Fixed,
            BackoffStrategy.Exponential,
            BackoffStrategy.ExponentialWithJitter)
        from delaySeconds in Gen.Choose(
            ConfigurationBounds.PerAttemptDelaySeconds.Min,
            ConfigurationBounds.PerAttemptDelaySeconds.Max)
        select new PoisonScenario(
            envelope,
            new RetryPolicyConfig
            {
                MaxAttempts = maxAttempts,
                Backoff = backoff,
                PerAttemptDelaySeconds = delaySeconds,
            });

    /// <summary>
    /// Generates a valid <see cref="MessageEnvelope"/>: non-empty id, a correlation id within the
    /// permitted 1..128 range, a non-empty message type, a UTC timestamp, 0..64 headers, arbitrary
    /// payload bytes, and a contract version &gt;= 1.
    /// </summary>
    private static Gen<MessageEnvelope> EnvelopeGen() =>
        from messageId in NonEmptyGuidGen()
        from correlationId in CorrelationIdGen()
        from messageType in NonWhitespaceTokenGen()
        from headers in HeadersGen()
        from payload in PayloadGen()
        from contractVersion in Gen.Choose(1, 50)
        select new MessageEnvelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = messageType,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Headers = headers,
            Payload = payload,
            ContractVersion = contractVersion,
        };

    private static Gen<Guid> NonEmptyGuidGen() =>
        Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty);

    /// <summary>A correlation id of 1..128 characters (inclusive).</summary>
    private static Gen<string> CorrelationIdGen() =>
        from length in Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<string> NonWhitespaceTokenGen() =>
        from length in Gen.Choose(1, 32)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    /// <summary>Visible ASCII token characters (no whitespace), keeping generated values readable.</summary>
    private static Gen<char> TokenCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:".ToCharArray());

    /// <summary>0..64 headers with distinct keys so the generated map always honours the count bound.</summary>
    private static Gen<IReadOnlyDictionary<string, string>> HeadersGen() =>
        from count in Gen.Choose(0, MessageEnvelope.MaxHeaderCount)
        from values in Gen.ArrayOf(NonWhitespaceTokenGen(), count)
        select (IReadOnlyDictionary<string, string>)Enumerable
            .Range(0, count)
            .ToDictionary(i => $"header-{i}", i => values[i]);

    private static Gen<ReadOnlyMemory<byte>> PayloadGen() =>
        from length in Gen.Choose(0, 256)
        from bytes in Gen.ArrayOf(ByteGen(), length)
        select new ReadOnlyMemory<byte>(bytes);

    private static Gen<byte> ByteGen() =>
        from value in Gen.Choose(0, 255)
        select (byte)value;
}
