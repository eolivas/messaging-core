using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.Resilience;

namespace Messaging.Tests.Resilience;

// Feature: messaging-core, Property 14: Dead-lettering preserves the message and its context.
// Validates: Requirements 6.6, 8.2.
//
// For any valid original envelope and any dead-letter context (correlation id carried by the
// envelope, an optional idempotency key, a failure reason, and a total attempt count), routing the
// message through the DeadLetterRouter produces a DeadLetterMessage that:
//   - preserves the complete original envelope byte-for-byte: payload bytes identical, message id,
//     message type, creation timestamp, correlation id, contract version, and all headers unchanged
//     (key-for-key and value-for-value) — Req 8.2;
//   - carries the context verbatim: the envelope's correlation id, the idempotency key (including
//     null), the failure reason, and the total processing attempt count — Req 6.6, 8.2;
//   - stamps a UTC dead-letter timestamp (zero offset) — Req 8.2.
//
// A recording sink captures the exact DeadLetterMessage the router sent, which the property compares
// field-for-field against the generated envelope and context. No-op observability doubles let the
// router be built without standing up real metrics or logging.
[Trait("Feature", "messaging-core")]
[Trait("Property", "14")]
public class DeadLetterRouterPreservationPropertyTests
{
    /// <summary>A generated dead-letter scenario: the original envelope plus the routing context.</summary>
    public sealed record DeadLetterScenario(
        MessageEnvelope Envelope,
        string? IdempotencyKey,
        string FailureReason,
        int AttemptCount);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(DeadLetterRouterPreservationPropertyTests) })]
    public Property RouteAsync_ValidEnvelopeAndContext_PreservesMessageAndContext(DeadLetterScenario scenario)
    {
        // Arrange: a recording sink wrapped in the router with no-op observability, and a clock
        // bracket so the stamped dead-letter timestamp can be bounded to the routing window.
        var sink = new RecordingDeadLetterSink();
        var router = new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());

        var before = DateTimeOffset.UtcNow;

        // Act.
        var result = router
            .RouteAsync(
                scenario.Envelope,
                scenario.FailureReason,
                scenario.IdempotencyKey,
                scenario.AttemptCount,
                CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        var after = DateTimeOffset.UtcNow;

        // Assert: routed exactly once, acknowledged, and the captured dead-letter preserves the
        // original envelope byte-for-byte together with the full context.
        var routedExactlyOnce =
            result == DeadLetterRoutingResult.RoutedAndAcknowledged && sink.Sent.Count == 1;

        var holds = routedExactlyOnce && Preserves(scenario, sink.Sent[0], before, after);

        return holds
            .ToProperty()
            .Classify(scenario.IdempotencyKey is null, "null idempotency key")
            .Classify(scenario.Envelope.Headers.Count == 0, "no headers")
            .Classify(scenario.Envelope.Headers.Count == MessageEnvelope.MaxHeaderCount, "max headers")
            .Classify(scenario.Envelope.Payload.Length == 0, "empty payload")
            .Label(
                $"routed={sink.Sent.Count} result={result}; " +
                $"headers={scenario.Envelope.Headers.Count} attempts={scenario.AttemptCount}");
    }

    /// <summary>
    /// True only when the dead-letter preserves the complete original envelope byte-for-byte and
    /// carries every context value verbatim, with a UTC dead-letter timestamp inside the routing
    /// window (Req 6.6, 8.2).
    /// </summary>
    private static bool Preserves(
        DeadLetterScenario scenario,
        DeadLetterMessage deadLetter,
        DateTimeOffset before,
        DateTimeOffset after)
    {
        var envelope = scenario.Envelope;

        var envelopePreserved =
            ReferenceEquals(deadLetter.Envelope, envelope)
            || (deadLetter.Envelope.MessageId == envelope.MessageId
                && deadLetter.Envelope.CorrelationId == envelope.CorrelationId
                && deadLetter.Envelope.MessageType == envelope.MessageType
                && deadLetter.Envelope.CreatedAtUtc == envelope.CreatedAtUtc
                && deadLetter.Envelope.ContractVersion == envelope.ContractVersion
                && HeadersEqual(envelope.Headers, deadLetter.Envelope.Headers)
                && envelope.Payload.Span.SequenceEqual(deadLetter.Envelope.Payload.Span));

        var contextPreserved =
            deadLetter.CorrelationId == envelope.CorrelationId
            && deadLetter.IdempotencyKey == scenario.IdempotencyKey
            && deadLetter.FailureReason == scenario.FailureReason
            && deadLetter.AttemptCount == scenario.AttemptCount;

        var timestampIsUtcAndInWindow =
            deadLetter.DeadLetteredAtUtc.Offset == TimeSpan.Zero
            && deadLetter.DeadLetteredAtUtc >= before
            && deadLetter.DeadLetteredAtUtc <= after;

        return envelopePreserved && contextPreserved && timestampIsUtcAndInWindow;
    }

    private static bool HeadersEqual(
        IReadOnlyDictionary<string, string> expected,
        IReadOnlyDictionary<string, string> actual)
    {
        if (expected.Count != actual.Count)
        {
            return false;
        }

        return expected.All(kvp => actual.TryGetValue(kvp.Key, out var value) && value == kvp.Value);
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<DeadLetterScenario> DeadLetterScenarioArbitrary() =>
        Arb.From(DeadLetterScenarioGen());

    /// <summary>
    /// Generates a valid original envelope (randomized payload bytes, message id, type, timestamp,
    /// 0..64 headers, correlation id, contract version) paired with a dead-letter context: an
    /// optional idempotency key (including null, since one may not have been derived — Req 6.6), a
    /// non-empty failure reason, and a total attempt count across the valid retry range (1..100).
    /// </summary>
    private static Gen<DeadLetterScenario> DeadLetterScenarioGen() =>
        from envelope in ValidEnvelopeGen()
        from idempotencyKey in OptionalKeyGen()
        from failureReason in NonWhitespaceTokenGen()
        from attemptCount in Gen.Choose(1, 100)
        select new DeadLetterScenario(envelope, idempotencyKey, failureReason, attemptCount);

    private static Gen<MessageEnvelope> ValidEnvelopeGen() =>
        from messageId in NonEmptyGuidGen()
        from correlationId in CorrelationIdGen()
        from messageType in NonWhitespaceTokenGen()
        from createdAtUtc in UtcTimestampGen()
        from headers in HeadersGen()
        from payload in PayloadGen()
        from contractVersion in Gen.Choose(1, 50)
        select new MessageEnvelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = messageType,
            CreatedAtUtc = createdAtUtc,
            Headers = headers,
            Payload = payload,
            ContractVersion = contractVersion,
        };

    /// <summary>Either null (no key derived) or a non-whitespace token, to cover both Req 6.6 cases.</summary>
    private static Gen<string?> OptionalKeyGen() =>
        Gen.OneOf(
            Gen.Constant((string?)null),
            NonWhitespaceTokenGen().Select(key => (string?)key));

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

    private static Gen<DateTimeOffset> UtcTimestampGen() =>
        from ticks in Gen.Choose(0, int.MaxValue)
        select new DateTimeOffset(
            DateTimeOffset.UnixEpoch.Ticks + ((long)ticks * TimeSpan.TicksPerSecond),
            TimeSpan.Zero);

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
