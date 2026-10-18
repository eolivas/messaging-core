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

namespace Messaging.MassTransit.Tests;

// Feature: rabbitmq-provider, Property 5: Dead-letter preserves context.
// Validates: Requirements 8.5.
//
// When a Message_Envelope is routed to the Dead_Letter_Sink, the DeadLetterMessage captured by the
// sink preserves, exactly, the context the operator needs to inspect and reprocess the message:
//   - the message id carried on the original envelope (Envelope.MessageId);
//   - the correlation id, character-for-character (CorrelationId);
//   - the payload byte sequence — equal length and equal at every byte position (Envelope.Payload);
//   - the total attempt count performed before dead-lettering (AttemptCount), equal exactly to the
//     attempt count the processor supplied.
//
// The property drives the production DeadLetterRouter.RouteAsync — the single path through which both
// dead-letter triggers (retry exhaustion / non-transient and poison / unbindable) funnel — with a
// capturing sink that records the DeadLetterMessage. No broker is involved. The generator produces
// arbitrary failing envelopes (random message id, correlation id 1..128 chars, arbitrary message
// type, UTC timestamp, 0..64 headers, arbitrary payload including empty, contract version >= 1) and
// an arbitrary total attempt count across its full validated range, so the preservation is exercised
// across the full dead-letter input space.
[Trait("Feature", "rabbitmq-provider")]
[Trait("Property", "5")]
public class DeadLetterContextPreservationPropertyTests
{
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(DeadLetterContextPreservationPropertyTests) })]
    public Property RouteAsync_WhenDeadLettering_PreservesMessageIdCorrelationIdPayloadAndAttemptCount(
        MessageEnvelope envelope,
        string failureReason,
        int attemptCount)
    {
        // Arrange: a capturing sink and the production router that builds the DeadLetterMessage.
        var sink = new CapturingDeadLetterSink();
        var router = new DeadLetterRouter(sink, new NoOpDeadLetterMetrics(), new NoOpDeadLetterLog());

        // Act: route the failing envelope through the single production dead-letter path.
        DeadLetterRoutingResult result = router
            .RouteAsync(envelope, failureReason, idempotencyKey: null, attemptCount, CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        // The accepting sink routes-and-acknowledges, so exactly one dead-letter was captured.
        DeadLetterMessage captured = sink.Captured.Single();

        bool holds =
            result == DeadLetterRoutingResult.RoutedAndAcknowledged
            && Preserves(envelope, attemptCount, captured);

        return holds
            .ToProperty()
            .Classify(envelope.Payload.Length == 0, "empty payload")
            .Classify(envelope.CorrelationId.Length == 1, "min-length correlation id")
            .Classify(envelope.CorrelationId.Length == MessageEnvelope.MaxCorrelationIdLength, "max-length correlation id")
            .Classify(attemptCount == 1, "attempt count = 1 (min)")
            .Classify(attemptCount == ConfigurationBoundsMaxAttempts, "attempt count = 100 (max)")
            .Label(
                $"payloadBytes={envelope.Payload.Length} correlationLen={envelope.CorrelationId.Length} " +
                $"attemptCount={attemptCount}");
    }

    /// <summary>
    /// True only when the captured dead-letter message preserves the message id and correlation id
    /// from the original envelope, the payload byte sequence in length and at every position, and the
    /// total attempt count exactly as supplied (Req 8.5).
    /// </summary>
    private static bool Preserves(MessageEnvelope original, int attemptCount, DeadLetterMessage captured)
    {
        var messageIdPreserved = captured.Envelope.MessageId == original.MessageId;

        var correlationIdPreserved =
            captured.CorrelationId == original.CorrelationId
            && captured.Envelope.CorrelationId == original.CorrelationId;

        var payloadPreserved =
            captured.Envelope.Payload.Length == original.Payload.Length
            && captured.Envelope.Payload.Span.SequenceEqual(original.Payload.Span);

        var attemptCountPreserved = captured.AttemptCount == attemptCount;

        return messageIdPreserved && correlationIdPreserved && payloadPreserved && attemptCountPreserved;
    }

    // The validated inclusive upper bound for a Retry_Policy attempt count (Req 8.1); the total
    // attempt count performed before dead-lettering never exceeds it.
    private const int ConfigurationBoundsMaxAttempts = 100;

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<MessageEnvelope> MessageEnvelopeArbitrary() =>
        Arb.From(EnvelopeGen());

    /// <summary>A non-whitespace failure reason, as the router requires a non-null reason.</summary>
    public static Arbitrary<string> FailureReasonArbitrary() =>
        Arb.From(NonWhitespaceTokenGen());

    /// <summary>A total attempt count across the full validated inclusive range [1, 100] (Req 8.1).</summary>
    public static Arbitrary<int> AttemptCountArbitrary() =>
        Arb.From(Gen.Choose(1, ConfigurationBoundsMaxAttempts));

    /// <summary>
    /// Generates an arbitrary valid failing envelope (random non-empty message id, correlation id
    /// 1..128 chars, non-whitespace message type, UTC timestamp, 0..64 headers, arbitrary payload
    /// including empty, contract version >= 1), matching the envelope generator style used by the
    /// sibling property tests. The envelope content does not steer the routing result — the accepting
    /// sink does — but it exercises preservation across the full envelope input space.
    /// </summary>
    private static Gen<MessageEnvelope> EnvelopeGen() =>
        from messageId in Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty)
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

    private static Gen<string> CorrelationIdGen() =>
        from length in Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<string> NonWhitespaceTokenGen() =>
        from length in Gen.Choose(1, 32)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<char> TokenCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:".ToCharArray());

    private static Gen<DateTimeOffset> UtcTimestampGen() =>
        from ticks in Gen.Choose(0, int.MaxValue)
        select new DateTimeOffset(
            DateTimeOffset.UnixEpoch.Ticks + ((long)ticks * TimeSpan.TicksPerSecond),
            TimeSpan.Zero);

    private static Gen<IReadOnlyDictionary<string, string>> HeadersGen() =>
        from count in Gen.Choose(0, MessageEnvelope.MaxHeaderCount)
        from keys in Gen.ArrayOf(NonWhitespaceTokenGen(), count)
        from values in Gen.ArrayOf(NonWhitespaceTokenGen(), count)
        select (IReadOnlyDictionary<string, string>)Enumerable
            .Range(0, count)
            .ToDictionary(i => $"{keys[i]}-{i}", i => values[i], StringComparer.Ordinal);

    private static Gen<ReadOnlyMemory<byte>> PayloadGen() =>
        from length in Gen.Choose(0, 256)
        from bytes in Gen.ArrayOf(ByteGen(), length)
        select new ReadOnlyMemory<byte>(bytes);

    private static Gen<byte> ByteGen() =>
        from value in Gen.Choose(0, 255)
        select (byte)value;

    // ----- Minimal in-test fakes for the substitutable seams ----------------------------------

    /// <summary>
    /// A dead-letter sink that accepts and records every message routed to it, so the test can assert
    /// the captured <see cref="DeadLetterMessage"/> preserves the dead-letter context (Req 8.5).
    /// </summary>
    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        private readonly List<DeadLetterMessage> _captured = new();

        public IReadOnlyList<DeadLetterMessage> Captured => _captured;

        public Task SendAsync(DeadLetterMessage message, CancellationToken cancellationToken)
        {
            _captured.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class NoOpDeadLetterMetrics : IDeadLetterMetrics
    {
        public void RecordDeadLetter()
        {
        }
    }

    private sealed class NoOpDeadLetterLog : IDeadLetterLog
    {
        public void DeadLettered(string correlationId, string failureReason, int attemptCount)
        {
        }

        public void DeadLetterRoutingFailed(string correlationId, string failureReason, int attemptCount, Exception error)
        {
        }
    }
}
