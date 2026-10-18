using System;
using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Contracts;
using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.Tests;

// Feature: rabbitmq-provider, Property 1: Envelope fidelity round trip.
// Validates: Requirements 6.1, 6.2, 6.3.
//
// For any valid transport-neutral MessageEnvelope e, projecting it onto the wire contract and back
// — EnvelopeMessage.FromEnvelope(e).ToEnvelope() — reconstructs an envelope equal to e in every
// delivered field:
//   - message id, correlation id, message type, UTC creation timestamp, contract version (Req 6.1);
//   - the header collection: same entry count, every key present and mapped to an equal value (Req 6.2);
//   - the payload byte sequence: equal length and equal at every byte position (Req 6.3).
//
// The generator produces arbitrary valid envelopes — correlation id 1..128 characters, 0..64 headers
// with arbitrary keys and values, and an arbitrary payload including empty — so the round trip is
// exercised across the full envelope input space.
[Trait("Feature", "rabbitmq-provider")]
[Trait("Property", "1")]
public class EnvelopeMessageFidelityPropertyTests
{
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(EnvelopeMessageFidelityPropertyTests) })]
    public Property RoundTrip_FromEnvelopeThenToEnvelope_PreservesEveryDeliveredField(MessageEnvelope original)
    {
        // Act: the lossless wire round trip under test.
        MessageEnvelope roundTripped = EnvelopeMessage.FromEnvelope(original).ToEnvelope();

        // Assert: every delivered field equals the published value.
        var holds = Preserves(original, roundTripped);

        return holds
            .ToProperty()
            .Classify(original.Headers.Count == 0, "no headers")
            .Classify(original.Headers.Count == MessageEnvelope.MaxHeaderCount, "max headers")
            .Classify(original.Payload.Length == 0, "empty payload")
            .Classify(original.CorrelationId.Length == 1, "min-length correlation id")
            .Classify(original.CorrelationId.Length == MessageEnvelope.MaxCorrelationIdLength, "max-length correlation id")
            .Label(
                $"headers={original.Headers.Count} payloadBytes={original.Payload.Length} " +
                $"correlationLen={original.CorrelationId.Length}");
    }

    /// <summary>
    /// True only when the round-tripped envelope preserves the scalar fields (Req 6.1), the header
    /// collection key-for-key and value-for-value (Req 6.2), and the payload byte sequence in length
    /// and at every position (Req 6.3).
    /// </summary>
    private static bool Preserves(MessageEnvelope original, MessageEnvelope roundTripped)
    {
        var scalarsPreserved =
            roundTripped.MessageId == original.MessageId
            && roundTripped.CorrelationId == original.CorrelationId
            && roundTripped.MessageType == original.MessageType
            && roundTripped.CreatedAtUtc == original.CreatedAtUtc
            && roundTripped.ContractVersion == original.ContractVersion;

        var headersPreserved = HeadersEqual(original.Headers, roundTripped.Headers);

        var payloadPreserved =
            roundTripped.Payload.Length == original.Payload.Length
            && roundTripped.Payload.Span.SequenceEqual(original.Payload.Span);

        return scalarsPreserved && headersPreserved && payloadPreserved;
    }

    /// <summary>Equal when both maps have the same entry count and every key maps to an equal value.</summary>
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

    public static Arbitrary<MessageEnvelope> MessageEnvelopeArbitrary() =>
        Arb.From(EnvelopeGen());

    /// <summary>
    /// Generates an arbitrary valid envelope: a non-empty message id, a correlation id of 1..128
    /// characters, a non-whitespace message type, a UTC timestamp, 0..64 headers with arbitrary keys
    /// and values, an arbitrary payload (including empty), and a contract version >= 1.
    /// </summary>
    private static Gen<MessageEnvelope> EnvelopeGen() =>
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

    /// <summary>
    /// 0..64 headers with arbitrary (but distinct) keys and arbitrary values, honouring the envelope
    /// header-count bound. Keys use a generated token suffixed with the index to guarantee distinctness
    /// without collapsing the generated count.
    /// </summary>
    private static Gen<IReadOnlyDictionary<string, string>> HeadersGen() =>
        from count in Gen.Choose(0, MessageEnvelope.MaxHeaderCount)
        from keys in Gen.ArrayOf(NonWhitespaceTokenGen(), count)
        from values in Gen.ArrayOf(NonWhitespaceTokenGen(), count)
        select (IReadOnlyDictionary<string, string>)Enumerable
            .Range(0, count)
            .ToDictionary(i => $"{keys[i]}-{i}", i => values[i], StringComparer.Ordinal);

    /// <summary>An arbitrary payload of 0..256 bytes, including the empty payload.</summary>
    private static Gen<ReadOnlyMemory<byte>> PayloadGen() =>
        from length in Gen.Choose(0, 256)
        from bytes in Gen.ArrayOf(ByteGen(), length)
        select new ReadOnlyMemory<byte>(bytes);

    private static Gen<byte> ByteGen() =>
        from value in Gen.Choose(0, 255)
        select (byte)value;
}
