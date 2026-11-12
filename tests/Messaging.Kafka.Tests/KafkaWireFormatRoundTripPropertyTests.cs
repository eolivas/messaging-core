using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Contracts;
using ProductionWireFormat = Messaging.Kafka.Streaming.KafkaWireFormat;

namespace Messaging.Kafka.Tests;

// Feature: kafka-provider, Property 1: Wire-format round-trip identity.
// Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 10.5.
//
// For any valid MessageEnvelope e, the PRODUCTION wire format round trip
// ToEnvelope(ToMessage(e)) reconstructs an envelope equal to e in every field: message id,
// correlation id, message type, contract version, full-precision UTC timestamp (all ticks), the user
// header collection (count, keys, values) and every payload byte (including the empty payload).
//
[Trait("Feature", "kafka-provider")]
[Trait("Property", "1")]
public class KafkaWireFormatRoundTripPropertyTests
{
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(KafkaWireFormatRoundTripPropertyTests) })]
    public Property RoundTrip_ToEnvelopeOfToMessage_PreservesEveryField(MessageEnvelope original)
    {
        // Act: produce-side mapping, then the consume side sees the same value/headers as Ignore-keyed.
        Message<Null, byte[]> produced = ProductionWireFormat.ToMessage(original);
        var consumed = new Message<Ignore, byte[]>
        {
            Value = produced.Value,
            Headers = produced.Headers,
        };
        MessageEnvelope roundTripped = ProductionWireFormat.ToEnvelope(consumed);

        return Preserves(original, roundTripped)
            .ToProperty()
            .Classify(original.Headers.Count == 0, "no headers")
            .Classify(original.Headers.Count == MessageEnvelope.MaxHeaderCount, "max headers")
            .Classify(original.Payload.Length == 0, "empty payload")
            .Label(
                $"headers={original.Headers.Count} payloadBytes={original.Payload.Length} " +
                $"correlationLen={original.CorrelationId.Length} createdAt={original.CreatedAtUtc:O}");
    }

    private static bool Preserves(MessageEnvelope original, MessageEnvelope actual)
    {
        var scalars =
            actual.MessageId == original.MessageId
            && actual.CorrelationId == original.CorrelationId
            && actual.MessageType == original.MessageType
            && actual.ContractVersion == original.ContractVersion
            && actual.CreatedAtUtc == original.CreatedAtUtc
            && actual.CreatedAtUtc.Ticks == original.CreatedAtUtc.Ticks
            && actual.CreatedAtUtc.Offset == TimeSpan.Zero;

        var headers =
            actual.Headers.Count == original.Headers.Count
            && original.Headers.All(kvp => actual.Headers.TryGetValue(kvp.Key, out var v) && v == kvp.Value);

        var payload =
            actual.Payload.Length == original.Payload.Length
            && actual.Payload.Span.SequenceEqual(original.Payload.Span);

        return scalars && headers && payload;
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<MessageEnvelope> MessageEnvelopeArbitrary() => Arb.From(EnvelopeGen());

    private static Gen<MessageEnvelope> EnvelopeGen() =>
        from messageId in Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty)
        from correlationId in CorrelationIdGen()
        from messageType in TokenGen(1, 32)
        from createdAtUtc in UtcTimestampGen()
        from headers in HeadersGen()
        from payload in PayloadGen()
        from contractVersion in ContractVersionGen()
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

    private static Gen<string> CorrelationIdGen() => TokenGen(1, MessageEnvelope.MaxCorrelationIdLength);

    /// <summary>Tokens of ASCII plus a few non-ASCII BMP characters (valid UTF-8, no surrogates).</summary>
    private static Gen<string> TokenGen(int minLength, int maxLength) =>
        from length in Gen.Choose(minLength, maxLength)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<char> TokenCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:/ \u00e9\u00f1\u00fc\u4e2d\u6587\u20ac".ToCharArray());

    private static Gen<int> ContractVersionGen() =>
        Gen.OneOf(Gen.Choose(1, 50), Gen.Elements(1, int.MaxValue));

    /// <summary>UTC timestamp over a wide range with arbitrary tick (100ns) remainder, so fractional precision is exercised.</summary>
    private static Gen<DateTimeOffset> UtcTimestampGen() =>
        from seconds in Gen.Choose(0, int.MaxValue)
        from subSecondTicks in Gen.Choose(0, (int)(TimeSpan.TicksPerSecond - 1))
        select new DateTimeOffset(
            DateTimeOffset.UnixEpoch.Ticks + ((long)seconds * TimeSpan.TicksPerSecond) + subSecondTicks,
            TimeSpan.Zero);

    /// <summary>0..64 user headers; keys are made distinct (and non-reserved) with an index suffix.</summary>
    private static Gen<IReadOnlyDictionary<string, string>> HeadersGen() =>
        from count in Gen.Choose(0, MessageEnvelope.MaxHeaderCount)
        from keys in Gen.ArrayOf(TokenGen(1, 24), count)
        from values in Gen.ArrayOf(TokenGen(0, 48), count)
        select (IReadOnlyDictionary<string, string>)Enumerable
            .Range(0, count)
            .ToDictionary(i => $"h{keys[i]}-{i}", i => values[i], StringComparer.Ordinal);

    private static Gen<ReadOnlyMemory<byte>> PayloadGen() =>
        from length in Gen.OneOf(Gen.Elements(0), Gen.Choose(0, 512))
        from bytes in Gen.ArrayOf(Gen.Choose(0, 255).Select(v => (byte)v), length)
        select new ReadOnlyMemory<byte>(bytes);
}
