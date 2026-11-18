using System;
using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Contracts;
using Messaging.MassTransit.AmazonSqs;

namespace Messaging.MassTransit.Tests;

// Validates: Requirements 6.3, 6.4, 6.5, 6.7, 6.9, 7.2.
//
// FromWire(ToWire(envelope)) yields an equal envelope. The consume headers are simulated from the
// attributes produced by ToWire (the h.* attributes), as ConsumeContext.Headers delivers them
// (design spike R4).
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "3")]
public class SnsSqsWireRoundTripPropertyTests
{
    private static readonly string[] ReservedKeys =
    {
        SnsSqsHeaderMapper.CorrelationIdAttribute,
        SnsSqsHeaderMapper.TraceParentAttribute,
        SnsSqsHeaderMapper.MessageTypeAttribute,
    };

    // Feature: sns-sqs-provider, Property 3: Envelope wire round trip
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsWireRoundTripPropertyTests) })]
    public Property RoundTrip_ToWireThenFromWire_PreservesEveryDeliveredField(
        MessageEnvelope original,
        TraceHeaders trace)
    {
        var wire = SnsSqsWireFormat.ToWire(original, trace.Values);
        var consumeHeaders = wire.AttributeHeaders
            .Select(a => new KeyValuePair<string, object>(a.Key, a.Value))
            .ToList();

        var result = SnsSqsWireFormat.FromWire(wire.Body, consumeHeaders);

        var scalars =
            result.MessageId == original.MessageId
            && string.Equals(result.CorrelationId, original.CorrelationId, StringComparison.Ordinal)
            && result.MessageType == original.MessageType
            && result.CreatedAtUtc == original.CreatedAtUtc
            && result.ContractVersion == original.ContractVersion;

        var headers =
            result.Headers.Count == original.Headers.Count
            && original.Headers.All(h =>
                result.Headers.TryGetValue(h.Key, out var v) && string.Equals(v, h.Value, StringComparison.Ordinal));

        var payload =
            result.Payload.Length == original.Payload.Length
            && result.Payload.Span.SequenceEqual(original.Payload.Span);

        return (scalars && headers && payload)
            .ToProperty()
            .Classify(original.Headers.Count == 0, "no headers")
            .Classify(original.Headers.Count > SnsSqsHeaderMapper.MaxUserAttributes, "headers overflow attributes")
            .Classify(original.Headers.Keys.Any(k => ReservedKeys.Contains(k)), "reserved key collision")
            .Classify(original.Payload.Length == 0, "empty payload")
            .Label($"headers={original.Headers.Count} attributes={wire.AttributeHeaders.Count}");
    }

    /// <summary>Wrapper so FsCheck resolves a dedicated generator for trace headers.</summary>
    public sealed record TraceHeaders(IReadOnlyDictionary<string, string> Values);

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<MessageEnvelope> EnvelopeArbitrary() => Arb.From(EnvelopeGen());

    public static Arbitrary<TraceHeaders> TraceHeadersArbitrary() => Arb.From(TraceGen());

    private static Gen<TraceHeaders> TraceGen() =>
        from withTrace in Gen.Elements(true, false)
        from withCorrelation in Gen.Elements(true, false)
        from trace in TokenGen(1, 40)
        from correlation in TokenGen(1, 40)
        select new TraceHeaders(new Dictionary<string, string>(StringComparer.Ordinal)
            .WithIf(withTrace, SnsSqsHeaderMapper.TraceParentAttribute, trace)
            .WithIf(withCorrelation, SnsSqsHeaderMapper.CorrelationIdAttribute, correlation));

    private static Gen<MessageEnvelope> EnvelopeGen() =>
        from messageId in Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty)
        from correlationId in TextGen(1, MessageEnvelope.MaxCorrelationIdLength)
        from messageType in TokenGen(1, 32)
        from seconds in Gen.Choose(0, int.MaxValue)
        from headers in HeadersGen()
        from payload in PayloadGen()
        from contractVersion in Gen.Choose(1, 50)
        select new MessageEnvelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = messageType,
            CreatedAtUtc = new DateTimeOffset(
                DateTimeOffset.UnixEpoch.Ticks + ((long)seconds * TimeSpan.TicksPerSecond),
                TimeSpan.Zero),
            Headers = headers,
            Payload = payload,
            ContractVersion = contractVersion,
        };

    private static Gen<char> AsciiTokenChar() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_".ToCharArray());

    // Printable ASCII plus non-ASCII BMP characters (accents, Greek, CJK); no whitespace, no surrogates.
    private static Gen<char> TextChar() => Gen.OneOf(
        AsciiTokenChar(),
        Gen.Elements(".:/h".ToCharArray()),
        Gen.Elements("éñüßΩλЖ日本語한".ToCharArray()));

    private static Gen<string> TokenGen(int min, int max) =>
        from length in Gen.Choose(min, max)
        from chars in Gen.ArrayOf(AsciiTokenChar(), length)
        select new string(chars);

    private static Gen<string> TextGen(int min, int max) =>
        from length in Gen.Choose(min, max)
        from chars in Gen.ArrayOf(TextChar(), length)
        select new string(chars);

    // Values may be empty, over the 1024 attribute limit, or non-ASCII, so both partitions are exercised.
    private static Gen<string> HeaderValueGen() => Gen.OneOf(
        TokenGen(1, 16),
        TextGen(0, 16),
        TextGen(1025, 1100));

    private static Gen<string> HeaderKeyGen() => Gen.OneOf(
        TokenGen(1, 12),
        TextGen(1, 12),
        Gen.Elements(ReservedKeys),
        from suffix in TokenGen(1, 8) select SnsSqsHeaderMapper.UserHeaderPrefix + suffix);

    private static Gen<IReadOnlyDictionary<string, string>> HeadersGen() =>
        from count in Gen.Choose(0, MessageEnvelope.MaxHeaderCount)
        from keys in Gen.ArrayOf(HeaderKeyGen(), count)
        from values in Gen.ArrayOf(HeaderValueGen(), count)
        select (IReadOnlyDictionary<string, string>)Enumerable
            .Range(0, count)
            .GroupBy(i => keys[i], StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => values[g.Last()], StringComparer.Ordinal);

    // 0..256 arbitrary bytes, so empty and non-ASCII (>= 0x80) payloads are both produced.
    private static Gen<ReadOnlyMemory<byte>> PayloadGen() =>
        from length in Gen.Choose(0, 256)
        from values in Gen.ArrayOf(Gen.Choose(0, 255), length)
        select new ReadOnlyMemory<byte>(values.Select(v => (byte)v).ToArray());
}

internal static class DictionaryTestExtensions
{
    public static Dictionary<string, string> WithIf(
        this Dictionary<string, string> dictionary, bool condition, string key, string value)
    {
        if (condition)
        {
            dictionary[key] = value;
        }

        return dictionary;
    }
}
