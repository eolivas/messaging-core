using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Contracts;
using Messaging.Kafka.Streaming;

namespace Messaging.Kafka.Tests;

// Feature: kafka-provider, Property 2: Missing required header is rejected.
// Validates: Requirements 9.6.
//
// For any valid envelope, mapping it to a Kafka record with the production wire format and then
// removing exactly one of the five required metadata headers makes ToEnvelope throw
// MissingRequiredHeaderException naming that header, and no envelope is produced (all-or-nothing).
//
[Trait("Feature", "kafka-provider")]
[Trait("Property", "2")]
public class KafkaWireFormatMissingHeaderPropertyTests
{
    private static readonly string[] RequiredHeaders =
    {
        Messaging.Kafka.Streaming.KafkaWireFormat.MessageIdHeader,
        Messaging.Kafka.Streaming.KafkaWireFormat.CorrelationIdHeader,
        Messaging.Kafka.Streaming.KafkaWireFormat.MessageTypeHeader,
        Messaging.Kafka.Streaming.KafkaWireFormat.CreatedAtHeader,
        Messaging.Kafka.Streaming.KafkaWireFormat.ContractVersionHeader,
    };

    /// <summary>An arbitrary envelope paired with the one required header to strip from its record.</summary>
    public sealed record MissingHeaderCase(MessageEnvelope Envelope, string RemovedHeader);

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(KafkaWireFormatMissingHeaderPropertyTests) })]
    public Property ToEnvelope_WithOneRequiredHeaderRemoved_ThrowsNamingThatHeaderAndYieldsNoEnvelope(MissingHeaderCase testCase)
    {
        // Arrange: serialize with the production format, then drop exactly one required header.
        var produced = Messaging.Kafka.Streaming.KafkaWireFormat.ToMessage(testCase.Envelope);
        var headers = new Headers();
        foreach (var header in produced.Headers)
        {
            if (!string.Equals(header.Key, testCase.RemovedHeader, StringComparison.Ordinal))
            {
                headers.Add(header.Key, header.GetValueBytes());
            }
        }

        var consumed = new Message<Ignore, byte[]> { Value = produced.Value, Headers = headers };

        // Act
        MessageEnvelope? result = null;
        MissingRequiredHeaderException? thrown = null;
        try
        {
            result = Messaging.Kafka.Streaming.KafkaWireFormat.ToEnvelope(consumed);
        }
        catch (MissingRequiredHeaderException ex)
        {
            thrown = ex;
        }

        // Assert: rejected, names the removed header, and no envelope escaped.
        var holds = thrown is not null
            && result is null
            && thrown.HeaderName == testCase.RemovedHeader
            && thrown.Message.Contains(testCase.RemovedHeader, StringComparison.Ordinal);

        return holds
            .ToProperty()
            .Label($"removed={testCase.RemovedHeader} threw={thrown is not null} envelopeReturned={result is not null}");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<MissingHeaderCase> MissingHeaderCaseArbitrary() =>
        Arb.From(
            from envelope in EnvelopeGen()
            from removed in Gen.Elements(RequiredHeaders)
            select new MissingHeaderCase(envelope, removed));

    private static Gen<MessageEnvelope> EnvelopeGen() =>
        from messageId in Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty)
        from correlationId in TokenGen(1, MessageEnvelope.MaxCorrelationIdLength)
        from messageType in TokenGen(1, 32)
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

    private static Gen<string> TokenGen(int minLength, int maxLength) =>
        from length in Gen.Choose(minLength, maxLength)
        from chars in Gen.ArrayOf(TokenCharGen(), length)
        select new string(chars);

    private static Gen<char> TokenCharGen() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:".ToCharArray());

    private static Gen<DateTimeOffset> UtcTimestampGen() =>
        from seconds in Gen.Choose(0, int.MaxValue)
        select new DateTimeOffset(
            DateTimeOffset.UnixEpoch.Ticks + ((long)seconds * TimeSpan.TicksPerSecond),
            TimeSpan.Zero);

    /// <summary>0..64 user headers with distinct keys (index-suffixed so they never equal a reserved name).</summary>
    private static Gen<IReadOnlyDictionary<string, string>> HeadersGen() =>
        from count in Gen.Choose(0, MessageEnvelope.MaxHeaderCount)
        from keys in Gen.ArrayOf(TokenGen(1, 32), count)
        from values in Gen.ArrayOf(TokenGen(1, 32), count)
        select (IReadOnlyDictionary<string, string>)Enumerable
            .Range(0, count)
            .ToDictionary(i => $"{keys[i]}-{i}", i => values[i], StringComparer.Ordinal);

    private static Gen<ReadOnlyMemory<byte>> PayloadGen() =>
        from length in Gen.Choose(0, 256)
        from bytes in Gen.ArrayOf(Gen.Choose(0, 255).Select(v => (byte)v), length)
        select new ReadOnlyMemory<byte>(bytes);
}
