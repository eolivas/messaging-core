using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.Providers;
using Messaging.Publishing;

namespace Messaging.Tests.Publishing;

// Feature: messaging-core, Property 6: Publishing preserves envelope fields to delivery.
// Validates: Requirements 4.4.
//
// For any VALID envelope that already carries a valid correlation id (so the correlation id gate
// regenerates nothing), the PublishPipeline hands the active provider at delivery an envelope that
// equals the submitted one on every field: message id, correlation id, message type, creation
// timestamp, all headers (key-for-key and value-for-value), payload (byte-for-byte), and contract
// version. The CapturingMessagingProvider records exactly what the publisher received, which the
// property compares against the submitted envelope. (Req 4.4)
[Trait("Feature", "messaging-core")]
[Trait("Property", "6")]
public class PublishPipelineFieldPreservationPropertyTests
{
    private const MessagingMode ActiveMode = MessagingMode.PublishSubscribe;

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(PublishPipelineFieldPreservationPropertyTests) })]
    public Property Publishing_PreservesEveryEnvelopeField_ThroughToDelivery(MessageEnvelope submitted)
    {
        // Arrange: a pipeline whose resolved provider captures the delivered envelope verbatim.
        var provider = new CapturingMessagingProvider(ActiveMode);
        var registry = new ProviderRegistry(new IMessagingProvider[] { provider });
        var pipeline = new PublishPipeline(new CorrelationIdGate(), registry, ActiveMode);

        // Act: publish the already-valid envelope (synchronous completion for the in-memory double).
        var result = pipeline.PublishAsync(submitted, CancellationToken.None).GetAwaiter().GetResult();

        // Assert: delivery happened exactly once and the captured envelope matches field-for-field.
        var delivered = provider.DeliveredEnvelopes;
        var deliveredExactlyOnce = result.Succeeded && delivered.Count == 1;
        var fieldsPreserved = deliveredExactlyOnce && FieldsPreserved(submitted, delivered[0]);

        return fieldsPreserved
            .ToProperty()
            .Classify(submitted.Headers.Count == 0, "no headers")
            .Classify(submitted.Headers.Count == MessageEnvelope.MaxHeaderCount, "max headers")
            .Classify(submitted.Payload.Length == 0, "empty payload")
            .Label(
                $"delivered={delivered.Count} succeeded={result.Succeeded}; " +
                $"headers submitted={submitted.Headers.Count}");
    }

    /// <summary>
    /// True only when every envelope field is preserved value-for-value / byte-for-byte: message
    /// id, correlation id, message type, creation timestamp, headers (same keys and values),
    /// payload bytes, and contract version (Req 4.4).
    /// </summary>
    private static bool FieldsPreserved(MessageEnvelope submitted, MessageEnvelope delivered) =>
        delivered.MessageId == submitted.MessageId
        && delivered.CorrelationId == submitted.CorrelationId
        && delivered.MessageType == submitted.MessageType
        && delivered.CreatedAtUtc == submitted.CreatedAtUtc
        && delivered.ContractVersion == submitted.ContractVersion
        && HeadersEqual(submitted.Headers, delivered.Headers)
        && submitted.Payload.Span.SequenceEqual(delivered.Payload.Span);

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

    public static Arbitrary<MessageEnvelope> ValidEnvelopeArbitrary() => Arb.From(ValidEnvelopeGen());

    /// <summary>
    /// Generates envelopes that already satisfy every invariant so the pipeline neither rejects
    /// them nor regenerates the correlation id: a non-empty GUID message id, a 1..128 character
    /// correlation id, a non-whitespace message type, a UTC timestamp, 0..64 distinct headers, an
    /// arbitrary payload (including empty), and a contract version >= 1.
    /// </summary>
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

    private static Gen<Guid> NonEmptyGuidGen() =>
        Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty);

    /// <summary>A correlation id of 1..128 characters (inclusive), so the gate leaves it untouched.</summary>
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
