using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Providers;
using Messaging.Publishing;
using Messaging.Tests.Publishing;

namespace Messaging.Tests.Consuming;

// Feature: messaging-core, Property 7: Correlation id propagates unchanged end-to-end.
// Validates: Requirements 9.2, 9.4.
//
// End-to-end means Producer publish -> provider delivery -> consumer handler. For any VALID
// envelope submitted to the publish pipeline carrying a valid correlation id (so the gate leaves it
// untouched), the correlation id observed by the consumer handler at the end of the consume pipeline
// is byte-for-byte identical to the one submitted at publish. The property wires the two existing
// sides together: PublishPipeline delivers into a CapturingMessagingProvider; the captured delivered
// envelope is then fed into a Running ConsumePipeline whose RecordingHandlerInvoker records the
// correlation id the handler sees. It asserts submitted == delivered == handler-observed, which also
// confirms the correlation id is present through both publish and consume (Req 9.2, 9.4).
[Trait("Feature", "messaging-core")]
[Trait("Property", "7")]
public class ConsumePipelineCorrelationPropagationPropertyTests
{
    private const MessagingMode ActiveMode = MessagingMode.PublishSubscribe;

    // A single registered message type keeps generated payloads bindable on the consume side while
    // the publish side treats the type as an opaque, preserved field.
    private const string ProbeMessageType = "correlation-probe";

    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(ConsumePipelineCorrelationPropagationPropertyTests) })]
    public Property Propagation_PreservesCorrelationId_FromPublishThroughToHandler(MessageEnvelope submitted)
    {
        // Arrange: publish side delivers verbatim into the capturing provider.
        var provider = new CapturingMessagingProvider(ActiveMode);
        var registry = new ProviderRegistry(new IMessagingProvider[] { provider });
        var publishPipeline = new PublishPipeline(new CorrelationIdGate(), registry, ActiveMode);

        // Consume side: a Running host whose handler invoker records the observed correlation id.
        var invoker = new RecordingHandlerInvoker();
        var binder = new JsonContractBinder(new[]
        {
            new ContractRegistration(ProbeMessageType, typeof(CorrelationProbeContract)),
        });
        var consumePipeline = new ConsumePipeline(new ConsumerHostStateMachine(), binder, invoker);
        consumePipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        // Act: publish, then feed the captured delivered envelope into the consume pipeline.
        var publishResult = publishPipeline.PublishAsync(submitted, CancellationToken.None).GetAwaiter().GetResult();

        var delivered = provider.DeliveredEnvelopes;
        var deliveredExactlyOnce = publishResult.Succeeded && delivered.Count == 1;

        if (deliveredExactlyOnce)
        {
            consumePipeline.DeliverAsync(delivered[0], CancellationToken.None).GetAwaiter().GetResult();
        }

        // Assert: submitted == delivered == handler-observed, byte-for-byte on the correlation id.
        var handlerObservedOnce = invoker.ObservedCorrelationIds.Count == 1;
        var propagatedUnchanged =
            deliveredExactlyOnce
            && handlerObservedOnce
            && string.Equals(submitted.CorrelationId, delivered[0].CorrelationId, StringComparison.Ordinal)
            && string.Equals(delivered[0].CorrelationId, invoker.ObservedCorrelationIds[0], StringComparison.Ordinal);

        return propagatedUnchanged
            .ToProperty()
            .Classify(submitted.CorrelationId.Length == 1, "min-length correlation id")
            .Classify(submitted.CorrelationId.Length == MessageEnvelope.MaxCorrelationIdLength, "max-length correlation id")
            .Label(
                $"submitted='{submitted.CorrelationId}' " +
                $"delivered={(deliveredExactlyOnce ? delivered[0].CorrelationId : "<none>")} " +
                $"handler={(handlerObservedOnce ? invoker.ObservedCorrelationIds[0] : "<none>")}");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<MessageEnvelope> ValidEnvelopeArbitrary() => Arb.From(ValidEnvelopeGen());

    /// <summary>
    /// Generates envelopes that are valid for BOTH pipelines: a non-empty GUID message id, a 1..128
    /// character correlation id (so the publish gate regenerates nothing), the single registered
    /// probe message type (so the consume bind step succeeds), a UTC timestamp, 0..64 distinct
    /// headers, a JSON payload that deserializes into <see cref="CorrelationProbeContract"/>, and a
    /// contract version >= 1.
    /// </summary>
    private static Gen<MessageEnvelope> ValidEnvelopeGen() =>
        from messageId in NonEmptyGuidGen()
        from correlationId in CorrelationIdGen()
        from createdAtUtc in UtcTimestampGen()
        from headers in HeadersGen()
        from payload in ProbePayloadGen()
        from contractVersion in Gen.Choose(1, 50)
        select new MessageEnvelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = ProbeMessageType,
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

    /// <summary>
    /// A JSON payload that binds to <see cref="CorrelationProbeContract"/>, so the consume pipeline's
    /// bind step succeeds for every generated envelope.
    /// </summary>
    private static Gen<ReadOnlyMemory<byte>> ProbePayloadGen() =>
        from value in Gen.Choose(int.MinValue, int.MaxValue)
        select new ReadOnlyMemory<byte>(
            JsonSerializer.SerializeToUtf8Bytes(new CorrelationProbeContract { Value = value }));
}
