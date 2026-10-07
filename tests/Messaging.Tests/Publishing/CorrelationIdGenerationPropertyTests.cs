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
using Xunit;

namespace Messaging.Tests.Publishing;

// Feature: messaging-core, Property 5: A correlation id is always generated when absent.
// Validates: Requirements 4.2, 9.3.
//
// For any otherwise-valid envelope submitted WITHOUT a correlation id (null, empty, or
// whitespace-only), the PublishPipeline always:
//   GENERATES  — attaches a non-empty, non-whitespace correlation id within the 1..128 char bound
//                (Req 4.2, 9.3), never leaving the absent value in place.
//   DELIVERS   — hands the envelope to the active provider, and the delivered envelope carries the
//                generated id (the message is published with the id that was produced).
//   UNIQUE     — distinct absent-correlation-id submissions receive distinct generated ids, so the
//                id is globally unique rather than a fixed constant (Req 9.3).
// Every other envelope field is left valid so the only variable under test is the absent
// correlation id; the generator varies message id, message type, timestamp, header set, payload,
// contract version, and the specific flavor of "absent" (null/empty/whitespace).
[Trait("Feature", "messaging-core")]
[Trait("Property", "5")]
public class CorrelationIdGenerationPropertyTests
{
    private const MessagingMode ActiveMode = MessagingMode.PublishSubscribe;

    /// <summary>
    /// An otherwise-valid envelope whose correlation id is absent, paired with the specific
    /// absent form (null/empty/whitespace) so failures report which flavor triggered them.
    /// </summary>
    public sealed record AbsentCorrelationEnvelope(MessageEnvelope Envelope, string AbsentForm);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(CorrelationIdGenerationPropertyTests) })]
    public Property Publish_AbsentCorrelationId_AlwaysGeneratesValidUniqueIdAndDelivers(
        AbsentCorrelationEnvelope first,
        AbsentCorrelationEnvelope second)
    {
        var (pipelineOne, providerOne) = BuildPipeline();
        var (pipelineTwo, providerTwo) = BuildPipeline();

        var resultOne = pipelineOne
            .PublishAsync(first.Envelope, CancellationToken.None)
            .GetAwaiter().GetResult();
        var resultTwo = pipelineTwo
            .PublishAsync(second.Envelope, CancellationToken.None)
            .GetAwaiter().GetResult();

        // Each submission succeeds and is delivered exactly once.
        var deliveredOnce =
            resultOne.Succeeded && providerOne.DeliveredEnvelopes.Count == 1 &&
            resultTwo.Succeeded && providerTwo.DeliveredEnvelopes.Count == 1;

        var deliveredOne = providerOne.DeliveredEnvelopes.Single();
        var deliveredTwo = providerTwo.DeliveredEnvelopes.Single();

        // GENERATES: the delivered envelope carries a valid, in-bound correlation id.
        var bothIdsValid =
            IsValidGeneratedId(deliveredOne.CorrelationId) &&
            IsValidGeneratedId(deliveredTwo.CorrelationId);

        // UNIQUE: two independent absent submissions get distinct generated ids.
        var idsAreUnique = !string.Equals(
            deliveredOne.CorrelationId,
            deliveredTwo.CorrelationId,
            StringComparison.Ordinal);

        // DELIVERS unchanged otherwise: every non-correlation field is preserved to delivery.
        var fieldsPreserved =
            FieldsPreservedExceptCorrelationId(first.Envelope, deliveredOne) &&
            FieldsPreservedExceptCorrelationId(second.Envelope, deliveredTwo);

        return (deliveredOnce && bothIdsValid && idsAreUnique && fieldsPreserved)
            .ToProperty()
            .Classify(first.AbsentForm == "null" || second.AbsentForm == "null", "null correlation id")
            .Classify(first.AbsentForm == "empty" || second.AbsentForm == "empty", "empty correlation id")
            .Classify(first.AbsentForm == "whitespace" || second.AbsentForm == "whitespace", "whitespace correlation id");
    }

    private static bool IsValidGeneratedId(string? correlationId) =>
        !string.IsNullOrWhiteSpace(correlationId)
        && correlationId!.Length >= 1
        && correlationId.Length <= MessageEnvelope.MaxCorrelationIdLength;

    private static bool FieldsPreservedExceptCorrelationId(MessageEnvelope submitted, MessageEnvelope delivered) =>
        delivered.MessageId == submitted.MessageId
        && string.Equals(delivered.MessageType, submitted.MessageType, StringComparison.Ordinal)
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

        return expected.All(kvp =>
            actual.TryGetValue(kvp.Key, out var value)
            && string.Equals(value, kvp.Value, StringComparison.Ordinal));
    }

    private static (PublishPipeline Pipeline, CapturingMessagingProvider Provider) BuildPipeline()
    {
        var provider = new CapturingMessagingProvider(ActiveMode);
        var registry = new ProviderRegistry(new IMessagingProvider[] { provider });
        var pipeline = new PublishPipeline(new CorrelationIdGate(), registry, ActiveMode);
        return (pipeline, provider);
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<AbsentCorrelationEnvelope> AbsentCorrelationEnvelopeArbitrary() =>
        Arb.From(AbsentCorrelationEnvelopeGen());

    /// <summary>
    /// Produces an envelope valid in every respect except the correlation id, which is one of the
    /// three "absent" forms the gate treats as "generate one": null, empty, or whitespace-only.
    /// All other fields are generated within their valid bounds so the only reason the pipeline
    /// could touch the correlation id is its absence.
    /// </summary>
    private static Gen<AbsentCorrelationEnvelope> AbsentCorrelationEnvelopeGen()
    {
        var messageIdGen = Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty);

        var messageTypeGen = Gen.Elements(
            "orders.created", "orders.updated", "payments.captured", "inventory.reserved", "test.contract");

        // UTC timestamps (zero offset) within a plausible window; the envelope requires zero offset.
        var timestampGen = from offsetSeconds in Gen.Choose(-315_360_000, 315_360_000) // +/- ~10 years
                           select new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(offsetSeconds);

        var contractVersionGen = Gen.Choose(1, 5);

        var byteGen = Gen.Choose(0, 255).Select(b => (byte)b);
        var payloadGen = Gen.Choose(0, 16)
            .SelectMany(len => Gen.ArrayOf(byteGen, len))
            .Select(bytes => new ReadOnlyMemory<byte>(bytes));

        var headersGen = HeadersGen();

        // The three flavors of "absent" the gate normalizes by generating a new id (Req 4.2, 9.3).
        var absentGen = Gen.Elements<(string Value, string Form)>(
            (null!, "null"),
            (string.Empty, "empty"),
            ("   ", "whitespace"),
            ("\t", "whitespace"),
            ("\n", "whitespace"));

        return from messageId in messageIdGen
               from messageType in messageTypeGen
               from createdAtUtc in timestampGen
               from contractVersion in contractVersionGen
               from headers in headersGen
               from payload in payloadGen
               from absent in absentGen
               select new AbsentCorrelationEnvelope(
                   new MessageEnvelope
                   {
                       MessageId = messageId,
                       CorrelationId = absent.Value,
                       MessageType = messageType,
                       CreatedAtUtc = createdAtUtc,
                       Headers = headers,
                       Payload = payload,
                       ContractVersion = contractVersion,
                   },
                   absent.Form);
    }

    /// <summary>
    /// Generates a header dictionary within the 0..64 bound, including the empty case.
    /// </summary>
    private static Gen<IReadOnlyDictionary<string, string>> HeadersGen()
    {
        var entryGen = from key in Gen.Choose(0, 1000).Select(i => $"header-{i}")
                       from value in Gen.Choose(0, 1000).Select(i => $"value-{i}")
                       select new KeyValuePair<string, string>(key, value);

        return from count in Gen.Choose(0, MessageEnvelope.MaxHeaderCount)
               from entries in Gen.ArrayOf(entryGen, count)
               select (IReadOnlyDictionary<string, string>)entries
                   .GroupBy(e => e.Key)
                   .ToDictionary(g => g.Key, g => g.Last().Value);
    }
}
