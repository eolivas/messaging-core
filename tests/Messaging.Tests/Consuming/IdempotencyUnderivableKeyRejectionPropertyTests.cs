using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Configuration;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Tests.Fakes;

namespace Messaging.Tests.Consuming;

// Feature: messaging-core, Property 18: An underivable idempotency key rejects before side effects.
// Validates: Requirements 7.6.
//
// For any received envelope whose idempotency key CANNOT be derived (no stable identity — an empty
// MessageId or a blank MessageType), when idempotency is ENABLED the consume pipeline must:
//   REJECT        — delivery throws MissingIdempotencyKeyException (the missing-key error, Req 7.6),
//   NO SIDE EFFECTS — the handler/invoker is NEVER called (a CountingHandlerInvoker records zero
//                     invocations), and
//   NO STORE READ — the duplicate-store lookup never runs, proving the rejection happens before the
//                   gate consults any state (the rejection is strictly ahead of side effects).
//
// The guarantee is exercised end-to-end through ConsumePipeline.DeliverAsync (host started to
// Running) with a real DefaultIdempotencyKeyProvider and a real IdempotencyGate over an enabled
// IdempotencyConfig, so the production derivation + gate ordering is what is under test.
//
// Generator discipline: every generated envelope is guaranteed underivable by construction — one of
// two underivable shapes (empty MessageId, or whitespace/empty MessageType) — while all other fields
// stay valid, so the ONLY reason to reject is the underivable key.
[Trait("Feature", "messaging-core")]
[Trait("Property", "18")]
public class IdempotencyUnderivableKeyRejectionPropertyTests
{
    /// <summary>An envelope whose idempotency key cannot be derived, plus a label for diagnostics.</summary>
    public sealed record UnderivableEnvelope(MessageEnvelope Envelope, string Reason);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(IdempotencyUnderivableKeyRejectionPropertyTests) })]
    public Property UnderivableKey_WhenEnabled_RejectsBeforeSideEffects(UnderivableEnvelope testCase)
    {
        // Arrange: a running pipeline with idempotency ENABLED, a real key provider + gate, a counting
        // invoker (any invocation is a side effect), and a store that records whether it was consulted.
        var invoker = new CountingHandlerInvoker();
        var store = new FakeIdempotencyStore();
        var gate = new IdempotencyGate(
            new DefaultIdempotencyKeyProvider(),
            store,
            new IdempotencyConfig { Enabled = true });

        var pipeline = new ConsumePipeline(
            new ConsumerHostStateMachine(),
            new LifecycleStubContractBinder(),
            invoker,
            gate);

        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        // Act: deliver the underivable-key envelope; it must reject with the missing-key error.
        var rejectedWithMissingKey = false;
        try
        {
            pipeline.DeliverAsync(testCase.Envelope, CancellationToken.None).GetAwaiter().GetResult();
        }
        catch (MissingIdempotencyKeyException)
        {
            rejectedWithMissingKey = true;
        }
        catch
        {
            // Any other exception type is a failure of the property (wrong rejection reason).
        }

        // Assert: rejected with the missing-key error, handler never invoked, store never consulted.
        var handlerNeverInvoked = invoker.InvocationCount == 0;
        var storeNeverConsulted = store.Lookups.Count == 0;

        return (rejectedWithMissingKey && handlerNeverInvoked && storeNeverConsulted)
            .ToProperty()
            .Label(
                $"reason={testCase.Reason} => rejected={rejectedWithMissingKey}, " +
                $"handlerInvocations={invoker.InvocationCount}, storeLookups={store.Lookups.Count}")
            .Classify(testCase.Reason == "empty-message-id", "empty MessageId")
            .Classify(testCase.Reason == "blank-message-type", "blank MessageType");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<UnderivableEnvelope> UnderivableEnvelopeArbitrary() =>
        Arb.From(UnderivableEnvelopeGen());

    /// <summary>
    /// Produces an envelope that is underivable by construction: either its MessageId is empty, or its
    /// MessageType is blank. Every other field is valid so the sole reason to reject is the missing
    /// idempotency key.
    /// </summary>
    private static Gen<UnderivableEnvelope> UnderivableEnvelopeGen()
    {
        var correlationIdGen = Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
            .SelectMany(length => Gen.ArrayOf(Gen.Elements("abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray()), length))
            .Select(chars => new string(chars));

        var timestampGen = from offsetSeconds in Gen.Choose(-315_360_000, 315_360_000)
                           select new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(offsetSeconds);

        var byteGen = Gen.Choose(0, 255).Select(b => (byte)b);
        var payloadGen = Gen.Choose(0, 32)
            .SelectMany(len => Gen.ArrayOf(byteGen, len))
            .Select(bytes => new ReadOnlyMemory<byte>(bytes));

        // Two underivable shapes. For empty-message-type we draw from a set of blank/whitespace values
        // that string.IsNullOrWhiteSpace rejects; a non-empty valid type is paired with Guid.Empty.
        var nonBlankTypeGen = NonBlankMessageTypeGen();
        var blankTypeGen = Gen.Elements("", " ", "   ", "\t", "\n", " \t \n ");

        var emptyIdCaseGen =
            from correlationId in correlationIdGen
            from messageType in nonBlankTypeGen
            from createdAtUtc in timestampGen
            from payload in payloadGen
            select new UnderivableEnvelope(
                BuildEnvelope(Guid.Empty, messageType, correlationId, createdAtUtc, payload),
                "empty-message-id");

        var blankTypeCaseGen =
            from correlationId in correlationIdGen
            from messageType in blankTypeGen
            from createdAtUtc in timestampGen
            from payload in payloadGen
            select new UnderivableEnvelope(
                BuildEnvelope(Guid.NewGuid(), messageType, correlationId, createdAtUtc, payload),
                "blank-message-type");

        return Gen.OneOf(emptyIdCaseGen, blankTypeCaseGen);
    }

    private static MessageEnvelope BuildEnvelope(
        Guid messageId,
        string messageType,
        string correlationId,
        DateTimeOffset createdAtUtc,
        ReadOnlyMemory<byte> payload) => new()
    {
        MessageId = messageId,
        CorrelationId = correlationId,
        MessageType = messageType,
        CreatedAtUtc = createdAtUtc,
        Headers = new Dictionary<string, string>(),
        Payload = payload,
        ContractVersion = 1,
    };

    private static Gen<string> NonBlankMessageTypeGen()
    {
        var tokenCharGen = Gen.Elements("abcdefghijklmnopqrstuvwxyz.-_0123456789".ToCharArray());
        return (from length in Gen.Choose(1, 24)
                from chars in Gen.ArrayOf(tokenCharGen, length)
                select new string(chars))
            .Where(messageType => !string.IsNullOrWhiteSpace(messageType));
    }
}
