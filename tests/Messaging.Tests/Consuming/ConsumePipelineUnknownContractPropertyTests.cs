using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;

namespace Messaging.Tests.Consuming;

// Feature: messaging-core, Property 8: Unknown contracts are rejected before the consumer.
// Validates: Requirements 4.6.
//
// For any MessageEnvelope whose MessageType is NOT in the registered contract set, the consume
// pipeline must:
//   REJECT         — binding/delivery throws UnknownContractException (the unknown-contract error),
//                    carrying the offending MessageType (Req 4.6), and
//   NEVER CONSUME  — the registered handler is never invoked, so the untrusted payload never
//                    reaches the consumer. A CountingHandlerInvoker records zero invocations.
//
// The guarantee is exercised end-to-end through ConsumePipeline.DeliverAsync (the host is started to
// Running first, so the "before the consumer" ordering is the real pipeline ordering) AND directly
// against JsonContractBinder.Bind (the binding seam the pipeline relies on). Both must agree.
//
// Generator discipline: the registered set is a fixed, non-empty universe of known message types;
// each generated case produces a MessageType that is guaranteed to differ from every registered one
// (an unknown contract). All other envelope fields are generated valid so the only reason to reject
// is the unknown message type.
[Trait("Feature", "messaging-core")]
[Trait("Property", "8")]
public class ConsumePipelineUnknownContractPropertyTests
{
    /// <summary>The closed universe of KNOWN message types; anything outside this set is unknown.</summary>
    private static readonly IReadOnlyList<string> KnownMessageTypes = new[]
    {
        "orders.created",
        "orders.updated",
        "payments.captured",
        "inventory.reserved",
    };

    /// <summary>
    /// An otherwise-valid envelope whose <see cref="MessageEnvelope.MessageType"/> is NOT registered,
    /// paired with the unknown type for counterexample labeling.
    /// </summary>
    public sealed record UnknownContractEnvelope(MessageEnvelope Envelope, string UnknownMessageType);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(ConsumePipelineUnknownContractPropertyTests) })]
    public Property UnknownContract_IsRejectedBeforeConsumer_AndHandlerNeverInvoked(UnknownContractEnvelope testCase)
    {
        // Arrange: a running consume pipeline over a binder that knows only the registered contracts,
        // plus a counting handler invoker that records any (illegitimate) invocation.
        var binder = BuildBinder();
        var invoker = new CountingHandlerInvoker();
        var pipeline = new ConsumePipeline(new ConsumerHostStateMachine(), binder, invoker);

        // Drive the host to Running so delivery is accepted and the full ordering is exercised.
        pipeline.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        // Act 1 (end-to-end): deliver through the pipeline; it must reject before invoking the handler.
        var deliveryThrewUnknownContract = ThrowsUnknownContract(
            () => pipeline.DeliverAsync(testCase.Envelope, CancellationToken.None).GetAwaiter().GetResult(),
            testCase.UnknownMessageType);

        // Act 2 (binding seam): the same rejection holds directly against the binder.
        var bindThrewUnknownContract = ThrowsUnknownContract(
            () => binder.Bind(testCase.Envelope),
            testCase.UnknownMessageType);

        // Assert: both reject with UnknownContractException AND the consumer was never reached.
        var rejected = deliveryThrewUnknownContract && bindThrewUnknownContract;
        var handlerNeverInvoked = invoker.InvocationCount == 0;
        var reportsUnknownAsNotKnown = !binder.IsKnown(testCase.UnknownMessageType);

        return (rejected && handlerNeverInvoked && reportsUnknownAsNotKnown)
            .ToProperty()
            .Label(
                $"unknownMessageType='{testCase.UnknownMessageType}' => " +
                $"deliveryRejected={deliveryThrewUnknownContract}, bindRejected={bindThrewUnknownContract}, " +
                $"handlerInvocations={invoker.InvocationCount}, isKnown={binder.IsKnown(testCase.UnknownMessageType)}");
    }

    /// <summary>
    /// Runs <paramref name="action"/> and returns true only when it throws
    /// <see cref="UnknownContractException"/> whose <see cref="UnknownContractException.MessageType"/>
    /// matches the offending type. Any other outcome (no throw, or a different exception) is a failure
    /// of the property.
    /// </summary>
    private static bool ThrowsUnknownContract(Action action, string expectedMessageType)
    {
        try
        {
            action();
            return false;
        }
        catch (UnknownContractException ex)
        {
            return string.Equals(ex.MessageType, expectedMessageType, StringComparison.Ordinal);
        }
        catch
        {
            return false;
        }
    }

    private static IContractBinder BuildBinder()
    {
        var registrations = KnownMessageTypes
            .Select(messageType => new ContractRegistration(messageType, typeof(OrderCreatedContract)));
        return new JsonContractBinder(registrations);
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<UnknownContractEnvelope> UnknownContractEnvelopeArbitrary() =>
        Arb.From(UnknownContractEnvelopeGen());

    /// <summary>
    /// Produces an envelope valid in every respect except that its message type is guaranteed to be
    /// outside <see cref="KnownMessageTypes"/>. The unknown type is a random non-empty token filtered
    /// to never collide with a registered type, so the only reason to reject is the unknown contract.
    /// </summary>
    private static Gen<UnknownContractEnvelope> UnknownContractEnvelopeGen()
    {
        var messageIdGen = Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty);

        var correlationIdGen = Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
            .Select(length => new string('c', length));

        var timestampGen = from offsetSeconds in Gen.Choose(-315_360_000, 315_360_000) // +/- ~10 years
                           select new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(offsetSeconds);

        var contractVersionGen = Gen.Choose(1, 5);

        var byteGen = Gen.Choose(0, 255).Select(b => (byte)b);
        var payloadGen = Gen.Choose(0, 32)
            .SelectMany(len => Gen.ArrayOf(byteGen, len))
            .Select(bytes => new ReadOnlyMemory<byte>(bytes));

        var headersGen = HeadersGen();
        var unknownTypeGen = UnknownMessageTypeGen();

        return from messageId in messageIdGen
               from correlationId in correlationIdGen
               from unknownType in unknownTypeGen
               from createdAtUtc in timestampGen
               from headers in headersGen
               from payload in payloadGen
               from contractVersion in contractVersionGen
               select new UnknownContractEnvelope(
                   new MessageEnvelope
                   {
                       MessageId = messageId,
                       CorrelationId = correlationId,
                       MessageType = unknownType,
                       CreatedAtUtc = createdAtUtc,
                       Headers = headers,
                       Payload = payload,
                       ContractVersion = contractVersion,
                   },
                   unknownType);
    }

    /// <summary>
    /// Generates non-empty message types that are guaranteed to differ from every registered type.
    /// Mixes random tokens, deliberate near-misses of known types, and prefixed/suffixed variants to
    /// probe the boundary of the known set — all filtered so a collision can never slip through.
    /// </summary>
    private static Gen<string> UnknownMessageTypeGen()
    {
        var tokenCharGen = Gen.Elements("abcdefghijklmnopqrstuvwxyz.-_0123456789".ToCharArray());
        var randomTokenGen = from length in Gen.Choose(1, 24)
                             from chars in Gen.ArrayOf(tokenCharGen, length)
                             select new string(chars);

        // Near-misses of real known types (casing / affixes) to pressure the Ordinal exact-match rule.
        var nearMissGen = from known in Gen.Elements(KnownMessageTypes.ToArray())
                          from mutate in Gen.Elements<Func<string, string>>(
                              s => s.ToUpperInvariant(),
                              s => s + ".v2",
                              s => "x-" + s,
                              s => s + " ",
                              s => s.Replace('.', '_'))
                          select mutate(known);

        return Gen.OneOf(randomTokenGen, nearMissGen)
            .Where(messageType =>
                !string.IsNullOrWhiteSpace(messageType)
                && !KnownMessageTypes.Contains(messageType, StringComparer.Ordinal));
    }

    /// <summary>Generates a header dictionary within the 0..MaxHeaderCount bound, including empty.</summary>
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
