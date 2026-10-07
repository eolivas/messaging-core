using System;
using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Consuming;
using Messaging.Contracts;

namespace Messaging.Tests.Consuming;

// Feature: messaging-core, Property 15: Idempotency keys are deterministic.
// Validates: Requirements 7.1.
//
// For any logical message — defined by its stable identity (MessageId, MessageType, Payload) — the
// DefaultIdempotencyKeyProvider must derive the SAME key regardless of the transport/handling
// metadata that can legitimately differ between two deliveries of that same message (correlation id,
// headers, creation timestamp, contract version). Concretely, for each generated case the property
// asserts:
//   DERIVABLE   — both envelopes derive a key (TryDerive == true for the same-identity pair), and
//   IDENTICAL   — the two keys are equal (two envelopes for the same logical message => same key),
//   STABLE      — re-deriving from the very same envelope yields the identical key again.
//
// Generator discipline: a generated case produces ONE logical identity (id/type/payload) and TWO
// envelopes that share that identity but carry independently-generated metadata, so the only thing
// held constant is exactly what the key is supposed to depend on.
[Trait("Feature", "messaging-core")]
[Trait("Property", "15")]
public class IdempotencyKeyDeterminismPropertyTests
{
    /// <summary>Two envelopes that represent the same logical message (identical stable identity).</summary>
    public sealed record SameLogicalMessagePair(MessageEnvelope First, MessageEnvelope Second);

    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(IdempotencyKeyDeterminismPropertyTests) })]
    public Property IdempotencyKey_ForSameLogicalMessage_IsDeterministicAndStable(SameLogicalMessagePair pair)
    {
        var provider = new DefaultIdempotencyKeyProvider();

        // Derive from both envelopes of the pair.
        var firstDerived = provider.TryDerive(pair.First, out var firstKey);
        var secondDerived = provider.TryDerive(pair.Second, out var secondKey);

        // Stability: re-deriving from the same envelope must reproduce the same key.
        provider.TryDerive(pair.First, out var firstKeyAgain);

        var derivable = firstDerived && secondDerived;
        var identicalAcrossPair = string.Equals(firstKey, secondKey, StringComparison.Ordinal);
        var stableOnRederive = string.Equals(firstKey, firstKeyAgain, StringComparison.Ordinal);
        var nonEmpty = firstKey.Length > 0;

        return (derivable && identicalAcrossPair && stableOnRederive && nonEmpty)
            .ToProperty()
            .Label(
                $"derivable={derivable}, identical={identicalAcrossPair}, stable={stableOnRederive}, " +
                $"firstKey='{firstKey}', secondKey='{secondKey}'");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<SameLogicalMessagePair> SameLogicalMessagePairArbitrary() =>
        Arb.From(SameLogicalMessagePairGen());

    /// <summary>
    /// Builds one shared logical identity (MessageId, MessageType, Payload) then wraps it in two
    /// envelopes whose metadata — correlation id, headers, timestamp, contract version — is generated
    /// independently, so the pair differs in everything the key must ignore and agrees on everything
    /// the key must depend on.
    /// </summary>
    private static Gen<SameLogicalMessagePair> SameLogicalMessagePairGen()
    {
        var messageIdGen = Gen.Fresh(Guid.NewGuid).Where(id => id != Guid.Empty);
        var messageTypeGen = NonBlankMessageTypeGen();
        var payloadGen = PayloadGen();

        return from messageId in messageIdGen
               from messageType in messageTypeGen
               from payload in payloadGen
               from first in EnvelopeWithIdentityGen(messageId, messageType, payload)
               from second in EnvelopeWithIdentityGen(messageId, messageType, payload)
               select new SameLogicalMessagePair(first, second);
    }

    /// <summary>
    /// Produces an envelope pinned to the given stable identity but with freely-varying metadata, so
    /// two draws for the same identity differ only in fields the idempotency key must not consider.
    /// </summary>
    private static Gen<MessageEnvelope> EnvelopeWithIdentityGen(
        Guid messageId,
        string messageType,
        byte[] payload)
    {
        var correlationIdGen = Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
            .SelectMany(length => Gen.ArrayOf(Gen.Elements("abcdefghijklmnopqrstuvwxyz0123456789".ToCharArray()), length))
            .Select(chars => new string(chars));

        var timestampGen = from offsetSeconds in Gen.Choose(-315_360_000, 315_360_000) // +/- ~10 years
                           select new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(offsetSeconds);

        var contractVersionGen = Gen.Choose(1, 10);
        var headersGen = HeadersGen();

        return from correlationId in correlationIdGen
               from createdAtUtc in timestampGen
               from headers in headersGen
               from contractVersion in contractVersionGen
               select new MessageEnvelope
               {
                   MessageId = messageId,
                   CorrelationId = correlationId,
                   MessageType = messageType,
                   CreatedAtUtc = createdAtUtc,
                   Headers = headers,
                   // Fresh copy of the same bytes so the two envelopes never share a buffer reference;
                   // the key must depend on the byte CONTENT, not on identity of the array.
                   Payload = new ReadOnlyMemory<byte>(payload.ToArray()),
                   ContractVersion = contractVersion,
               };
    }

    /// <summary>Generates a non-blank message type (so the identity is always derivable).</summary>
    private static Gen<string> NonBlankMessageTypeGen()
    {
        var tokenCharGen = Gen.Elements("abcdefghijklmnopqrstuvwxyz.-_0123456789".ToCharArray());
        return (from length in Gen.Choose(1, 32)
                from chars in Gen.ArrayOf(tokenCharGen, length)
                select new string(chars))
            .Where(messageType => !string.IsNullOrWhiteSpace(messageType));
    }

    /// <summary>Generates a payload of 0..64 bytes, including the empty payload.</summary>
    private static Gen<byte[]> PayloadGen()
    {
        var byteGen = Gen.Choose(0, 255).Select(b => (byte)b);
        return from length in Gen.Choose(0, 64)
               from bytes in Gen.ArrayOf(byteGen, length)
               select bytes;
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
