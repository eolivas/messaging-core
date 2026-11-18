using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Transport;
namespace Messaging.MassTransit.Tests;
// Validates: Requirements 6.8.
//
// The size guard throws MessageTooLargeException (carrying the measured size and the limit)
// if and only if the measured size exceeds the limit. Boundary: exactly at the limit passes,
// one byte over throws.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "6")]
public class SnsSqsMessageSizeGuardPropertyTests
{
    private const int MinLimit = 1;
    private const int MaxLimit = 262_144;
    // Feature: sns-sqs-provider, Property 6: Message size guard
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsMessageSizeGuardPropertyTests) })]
    public Property Check_AnyInput_ThrowsIffMeasuredSizeExceedsLimit(GuardCase input)
    {
        // Independent measurement: serialized envelope + attributes + fixed allowance.
        var expected = JsonSerializer.SerializeToUtf8Bytes(input.Envelope).LongLength
            + 2048
            + input.Attributes.Sum(a =>
                Encoding.UTF8.GetByteCount(a.Key) + Encoding.UTF8.GetByteCount("String")
                + Encoding.UTF8.GetByteCount(a.Value));
        var limit = input.LimitFor(expected);
        MessageTooLargeException? thrown = null;
        try
        {
            SnsSqsMessageSizeGuard.Check(input.Envelope, input.Attributes, limit);
        }
        catch (MessageTooLargeException ex)
        {
            thrown = ex;
        }
        var ok = expected > limit
            ? thrown is not null && thrown.Actual == expected && thrown.Limit == limit
            : thrown is null;
        return ok.ToProperty()
            .Classify(expected > limit, "over limit")
            .Classify(expected == limit, "exactly at limit")
            .Classify(expected == limit + 1, "one over limit")
            .Label($"expected={expected} limit={limit} thrown={thrown?.Actual}");
    }
    /// <summary>Input case; <see cref="Offset"/> selects a limit relative to the measured size.</summary>
    public sealed record GuardCase(
        EnvelopeMessage Envelope,
        IReadOnlyDictionary<string, string> Attributes,
        int Mode,
        int RandomLimit)
    {
        // Mode 0: exactly at size, 1: one under size (throws), 2: random in bounds, 3: size + 1.
        public long LimitFor(long size) => Mode switch
        {
            0 => size,
            1 => size - 1,
            3 => size + 1,
            _ => RandomLimit,
        };
        public override string ToString() => $"payload={Envelope.Payload.Length} attrs={Attributes.Count} mode={Mode}";
    }
    public static Arbitrary<GuardCase> GuardCaseArbitrary() => Arb.From(GuardCaseGen());
    private static Gen<string> StringOf(string alphabet, int min, int max) =>
        from length in Gen.Choose(min, max)
        from items in Gen.ArrayOf(Gen.Elements(alphabet.ToCharArray()), length)
        select new string(items);
    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_é日";
    private static Gen<GuardCase> GuardCaseGen() =>
        from payloadSize in Gen.Frequency(
            (3, Gen.Choose(0, 64)),
            (2, Gen.Choose(0, 4096)),
            (1, Gen.Choose(150_000, 190_000)))
        from payload in Gen.ArrayOf(Gen.Choose(0, 255).Select(i => (byte)i), payloadSize)
        from correlation in StringOf(Alphabet, 1, 40)
        from type in StringOf(Alphabet, 1, 40)
        from attrCount in Gen.Choose(0, 10)
        from keys in Gen.ArrayOf(StringOf(Alphabet, 1, 20), attrCount)
        from values in Gen.ArrayOf(StringOf(Alphabet, 0, 40), attrCount)
        from mode in Gen.Choose(0, 3)
        from randomLimit in Gen.Choose(MinLimit, MaxLimit)
        select new GuardCase(
            new EnvelopeMessage
            {
                MessageId = Guid.NewGuid(),
                CorrelationId = correlation,
                MessageType = type,
                CreatedAtUtc = DateTimeOffset.UnixEpoch,
                Payload = payload,
            },
            Enumerable.Range(0, attrCount)
                .GroupBy(i => keys[i], StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => values[g.Last()], StringComparer.Ordinal),
            mode,
            randomLimit);
}
