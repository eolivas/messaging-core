using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.MassTransit.AmazonSqs;

namespace Messaging.MassTransit.Tests;

// Validates: Requirements 6.6, 6.7.
//
// The header split never exceeds the 10-attribute budget, places every header in exactly one
// partition (attribute or body) without loss, and yields the documented allow-list.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "4")]
public class SnsSqsAttributeBudgetPropertyTests
{
    private static readonly string[] ReservedKeys =
    {
        SnsSqsHeaderMapper.CorrelationIdAttribute,
        SnsSqsHeaderMapper.TraceParentAttribute,
        SnsSqsHeaderMapper.MessageTypeAttribute,
    };

    private static readonly Regex EligibleKey = new("^[A-Za-z0-9_-]{1,200}$", RegexOptions.CultureInvariant);

    // Feature: sns-sqs-provider, Property 4: Message attribute budget and header partition
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsAttributeBudgetPropertyTests) })]
    public Property Split_AnyHeaders_RespectsBudgetAndPartitionsWithoutLoss(
        HeaderSet input,
        TraceSet trace)
    {
        var headers = input.Values;
        var split = SnsSqsHeaderMapper.Split(headers, trace.Values);

        // x-message-type is added by the wire format, so it counts toward the budget.
        var totalAttributes = split.ReservedAttributes.Count + 1 + split.AttributeHeaders.Count;
        var withinBudget =
            totalAttributes <= SnsSqsHeaderMapper.MaxAttributes
            && split.AttributeHeaders.Count <= SnsSqsHeaderMapper.MaxUserAttributes;

        // Expected selection, computed independently: first 7 eligible keys in ordinal order.
        var expectedKeys = headers
            .Where(h => !string.IsNullOrEmpty(h.Value)
                && h.Value.Length <= SnsSqsHeaderMapper.MaxValueLength
                && EligibleKey.IsMatch(h.Key)
                && !ReservedKeys.Contains(h.Key))
            .Select(h => h.Key)
            .OrderBy(k => k, StringComparer.Ordinal)
            .Take(SnsSqsHeaderMapper.MaxUserAttributes)
            .ToList();

        var selection =
            split.AttributeHeaders.Keys
                .OrderBy(k => k, StringComparer.Ordinal)
                .SequenceEqual(
                    expectedKeys.Select(k => SnsSqsHeaderMapper.UserHeaderPrefix + k)
                        .OrderBy(k => k, StringComparer.Ordinal));

        // Every header lands in exactly one place, with its value intact.
        var noLoss = headers.All(h =>
        {
            var inAttribute = split.AttributeHeaders.TryGetValue(
                SnsSqsHeaderMapper.UserHeaderPrefix + h.Key, out var attributeValue);
            var inBody = split.BodyHeaders.TryGetValue(h.Key, out var bodyValue);
            return inAttribute != inBody
                && (!inAttribute || string.Equals(attributeValue, h.Value, StringComparison.Ordinal))
                && (!inBody || string.Equals(bodyValue, h.Value, StringComparison.Ordinal));
        });

        var noInvention =
            split.AttributeHeaders.Count + split.BodyHeaders.Count == headers.Count
            && split.AttributeHeaders.Keys.All(k =>
                k.StartsWith(SnsSqsHeaderMapper.UserHeaderPrefix, StringComparison.Ordinal));

        var expectedAllowList = ReservedKeys.Concat(split.AttributeHeaders.Keys).ToHashSet(StringComparer.Ordinal);
        var allowList = split.AllowedTransportHeaders.SetEquals(expectedAllowList);

        return (withinBudget && selection && noLoss && noInvention && allowList)
            .ToProperty()
            .Classify(headers.Count == 0, "no headers")
            .Classify(expectedKeys.Count == SnsSqsHeaderMapper.MaxUserAttributes, "attribute budget full")
            .Classify(headers.Keys.Any(k => ReservedKeys.Contains(k)), "reserved key collision")
            .Label($"headers={headers.Count} attributes={split.AttributeHeaders.Count}");
    }

    /// <summary>Wrapper so FsCheck resolves a dedicated generator for envelope headers.</summary>
    public sealed record HeaderSet(IReadOnlyDictionary<string, string> Values);

    /// <summary>Wrapper so FsCheck resolves a dedicated generator for trace headers.</summary>
    public sealed record TraceSet(IReadOnlyDictionary<string, string> Values);

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<HeaderSet> HeaderSetArbitrary() => Arb.From(HeaderSetGen());

    public static Arbitrary<TraceSet> TraceSetArbitrary() => Arb.From(TraceSetGen());

    private static Gen<char> TokenChar() => Gen.Elements(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_".ToCharArray());

    private static Gen<string> StringOf(Gen<char> chars, int min, int max) =>
        from length in Gen.Choose(min, max)
        from items in Gen.ArrayOf(chars, length)
        select new string(items);

    // Mostly eligible keys, plus ineligible ones (invalid chars, empty, over 200 chars) and reserved keys.
    private static Gen<string> KeyGen() => Gen.Frequency(
        (6, StringOf(TokenChar(), 1, 12)),
        (1, StringOf(Gen.Elements(" .:/é日".ToCharArray()), 1, 6)),
        (1, StringOf(TokenChar(), 201, 220)),
        (1, Gen.Constant(string.Empty)),
        (1, Gen.Elements(ReservedKeys)));

    // Mostly eligible values, plus empty and over-1024 values.
    private static Gen<string> ValueGen() => Gen.Frequency(
        (6, StringOf(TokenChar(), 1, 16)),
        (1, Gen.Constant(string.Empty)),
        (1, StringOf(TokenChar(), 1025, 1050)));

    private static Gen<HeaderSet> HeaderSetGen() =>
        from count in Gen.Choose(0, 64)
        from keys in Gen.ArrayOf(KeyGen(), count)
        from values in Gen.ArrayOf(ValueGen(), count)
        select new HeaderSet(Enumerable
            .Range(0, count)
            .GroupBy(i => keys[i], StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => values[g.Last()], StringComparer.Ordinal));

    private static Gen<TraceSet> TraceSetGen() =>
        from withTrace in Gen.Elements(true, false)
        from withCorrelation in Gen.Elements(true, false)
        from trace in StringOf(TokenChar(), 1, 40)
        from correlation in StringOf(TokenChar(), 1, 40)
        select new TraceSet(new Dictionary<string, string>(StringComparer.Ordinal)
            .WithIf(withTrace, SnsSqsHeaderMapper.TraceParentAttribute, trace)
            .WithIf(withCorrelation, SnsSqsHeaderMapper.CorrelationIdAttribute, correlation));
}
