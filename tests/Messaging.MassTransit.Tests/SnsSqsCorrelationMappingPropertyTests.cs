using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Contracts;
using Messaging.MassTransit.AmazonSqs;

namespace Messaging.MassTransit.Tests;

// Validates: Requirements 7.1.
//
// CorrelationGuid.From is deterministic, returns the parsed Guid for a Guid-formatted id, and the first
// 16 bytes of the SHA-256 of the UTF-8 text otherwise. ToWire carries the original correlation id
// unchanged in the x-correlation-id attribute (the publish path supplies it through the trace headers)
// and the derived Guid in SnsSqsWireMessage.CorrelationGuid.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "5")]
public class SnsSqsCorrelationMappingPropertyTests
{
    // Feature: sns-sqs-provider, Property 5: Correlation id mapping
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsCorrelationMappingPropertyTests) })]
    public Property From_ForAnyCorrelationId_IsDeterministicAndMatchesTheDocumentedMapping(CorrelationId id)
    {
        var first = CorrelationGuid.From(id.Value);
        var second = CorrelationGuid.From(id.Value);

        var expected = Guid.TryParse(id.Value, out var parsed)
            ? parsed
            : new Guid(SHA256.HashData(Encoding.UTF8.GetBytes(id.Value)).AsSpan(0, 16));

        return (first == second && first == expected)
            .ToProperty()
            .Classify(id.IsGuid, "guid formatted")
            .Classify(!id.IsGuid, "non-guid");
    }

    // Feature: sns-sqs-provider, Property 5: Correlation id mapping
    [Property(MaxTest = 100, QuietOnSuccess = true)]
    public Property From_ForAGuidFormattedId_ReturnsThatGuid(Guid guid, bool upper)
    {
        var text = guid.ToString(upper ? "D" : "N");
        if (upper)
        {
            text = text.ToUpperInvariant();
        }

        return (CorrelationGuid.From(text) == guid).ToProperty();
    }

    // Feature: sns-sqs-provider, Property 5: Correlation id mapping
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsCorrelationMappingPropertyTests) })]
    public Property ToWire_ForAnyEnvelope_CarriesTheOriginalCorrelationIdUnchanged(CorrelationId id)
    {
        var envelope = new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = id.Value,
            MessageType = "order.created",
            CreatedAtUtc = DateTimeOffset.UnixEpoch,
            Headers = new Dictionary<string, string>(StringComparer.Ordinal),
            Payload = new byte[] { 1, 2, 3 },
            ContractVersion = 1,
        };
        var trace = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SnsSqsHeaderMapper.CorrelationIdAttribute] = id.Value,
        };

        var wire = SnsSqsWireFormat.ToWire(envelope, trace);

        var attributeCarriesId =
            wire.AttributeHeaders.TryGetValue(SnsSqsHeaderMapper.CorrelationIdAttribute, out var attribute)
            && string.Equals(attribute, id.Value, StringComparison.Ordinal);
        var bodyCarriesId = string.Equals(wire.Body.CorrelationId, id.Value, StringComparison.Ordinal);

        return (attributeCarriesId
                && bodyCarriesId
                && wire.CorrelationGuid == CorrelationGuid.From(id.Value))
            .ToProperty();
    }

    /// <summary>Wrapper so FsCheck resolves a dedicated generator for correlation ids.</summary>
    public sealed record CorrelationId(string Value, bool IsGuid);

    public static Arbitrary<CorrelationId> CorrelationIdArbitrary() => Arb.From(CorrelationIdGen());

    private static Gen<CorrelationId> CorrelationIdGen() => Gen.OneOf(
        from bytes in Gen.ArrayOf(Gen.Choose(0, 255), 16)
        from format in Gen.Elements("D", "N", "B", "P")
        select new CorrelationId(new Guid(bytes.Select(b => (byte)b).ToArray()).ToString(format), true),
        from length in Gen.Choose(1, MessageEnvelope.MaxCorrelationIdLength)
        from chars in Gen.ArrayOf(TextChar(), length)
        let text = new string(chars)
        where !Guid.TryParse(text, out _)
        select new CorrelationId(text, false));

    private static Gen<char> TextChar() => Gen.OneOf(
        Gen.Elements("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:/".ToCharArray()),
        Gen.Elements("éñüßΩλЖ日本語한".ToCharArray()));
}
