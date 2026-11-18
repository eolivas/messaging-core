using System;
using System.Collections.Generic;
using System.Linq;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Contracts;
using Messaging.MassTransit.AmazonSqs;

namespace Messaging.MassTransit.Tests;

// Validates: Requirements 8.2, 8.3.
//
// FifoMessageIds.GroupId derives the group id from the correlation id (default) or the message type,
// keeps only printable ASCII (0x21 to 0x7E, everything else becomes '_'), caps it at 128 characters and
// is deterministic. DeduplicationId is the message id in D format. Distinct group-field values drawn
// from the allowed character set (and within the cap) yield distinct group ids.
[Trait("Feature", "sns-sqs-provider")]
[Trait("Property", "7")]
public class SnsSqsFifoMessageIdsPropertyTests
{
    private const string AllowedChars =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

    // Feature: sns-sqs-provider, Property 7: FIFO group and deduplication identifiers
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsFifoMessageIdsPropertyTests) })]
    public Property GroupId_ForAnyEnvelopeAndSelection_IsDerivedFromTheSelectedFieldWithAllowedCharsAndCap(
        GroupText correlation,
        GroupText messageType,
        bool selectMessageType)
    {
        var selection = selectMessageType ? FifoGroupSelection.MessageType : FifoGroupSelection.CorrelationId;
        var envelope = NewEnvelope(Guid.NewGuid(), correlation.Value, messageType.Value);
        var source = selectMessageType ? messageType.Value : correlation.Value;

        var groupId = FifoMessageIds.GroupId(envelope, selection);
        var again = FifoMessageIds.GroupId(envelope, selection);

        var expected = new string(
            source.Take(FifoMessageIds.GroupIdLimit)
                .Select(c => c is >= '!' and <= '~' ? c : '_')
                .ToArray());

        return (groupId.Length > 0
                && groupId.Length <= FifoMessageIds.GroupIdLimit
                && groupId.All(c => c is >= '!' and <= '~')
                && groupId == expected
                && groupId == again)
            .ToProperty()
            .Classify(source.Length > FifoMessageIds.GroupIdLimit, "truncated")
            .Classify(source.Any(c => c is < '!' or > '~'), "has disallowed chars");
    }

    // Feature: sns-sqs-provider, Property 7: FIFO group and deduplication identifiers
    [Property(MaxTest = 100, QuietOnSuccess = true)]
    public Property DeduplicationId_ForAnyMessageId_IsTheGuidInDFormat(Guid messageId)
    {
        var envelope = NewEnvelope(messageId, "corr", "order.created");

        return (FifoMessageIds.DeduplicationId(envelope) == messageId.ToString("D")).ToProperty();
    }

    // Feature: sns-sqs-provider, Property 7: FIFO group and deduplication identifiers
    [Property(MaxTest = 100, QuietOnSuccess = true, Arbitrary = new[] { typeof(SnsSqsFifoMessageIdsPropertyTests) })]
    public Property GroupId_ForDistinctAllowedGroupFieldValues_AreDistinct(
        AllowedText first,
        AllowedText second,
        bool selectMessageType)
    {
        var selection = selectMessageType ? FifoGroupSelection.MessageType : FifoGroupSelection.CorrelationId;
        var a = selectMessageType
            ? NewEnvelope(Guid.NewGuid(), "c", first.Value)
            : NewEnvelope(Guid.NewGuid(), first.Value, "t");
        var b = selectMessageType
            ? NewEnvelope(Guid.NewGuid(), "c", second.Value)
            : NewEnvelope(Guid.NewGuid(), second.Value, "t");

        var distinctInputs = !string.Equals(first.Value, second.Value, StringComparison.Ordinal);
        var distinctOutputs = FifoMessageIds.GroupId(a, selection) != FifoMessageIds.GroupId(b, selection);

        return (distinctOutputs == distinctInputs).ToProperty();
    }

    private static MessageEnvelope NewEnvelope(Guid messageId, string correlationId, string messageType) => new()
    {
        MessageId = messageId,
        CorrelationId = correlationId,
        MessageType = messageType,
        CreatedAtUtc = DateTimeOffset.UnixEpoch,
        Headers = new Dictionary<string, string>(StringComparer.Ordinal),
        Payload = new byte[] { 1 },
        ContractVersion = 1,
    };

    /// <summary>Non-empty text that may include disallowed characters and exceed the 128 cap.</summary>
    public sealed record GroupText(string Value);

    /// <summary>Non-empty text of allowed characters no longer than the 128 cap.</summary>
    public sealed record AllowedText(string Value);

    public static Arbitrary<GroupText> GroupTextArbitrary() => Arb.From(
        from length in Gen.Choose(1, 300)
        from chars in Gen.ArrayOf(
            Gen.Frequency(
                (6, Gen.Elements(AllowedChars.ToCharArray())),
                (2, Gen.Elements(" \t\n\u00e9\u00f1\u03a9\u65e5\u672c".ToCharArray()))),
            length)
        select new GroupText(new string(chars)));

    public static Arbitrary<AllowedText> AllowedTextArbitrary() => Arb.From(
        from length in Gen.Choose(1, FifoMessageIds.GroupIdLimit)
        from chars in Gen.ArrayOf(Gen.Elements(AllowedChars.ToCharArray()), length)
        select new AllowedText(new string(chars)));
}
