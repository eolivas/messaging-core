using System.Text;
using Messaging.Contracts;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Transport;
using Xunit;

namespace Messaging.MassTransit.Tests;

// Validates: Requirements 6.6, 6.7, 6.8, 17.3, 17.8.
[Trait("Feature", "sns-sqs-provider")]
public class SnsSqsWireLayerTests
{
    private static readonly IReadOnlyDictionary<string, string> NoTrace =
        new Dictionary<string, string>();

    private static readonly IReadOnlyDictionary<string, string> FullTrace =
        new Dictionary<string, string>
        {
            ["x-correlation-id"] = "corr-1",
            ["traceparent"] = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        };

    private static MessageEnvelope EnvelopeWith(
        IDictionary<string, string> headers,
        byte[]? payload = null,
        string correlationId = "corr-1") => new()
    {
        MessageId = Guid.NewGuid(),
        CorrelationId = correlationId,
        MessageType = "test.contract",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(headers),
        Payload = new ReadOnlyMemory<byte>(payload ?? new byte[] { 1, 2, 3 }),
    };

    private static Dictionary<string, string> Headers(int count) =>
        Enumerable.Range(0, count).ToDictionary(i => $"k{i:D2}", i => $"v{i}");

    private static IEnumerable<KeyValuePair<string, object>> AsConsumeHeaders(
        IReadOnlyDictionary<string, string> attributes) =>
        attributes.Select(a => new KeyValuePair<string, object>(a.Key, a.Value));

    private static MessageEnvelope RoundTrip(MessageEnvelope envelope, out SnsSqsWireMessage wire)
    {
        wire = SnsSqsWireFormat.ToWire(envelope, FullTrace);
        return SnsSqsWireFormat.FromWire(wire.Body, AsConsumeHeaders(wire.AttributeHeaders));
    }

    private static void AssertSame(MessageEnvelope expected, MessageEnvelope actual)
    {
        Assert.Equal(expected.MessageId, actual.MessageId);
        Assert.Equal(expected.CorrelationId, actual.CorrelationId);
        Assert.Equal(expected.MessageType, actual.MessageType);
        Assert.Equal(expected.CreatedAtUtc, actual.CreatedAtUtc);
        Assert.Equal(expected.Payload.ToArray(), actual.Payload.ToArray());
        Assert.Equal(
            expected.Headers.OrderBy(h => h.Key, StringComparer.Ordinal),
            actual.Headers.OrderBy(h => h.Key, StringComparer.Ordinal));
    }

    [Fact]
    public void Split_EmptyHeaders_ProducesOnlyReservedAllowList()
    {
        var split = SnsSqsHeaderMapper.Split(new Dictionary<string, string>(), NoTrace);

        Assert.Empty(split.ReservedAttributes);
        Assert.Empty(split.AttributeHeaders);
        Assert.Empty(split.BodyHeaders);
        Assert.Equal(
            new[] { "traceparent", "x-correlation-id", "x-message-type" },
            split.AllowedTransportHeaders.OrderBy(k => k, StringComparer.Ordinal));
    }

    [Fact]
    public void ToWire_EmptyHeaders_RoundTripsWithThreeAttributes()
    {
        var envelope = EnvelopeWith(new Dictionary<string, string>());

        var restored = RoundTrip(envelope, out var wire);

        Assert.Equal(3, wire.AttributeHeaders.Count);
        Assert.Empty(wire.Body.Headers);
        AssertSame(envelope, restored);
    }

    [Fact]
    public void ToWire_SevenUserHeaders_FillTenAttributesAndLeaveBodyEmpty()
    {
        var envelope = EnvelopeWith(Headers(SnsSqsHeaderMapper.MaxUserAttributes));

        var restored = RoundTrip(envelope, out var wire);

        Assert.Equal(SnsSqsHeaderMapper.MaxAttributes, wire.AttributeHeaders.Count);
        Assert.Empty(wire.Body.Headers);
        AssertSame(envelope, restored);
    }

    [Fact]
    public void Split_MoreThanSevenHeaders_KeepsFirstSevenByOrdinalKeyAndRestInBody()
    {
        var headers = Headers(12);

        var split = SnsSqsHeaderMapper.Split(headers, FullTrace);

        Assert.Equal(
            Enumerable.Range(0, 7).Select(i => $"h.k{i:D2}"),
            split.AttributeHeaders.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal(
            Enumerable.Range(7, 5).Select(i => $"k{i:D2}"),
            split.BodyHeaders.Keys.OrderBy(k => k, StringComparer.Ordinal));
    }

    [Fact]
    public void ToWire_MoreThanTenHeaders_NeverExceedsTenAttributesAndRestoresAllHeaders()
    {
        var envelope = EnvelopeWith(Headers(64));

        var restored = RoundTrip(envelope, out var wire);

        Assert.Equal(SnsSqsHeaderMapper.MaxAttributes, wire.AttributeHeaders.Count);
        Assert.Equal(64 - SnsSqsHeaderMapper.MaxUserAttributes, wire.Body.Headers.Count);
        AssertSame(envelope, restored);
    }

    [Fact]
    public void ToWire_HeaderCollidingWithReservedKey_StaysInBodyAndIsRestored()
    {
        var headers = new Dictionary<string, string>
        {
            ["x-correlation-id"] = "spoofed",
            ["traceparent"] = "spoofed-trace",
            ["x-message-type"] = "spoofed-type",
            ["tenant"] = "acme",
        };
        var envelope = EnvelopeWith(headers);

        var restored = RoundTrip(envelope, out var wire);

        Assert.Equal("corr-1", wire.AttributeHeaders["x-correlation-id"]);
        Assert.Equal("test.contract", wire.AttributeHeaders["x-message-type"]);
        Assert.DoesNotContain("h.x-correlation-id", wire.AttributeHeaders.Keys);
        Assert.Equal("spoofed", wire.Body.Headers["x-correlation-id"]);
        Assert.Equal("spoofed-trace", wire.Body.Headers["traceparent"]);
        Assert.Equal("spoofed-type", wire.Body.Headers["x-message-type"]);
        Assert.Equal("acme", wire.AttributeHeaders["h.tenant"]);
        AssertSame(envelope, restored);
        Assert.Equal("corr-1", restored.CorrelationId);
    }

    [Theory]
    [InlineData("bad key", "v")]
    [InlineData("k", "")]
    public void Split_IneligibleHeader_StaysInBody(string key, string value)
    {
        var split = SnsSqsHeaderMapper.Split(new Dictionary<string, string> { [key] = value }, NoTrace);

        Assert.Empty(split.AttributeHeaders);
        Assert.Equal(value, split.BodyHeaders[key]);
    }

    [Fact]
    public void Split_ValueLongerThanLimit_StaysInBodyAndValueAtLimitIsAttribute()
    {
        var headers = new Dictionary<string, string>
        {
            ["atlimit"] = new string('a', SnsSqsHeaderMapper.MaxValueLength),
            ["over"] = new string('b', SnsSqsHeaderMapper.MaxValueLength + 1),
        };

        var split = SnsSqsHeaderMapper.Split(headers, NoTrace);

        Assert.Contains("h.atlimit", split.AttributeHeaders.Keys);
        Assert.Contains("over", split.BodyHeaders.Keys);
    }

    [Fact]
    public void ToWire_NonAsciiPayloadAndHeaderValues_RoundTripExactly()
    {
        var payload = Encoding.UTF8.GetBytes("héllo wörld 日本語 🎉");
        var envelope = EnvelopeWith(
            new Dictionary<string, string> { ["city"] = "Zürich 東京", ["note"] = "ñandú" },
            payload,
            correlationId: "corr-é日");

        var restored = RoundTrip(envelope, out _);

        AssertSame(envelope, restored);
        Assert.Equal("héllo wörld 日本語 🎉", Encoding.UTF8.GetString(restored.Payload.Span));
    }

    [Fact]
    public void ToWire_EmptyPayload_RoundTrips()
    {
        var envelope = EnvelopeWith(new Dictionary<string, string>(), Array.Empty<byte>());

        var restored = RoundTrip(envelope, out _);

        AssertSame(envelope, restored);
    }

    private static long MeasuredSize(EnvelopeMessage body, IReadOnlyDictionary<string, string> attributes)
    {
        var ex = Assert.Throws<MessageTooLargeException>(
            () => SnsSqsMessageSizeGuard.Check(body, attributes, limit: 0));
        return ex.Actual;
    }

    [Fact]
    public void Check_SizeExactlyAtLimit_DoesNotThrow()
    {
        var wire = SnsSqsWireFormat.ToWire(EnvelopeWith(Headers(3)), FullTrace);
        var size = MeasuredSize(wire.Body, wire.AttributeHeaders);

        var ex = Record.Exception(
            () => SnsSqsMessageSizeGuard.Check(wire.Body, wire.AttributeHeaders, size));

        Assert.Null(ex);
    }

    [Fact]
    public void Check_SizeOneByteOverLimit_ThrowsWithActualAndLimit()
    {
        var wire = SnsSqsWireFormat.ToWire(EnvelopeWith(Headers(3)), FullTrace);
        var size = MeasuredSize(wire.Body, wire.AttributeHeaders);

        var ex = Assert.Throws<MessageTooLargeException>(
            () => SnsSqsMessageSizeGuard.Check(wire.Body, wire.AttributeHeaders, size - 1));

        Assert.Equal(size, ex.Actual);
        Assert.Equal(size - 1, ex.Limit);
    }

    [Fact]
    public void Check_EmptyAttributes_CountsAllowanceInSize()
    {
        var body = SnsSqsWireFormat.ToWire(EnvelopeWith(new Dictionary<string, string>()), NoTrace).Body;

        var size = MeasuredSize(body, new Dictionary<string, string>());

        Assert.True(size > SnsSqsMessageSizeGuard.EnvelopeAllowanceBytes);
    }

    [Fact]
    public void Check_ExceptionMessage_DoesNotContainPayload()
    {
        var wire = SnsSqsWireFormat.ToWire(
            EnvelopeWith(new Dictionary<string, string>(), Encoding.UTF8.GetBytes("SECRET-PAYLOAD")),
            FullTrace);

        var ex = Assert.Throws<MessageTooLargeException>(
            () => SnsSqsMessageSizeGuard.Check(wire.Body, wire.AttributeHeaders, 1));

        Assert.DoesNotContain("SECRET", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CorrelationGuid_ParsableGuid_ReturnsIt()
    {
        var guid = Guid.NewGuid();

        Assert.Equal(guid, CorrelationGuid.From(guid.ToString("N")));
    }

    [Fact]
    public void CorrelationGuid_NonGuid_IsDeterministicAndDistinct()
    {
        Assert.Equal(CorrelationGuid.From("order-42"), CorrelationGuid.From("order-42"));
        Assert.NotEqual(CorrelationGuid.From("order-42"), CorrelationGuid.From("order-43"));
    }

    [Fact]
    public void FifoMessageIds_GroupId_SanitizesAndCapsAt128()
    {
        var envelope = EnvelopeWith(new Dictionary<string, string>(), correlationId: "a b é" + new string('x', 200));

        var group = FifoMessageIds.GroupId(envelope, FifoGroupSelection.CorrelationId);

        Assert.Equal(FifoMessageIds.GroupIdLimit, group.Length);
        Assert.StartsWith("a_b__x", group, StringComparison.Ordinal);
    }

    [Fact]
    public void FifoMessageIds_MessageTypeSelectionAndDeduplicationId_UseExpectedFields()
    {
        var envelope = EnvelopeWith(new Dictionary<string, string>());

        Assert.Equal("test.contract", FifoMessageIds.GroupId(envelope, FifoGroupSelection.MessageType));
        Assert.Equal(envelope.MessageId.ToString("D"), FifoMessageIds.DeduplicationId(envelope));
    }

    [Fact]
    public void ToWire_Fifo_SetsIdsAndStandardLeavesThemNull()
    {
        var envelope = EnvelopeWith(new Dictionary<string, string>());

        var fifo = SnsSqsWireFormat.ToWire(envelope, FullTrace, fifo: true);
        var standard = SnsSqsWireFormat.ToWire(envelope, FullTrace);

        Assert.Equal("corr-1", fifo.GroupId);
        Assert.Equal(envelope.MessageId.ToString("D"), fifo.DeduplicationId);
        Assert.Null(standard.GroupId);
        Assert.Null(standard.DeduplicationId);
    }
}
