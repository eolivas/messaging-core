using System.Text;
using Messaging.Abstractions;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Xunit.Abstractions;
namespace Messaging.MassTransit.Tests;

/// <summary>
/// Task 14.13: header and size boundaries (Req 6.6, 6.7, 6.8, 18.5). Also measures the real wire
/// overhead MassTransit adds, to confirm the 2,048-byte allowance (design R6).
/// </summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsHeaderAndSizeBoundaryIntegrationTests : IAsyncLifetime
{
    private const int Limit = AmazonSqsProviderSettings.DefaultMaxMessageSizeBytes;

    private readonly LocalStackContainerFixture _fixture;
    private readonly ITestOutputHelper _output;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsHeaderAndSizeBoundaryIntegrationTests(
        LocalStackContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Publish_MoreThanTenHeaders_RoundTripsIntact()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var processor = new CapturingConsumeProcessor();
        var bus = _harness.CreateBus("headers", MassTransitRole.Both, MessagingMode.PublishSubscribe, processor);
        await bus.StartBusAsync(cts.Token);

        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < 12; i++)
        {
            headers[$"key-{i:00}"] = $"value-{i}";
        }

        headers["long-value"] = new string('x', 2000);
        headers["bad key!"] = "not-attribute-eligible";
        headers["h.prefixed"] = "collides-with-prefix";
        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("headers") with { Headers = headers };

        await bus.PublishAsync(envelope, cts.Token);
        await processor.WaitForCountAsync(1);

        var received = Assert.Single(processor.Received);
        Assert.True(received.Headers.Count > SnsSqsHeaderMapper.MaxAttributes);
        SnsSqsIntegrationHarness.AssertDeliveredEqualsPublished(envelope, received);
    }

    [Fact]
    public async Task Publish_EstimateJustBelowLimit_IsDelivered()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var processor = new CapturingConsumeProcessor();
        var bus = _harness.CreateBus("size-ok", MassTransitRole.Both, MessagingMode.PublishSubscribe, processor);
        await bus.StartBusAsync(cts.Token);

        var length = LargestPayloadLength(Limit);
        var envelope = SnsSqsIntegrationHarness.CreateEnvelope() with { Payload = Payload(length - 8) };
        _output.WriteLine($"Delivered payload bytes={length - 8}, estimate={Estimate(envelope)}, limit={Limit}");

        await bus.PublishAsync(envelope, cts.Token);
        await processor.WaitForCountAsync(1);

        SnsSqsIntegrationHarness.AssertDeliveredEqualsPublished(envelope, Assert.Single(processor.Received));
    }

    [Fact]
    public async Task Publish_EstimateJustAboveLimit_IsRejectedBeforeSend()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var bus = _harness.CreateBus("size-big", MassTransitRole.Publisher, MessagingMode.PublishSubscribe, null);
        await bus.StartBusAsync(cts.Token);

        var length = LargestPayloadLength(Limit);
        var envelope = SnsSqsIntegrationHarness.CreateEnvelope() with { Payload = Payload(length + 256) };

        var ex = await Assert.ThrowsAsync<MessageTooLargeException>(() => bus.PublishAsync(envelope, cts.Token));

        Assert.Equal(Limit, ex.Limit);
        Assert.True(ex.Actual > Limit);
        // Rejected before send: the topic was never created.
        Assert.Null(await _harness.Sns.FindTopicAsync(_harness.TopicName()));
    }

    [Fact]
    public async Task Publish_AtEstimatedLimit_MeasuredWireOverheadIsWithinAllowance()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var bus = _harness.CreateBus("wire", MassTransitRole.Publisher, MessagingMode.MessageQueue, null);
        await bus.StartBusAsync(cts.Token);

        var length = LargestPayloadLength(Limit);
        var envelope = SnsSqsIntegrationHarness.CreateEnvelope() with { Payload = Payload(length - 8) };
        await bus.PublishAsync(envelope, cts.Token);

        var message = await _harness.ReceiveRawAsync(_harness.QueueName("wire"), SnsSqsIntegrationHarness.WaitTimeout);
        Assert.NotNull(message);

        long wire = Encoding.UTF8.GetByteCount(message!.Body);
        foreach (var attribute in message.MessageAttributes)
        {
            wire += Encoding.UTF8.GetByteCount(attribute.Key)
                + Encoding.UTF8.GetByteCount(attribute.Value.DataType)
                + (attribute.Value.StringValue is null ? attribute.Value.BinaryValue?.Length ?? 0
                    : Encoding.UTF8.GetByteCount(attribute.Value.StringValue));
        }

        var estimateWithoutAllowance = Estimate(envelope) - SnsSqsMessageSizeGuard.EnvelopeAllowanceBytes;
        var overhead = wire - estimateWithoutAllowance;
        _output.WriteLine(
            $"MEASURED wire={wire}, estimateWithoutAllowance={estimateWithoutAllowance}, " +
            $"overhead={overhead}, allowance={SnsSqsMessageSizeGuard.EnvelopeAllowanceBytes}");

        Assert.True(wire <= Limit, $"Wire size {wire} exceeds the SQS limit {Limit}.");
        Assert.True(
            overhead <= SnsSqsMessageSizeGuard.EnvelopeAllowanceBytes,
            $"Measured overhead {overhead} exceeds the allowance.");
    }

    private static byte[] Payload(int length) => new byte[Math.Max(length, 0)].Select((_, i) => (byte)(i % 251)).ToArray();

    private static long Estimate(Messaging.Contracts.MessageEnvelope envelope)
    {
        var traceHeaders = new Dictionary<string, string> { [SnsSqsHeaderMapper.CorrelationIdAttribute] = envelope.CorrelationId };
        var wire = SnsSqsWireFormat.ToWire(envelope, traceHeaders);
        var ex = Assert.Throws<MessageTooLargeException>(
            () => SnsSqsMessageSizeGuard.Check(wire.Body, wire.AttributeHeaders, limit: 0));
        return ex.Actual;
    }

    /// <summary>The largest payload length whose estimate does not exceed <paramref name="limit"/>.</summary>
    private static int LargestPayloadLength(long limit)
    {
        int low = 0, high = (int)limit;
        while (low < high)
        {
            var mid = (low + high + 1) / 2;
            var envelope = SnsSqsIntegrationHarness.CreateEnvelope() with { Payload = new byte[mid] };
            if (Estimate(envelope) <= limit)
            {
                low = mid;
            }
            else
            {
                high = mid - 1;
            }
        }

        return low;
    }
}
