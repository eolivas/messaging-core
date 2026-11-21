using System.Text;
using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.9: FIFO ordering and deduplication across a FIFO topic and queue (Req 8.4, 8.5, 8.6, 18.12).</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsFifoDeliveryIntegrationTests : IAsyncLifetime
{
    private const int MessageCount = 10;
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsFifoDeliveryIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Publish_SameGroupInSequence_DeliversInPublishOrder()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var processor = new CapturingConsumeProcessor();
        var bus = CreateFifoBus("fifo-order", processor);
        await bus.StartBusAsync(cts.Token);
        var group = Guid.NewGuid().ToString("D");
        var published = Enumerable.Range(0, MessageCount)
            .Select(i => SnsSqsIntegrationHarness.CreateEnvelope($"seq-{i}") with { CorrelationId = group })
            .ToArray();

        foreach (var envelope in published)
        {
            await bus.PublishAsync(envelope, cts.Token);
        }

        await processor.WaitForCountAsync(MessageCount);
        Assert.Equal(published.Select(e => e.MessageId), processor.Received.Select(e => e.MessageId));
        Assert.Equal(
            Enumerable.Range(0, MessageCount).Select(i => $"seq-{i}"),
            processor.Received.Select(e => Encoding.UTF8.GetString(e.Payload.Span)));
    }

    [Fact]
    public async Task Publish_RepeatedDeduplicationId_DeliversOneCopy()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var processor = new CapturingConsumeProcessor();
        var bus = CreateFifoBus("fifo-dedup", processor);
        await bus.StartBusAsync(cts.Token);
        var group = Guid.NewGuid().ToString("D");
        var repeated = SnsSqsIntegrationHarness.CreateEnvelope("repeated") with { CorrelationId = group };
        var distinct = SnsSqsIntegrationHarness.CreateEnvelope("distinct") with { CorrelationId = group };

        await bus.PublishAsync(repeated, cts.Token);
        await bus.PublishAsync(repeated, cts.Token);
        await bus.PublishAsync(distinct, cts.Token);

        await processor.WaitForCountAsync(2);
        await Task.Delay(TimeSpan.FromSeconds(3));
        Assert.Equal(new[] { repeated.MessageId, distinct.MessageId }, processor.Received.Select(e => e.MessageId));
    }

    private Messaging.MassTransit.AmazonSqs.AmazonSqsBusAdapter CreateFifoBus(
        string endpoint, CapturingConsumeProcessor processor) =>
        _harness.CreateBus(
            endpoint,
            MassTransitRole.Both,
            MessagingMode.PublishSubscribe,
            processor,
            // MassTransit dispatches one receive batch concurrently, so in-process order is only
            // observable with a concurrency limit of 1 (SQS itself returns a group in order).
            s => s with { Fifo = true, ConcurrencyLimit = 1 });
}
