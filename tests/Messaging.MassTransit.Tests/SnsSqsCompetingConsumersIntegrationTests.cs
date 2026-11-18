using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.3: two instances on one queue handle each message once (Req 2.4, 18.6).</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsCompetingConsumersIntegrationTests : IAsyncLifetime
{
    private const int MessageCount = 20;
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsCompetingConsumersIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Publish_TwentyMessagesTwoInstancesOneQueue_EachHandledExactlyOnce()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var first = new CapturingConsumeProcessor();
        var second = new CapturingConsumeProcessor();
        var consumerA = _harness.CreateBus("competing", MassTransitRole.Consumer, MessagingMode.PublishSubscribe, first);
        var consumerB = _harness.CreateBus("competing", MassTransitRole.Consumer, MessagingMode.PublishSubscribe, second);
        var publisher = _harness.CreateBus("competing-pub", MassTransitRole.Publisher, MessagingMode.PublishSubscribe, null);
        await consumerA.StartBusAsync(cts.Token);
        await consumerB.StartBusAsync(cts.Token);
        await publisher.StartBusAsync(cts.Token);

        var published = Enumerable.Range(0, MessageCount)
            .Select(i => SnsSqsIntegrationHarness.CreateEnvelope($"msg-{i}"))
            .ToArray();
        foreach (var envelope in published)
        {
            await publisher.PublishAsync(envelope, cts.Token);
        }

        await WaitForTotalAsync(first, second, MessageCount);
        await Task.Delay(TimeSpan.FromSeconds(3));

        var handled = first.Received.Concat(second.Received).Select(e => e.MessageId).ToArray();
        Assert.Equal(MessageCount, handled.Length);
        Assert.Equal(
            published.Select(e => e.MessageId).OrderBy(id => id),
            handled.OrderBy(id => id));
    }

    private static async Task WaitForTotalAsync(
        CapturingConsumeProcessor first, CapturingConsumeProcessor second, int total)
    {
        var deadline = DateTime.UtcNow + SnsSqsIntegrationHarness.WaitTimeout;
        while (first.Received.Count + second.Received.Count < total)
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException(
                    $"Expected {total} messages; got {first.Received.Count + second.Received.Count}.");
            }

            await Task.Delay(100);
        }
    }
}
