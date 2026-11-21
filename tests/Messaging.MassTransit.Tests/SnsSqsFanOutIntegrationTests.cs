using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.MassTransit.Configuration;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.2: one publish reaches every subscribed endpoint (Req 2.1-2.3, 2.6, 2.7, 7.3, 18.5).</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsFanOutIntegrationTests : IAsyncLifetime
{
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsFanOutIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Publish_ThreeEndpoints_EachReceivesOneEqualCopy()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var processors = new[] { new CapturingConsumeProcessor(), new CapturingConsumeProcessor(), new CapturingConsumeProcessor() };
        var buses = processors
            .Select((p, i) => _harness.CreateBus($"fanout-{i}", MassTransitRole.Both, MessagingMode.PublishSubscribe, p))
            .ToArray();
        foreach (var bus in buses)
        {
            await bus.StartBusAsync(cts.Token);
        }

        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("fan-out");
        await buses[0].PublishAsync(envelope, cts.Token);

        foreach (var processor in processors)
        {
            await processor.WaitForCountAsync(1);
        }

        await Task.Delay(TimeSpan.FromSeconds(3));
        foreach (var processor in processors)
        {
            var received = Assert.Single(processor.Received);
            SnsSqsIntegrationHarness.AssertDeliveredEqualsPublished(envelope, received);
        }
    }

    [Fact]
    public async Task Publish_OneEndpointFails_OtherEndpointsStillReceiveOneCopy()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var failing = new CapturingConsumeProcessor(_ => throw new InvalidOperationException("boom"));
        var healthyA = new CapturingConsumeProcessor();
        var healthyB = new CapturingConsumeProcessor();
        var busFailing = _harness.CreateBus("fail-0", MassTransitRole.Both, MessagingMode.PublishSubscribe, failing);
        var busA = _harness.CreateBus("fail-1", MassTransitRole.Both, MessagingMode.PublishSubscribe, healthyA);
        var busB = _harness.CreateBus("fail-2", MassTransitRole.Both, MessagingMode.PublishSubscribe, healthyB);
        await busFailing.StartBusAsync(cts.Token);
        await busA.StartBusAsync(cts.Token);
        await busB.StartBusAsync(cts.Token);

        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("fail-one");
        await busA.PublishAsync(envelope, cts.Token);

        await failing.WaitForCountAsync(1);
        await healthyA.WaitForCountAsync(1);
        await healthyB.WaitForCountAsync(1);
        await Task.Delay(TimeSpan.FromSeconds(3));

        SnsSqsIntegrationHarness.AssertDeliveredEqualsPublished(envelope, Assert.Single(healthyA.Received));
        SnsSqsIntegrationHarness.AssertDeliveredEqualsPublished(envelope, Assert.Single(healthyB.Received));
    }

    [Fact]
    public async Task Publish_TopicWithNoSubscription_CompletesWithoutError()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        const string messageType = "test.nosubscribers";
        var contracts = new[] { new ContractRegistration(messageType, typeof(SnsSqsIntegrationHarness.HarnessContract)) };
        var publisher = _harness.CreateBus(
            "nosub-publisher", MassTransitRole.Publisher, MessagingMode.PublishSubscribe, null, contracts: contracts);
        await publisher.StartBusAsync(cts.Token);

        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("nobody", messageType);

        var exception = await Record.ExceptionAsync(() => publisher.PublishAsync(envelope, cts.Token));

        Assert.Null(exception);
    }
}
