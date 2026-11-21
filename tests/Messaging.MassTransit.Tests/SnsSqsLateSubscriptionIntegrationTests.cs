using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.4: a late endpoint receives only later messages (Req 2.5, 18.7).</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsLateSubscriptionIntegrationTests : IAsyncLifetime
{
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsLateSubscriptionIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Start_EndpointAfterFirstPublish_ReceivesOnlyLaterMessages()
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var early = new CapturingConsumeProcessor();
        var late = new CapturingConsumeProcessor();
        var earlyBus = _harness.CreateBus("late-early", MassTransitRole.Both, MessagingMode.PublishSubscribe, early);
        await earlyBus.StartBusAsync(cts.Token);

        var first = SnsSqsIntegrationHarness.CreateEnvelope("before");
        await earlyBus.PublishAsync(first, cts.Token);
        await early.WaitForCountAsync(1);

        var lateBus = _harness.CreateBus("late-joiner", MassTransitRole.Consumer, MessagingMode.PublishSubscribe, late);
        await lateBus.StartBusAsync(cts.Token);
        var second = SnsSqsIntegrationHarness.CreateEnvelope("after");
        await earlyBus.PublishAsync(second, cts.Token);

        await late.WaitForCountAsync(1);
        await early.WaitForCountAsync(1);
        await Task.Delay(TimeSpan.FromSeconds(3));

        var lateReceived = Assert.Single(late.Received);
        Assert.Equal(second.MessageId, lateReceived.MessageId);
        Assert.Equal(new[] { first.MessageId, second.MessageId }, early.Received.Select(e => e.MessageId));
    }
}
