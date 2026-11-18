using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.8: round trip with raw and wrapped delivery (Req 6.1, 6.2, 18.11).</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsRawAndWrappedDeliveryIntegrationTests : IAsyncLifetime
{
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsRawAndWrappedDeliveryIntegrationTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Publish_RawMessageDeliverySetting_DeliversEqualEnvelope(bool rawMessageDelivery)
    {
        using var cts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        var processor = new CapturingConsumeProcessor();
        var bus = _harness.CreateBus(
            $"raw-{rawMessageDelivery}".ToLowerInvariant(),
            MassTransitRole.Both,
            MessagingMode.PublishSubscribe,
            processor,
            s => s with { RawMessageDelivery = rawMessageDelivery });
        await bus.StartBusAsync(cts.Token);

        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("héllo wörld \u20ac");
        await bus.PublishAsync(envelope, cts.Token);
        await processor.WaitForCountAsync(1);

        SnsSqsIntegrationHarness.AssertDeliveredEqualsPublished(envelope, Assert.Single(processor.Received));
    }
}
