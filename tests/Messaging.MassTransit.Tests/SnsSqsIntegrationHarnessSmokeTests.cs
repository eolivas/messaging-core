using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
namespace Messaging.MassTransit.Tests;

/// <summary>Task 14.1 smoke test: publish then consume through the harness against LocalStack.</summary>
[Collection(LocalStackCollection.Name)]
[Trait("Category", "Integration")]
public sealed class SnsSqsIntegrationHarnessSmokeTests : IAsyncLifetime
{
    private readonly LocalStackContainerFixture _fixture;
    private SnsSqsIntegrationHarness _harness = null!;

    public SnsSqsIntegrationHarnessSmokeTests(LocalStackContainerFixture fixture) => _fixture = fixture;

    public async Task InitializeAsync() => _harness = await SnsSqsIntegrationHarness.CreateAsync(_fixture);

    public async Task DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public async Task Publish_then_consume_delivers_the_envelope_through_real_adapter()
    {
        var processor = new CapturingConsumeProcessor();
        var bus = _harness.CreateBus(
            "smoke-endpoint", MassTransitRole.Both, MessagingMode.PublishSubscribe, processor);
        using var startCts = new CancellationTokenSource(SnsSqsIntegrationHarness.WaitTimeout);
        await bus.StartBusAsync(startCts.Token);

        var envelope = SnsSqsIntegrationHarness.CreateEnvelope("smoke");
        await bus.PublishAsync(envelope, startCts.Token);
        await processor.WaitForCountAsync(1);

        var received = Assert.Single(processor.Received);
        Assert.Equal(envelope.MessageId, received.MessageId);
        Assert.Equal(envelope.CorrelationId, received.CorrelationId);
        Assert.Equal(envelope.MessageType, received.MessageType);
        Assert.Equal(envelope.Payload.ToArray(), received.Payload.ToArray());
        Assert.Matches("^t[0-9a-f]{8}-$", _harness.Prefix);
    }
}
