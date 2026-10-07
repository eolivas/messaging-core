using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Contracts;
using Messaging.Providers;
using Messaging.Publishing;
using Messaging.Tests.Consuming;
using Xunit;

namespace Messaging.Tests.Fakes;

// Unit tests for the FakeMessagingProvider test-support fake (task 10).
// The fake lets the real publish and consume pipelines be exercised end-to-end with no transport,
// capturing published/delivered envelopes and simulating confirm/fail outcomes.
// Covers Req 3.1 (publisher abstraction), 3.3 (consumer host lifecycle), 3.4 (registry maps a mode
// to exactly one provider).
[Trait("Feature", "messaging-core")]
public class FakeMessagingProviderTests
{
    private const MessagingMode ActiveMode = MessagingMode.PublishSubscribe;
    private const string OrderCreatedType = "orders.created";

    private static MessageEnvelope OrderEnvelope(string? correlationId = "corr-1")
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(new OrderCreatedContract("order-7", 42m));
        return new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = correlationId!,
            MessageType = OrderCreatedType,
            CreatedAtUtc = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, string>(),
            Payload = payload,
        };
    }

    private static (PublishPipeline Publish, FakeMessagingProvider Provider, RecordingHandlerInvoker Handler)
        BuildLoopback(FakePublishOutcome outcome = FakePublishOutcome.Confirm)
    {
        var handler = new RecordingHandlerInvoker();
        var binder = new JsonContractBinder(new[]
        {
            new ContractRegistration(OrderCreatedType, typeof(OrderCreatedContract)),
        });
        var consume = new ConsumePipeline(new ConsumerHostStateMachine(), binder, handler);

        var provider = FakeMessagingProvider.CreateWithLoopback(consume, ActiveMode, outcome);
        var registry = new ProviderRegistry(new IMessagingProvider[] { provider });
        var publish = new PublishPipeline(new CorrelationIdGate(), registry, ActiveMode);
        return (publish, provider, handler);
    }

    [Fact]
    public async Task PublishAsync_Confirmed_CapturesPublishedEnvelope()
    {
        var (publish, provider, _) = BuildLoopback();
        // A confirmed publish loops into the consume side, which only accepts while Running (Req 10.5).
        await provider.ConsumerHost.StartAsync(CancellationToken.None);
        var envelope = OrderEnvelope();

        var result = await publish.PublishAsync(envelope, CancellationToken.None);

        Assert.True(result.Succeeded);
        var published = Assert.Single(provider.FakePublisher.PublishedEnvelopes);
        Assert.Equal(envelope.MessageId, published.MessageId);
    }

    [Fact]
    public async Task PublishAsync_Confirmed_LoopsEnvelopeIntoConsumeSide()
    {
        var (publish, provider, handler) = BuildLoopback();
        // The consumer host must be Running to accept delivered messages (Req 10.5).
        await provider.ConsumerHost.StartAsync(CancellationToken.None);

        await publish.PublishAsync(OrderEnvelope("corr-xyz"), CancellationToken.None);

        var delivered = Assert.Single(provider.FakeConsumerHost!.DeliveredEnvelopes);
        Assert.Equal("corr-xyz", delivered.CorrelationId);
        // The real consume pipeline ran: the handler saw the same correlation id end-to-end (Req 9.4).
        Assert.Equal(new[] { "corr-xyz" }, handler.ObservedCorrelationIds);
    }

    [Fact]
    public async Task PublishAsync_FailOutcome_CapturesButLeavesUndelivered()
    {
        var (publish, provider, handler) = BuildLoopback(FakePublishOutcome.Fail);
        await provider.ConsumerHost.StartAsync(CancellationToken.None);

        var result = await publish.PublishAsync(OrderEnvelope(), CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.NotNull(result.Error);
        // Captured as attempted, but never looped into the consume side.
        Assert.Single(provider.FakePublisher.PublishedEnvelopes);
        Assert.Empty(provider.FakeConsumerHost!.DeliveredEnvelopes);
        Assert.Empty(handler.ObservedCorrelationIds);
    }

    [Fact]
    public async Task Outcome_SwitchedFailToConfirm_DrivesBothPathsOnOneInstance()
    {
        var (publish, provider, _) = BuildLoopback(FakePublishOutcome.Fail);
        await provider.ConsumerHost.StartAsync(CancellationToken.None);

        var failed = await publish.PublishAsync(OrderEnvelope(), CancellationToken.None);
        provider.FakePublisher.Outcome = FakePublishOutcome.Confirm;
        var confirmed = await publish.PublishAsync(OrderEnvelope(), CancellationToken.None);

        Assert.False(failed.Succeeded);
        Assert.True(confirmed.Succeeded);
        Assert.Equal(2, provider.FakePublisher.PublishedEnvelopes.Count);
        Assert.Single(provider.FakeConsumerHost!.DeliveredEnvelopes);
    }

    [Fact]
    public async Task ConsumerHost_StartStop_FollowsLifecycleThroughFake()
    {
        var (_, provider, _) = BuildLoopback();
        var host = provider.ConsumerHost;

        Assert.Equal(ConsumerHostState.Stopped, host.State);
        await host.StartAsync(CancellationToken.None);
        Assert.Equal(ConsumerHostState.Running, host.State);
        await host.StopAsync(CancellationToken.None);
        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task Delivery_WhileStopped_IsRejectedBeforeHandler()
    {
        var (_, provider, handler) = BuildLoopback();
        var host = provider.FakeConsumerHost!;

        // Host is Stopped: a stopped host does not accept new messages (Req 10.5).
        await Assert.ThrowsAsync<InvalidConsumerHostStateException>(
            () => host.DeliverAsync(OrderEnvelope(), CancellationToken.None));

        // Captured as attempted, but the handler was never invoked.
        Assert.Single(host.DeliveredEnvelopes);
        Assert.Empty(handler.ObservedCorrelationIds);
    }

    [Fact]
    public void Registry_ResolvesFakeProvider_ForItsMode()
    {
        var (_, provider, _) = BuildLoopback();
        var registry = new ProviderRegistry(new IMessagingProvider[] { provider });

        var resolved = registry.Resolve(ActiveMode);

        Assert.Same(provider, resolved);
    }

    [Fact]
    public async Task CheckTransportAsync_ReportsConfiguredHealth()
    {
        var (_, provider, _) = BuildLoopback();

        var healthy = await provider.CheckTransportAsync(CancellationToken.None);
        Assert.True(healthy.Reachable);

        provider.SetTransportHealth(new TransportHealth(Reachable: false, UnavailableDependency: "fake-broker"));
        var unhealthy = await provider.CheckTransportAsync(CancellationToken.None);
        Assert.False(unhealthy.Reachable);
        Assert.Equal("fake-broker", unhealthy.UnavailableDependency);
    }

    [Fact]
    public void PublishOnly_ConsumerHost_Throws()
    {
        var provider = FakeMessagingProvider.CreatePublishOnly(ActiveMode);

        Assert.Throws<NotSupportedException>(() => provider.ConsumerHost);
    }

    [Fact]
    public async Task PublishOnly_CapturesConfirmedPublish()
    {
        var provider = FakeMessagingProvider.CreatePublishOnly(ActiveMode);
        var registry = new ProviderRegistry(new IMessagingProvider[] { provider });
        var publish = new PublishPipeline(new CorrelationIdGate(), registry, ActiveMode);

        var result = await publish.PublishAsync(OrderEnvelope(), CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Single(provider.FakePublisher.PublishedEnvelopes);
    }

    [Fact]
    public void CreateWithLoopback_NullPipeline_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => FakeMessagingProvider.CreateWithLoopback(null!));
    }
}
