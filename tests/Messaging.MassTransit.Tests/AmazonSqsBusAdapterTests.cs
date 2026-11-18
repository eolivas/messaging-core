using MassTransit;
using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.Lifecycle;
using Messaging.MassTransit.AmazonSqs;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Unit tests for <see cref="AmazonSqsBusAdapter"/> publish, dead-letter send, role gating, start, and
/// stop over a fake bus and <see cref="FakeSnsSqsTopologyClient"/>. Reachability is tested separately.
/// Requirements 1.5, 1.6, 11.1 to 11.5, 12.1, 12.3, 12.4, 12.5, 12.7, 17.7.
/// </summary>
[Trait("Feature", "sns-sqs-provider")]
public class AmazonSqsBusAdapterTests
{
    private static readonly ContractRegistration[] Contracts =
        { new("order.created", typeof(object)) };

    private static readonly AmazonSqsProviderSettings Settings =
        new() { Region = "us-east-1", EndpointName = "orders" };

    private static AmazonSqsBusAdapter NewAdapter(
        FakeBusControl bus,
        FakeSnsSqsTopologyClient client,
        MassTransitRole role,
        InFlightTracker? tracker = null,
        int shutdownSeconds = 30,
        MessagingMode mode = MessagingMode.PublishSubscribe)
    {
        var settings = Settings;
        return new AmazonSqsBusAdapter(
            bus,
            settings,
            mode,
            role,
            Contracts,
            new SnsSqsTopologyManager(client, settings, NullLogger<SnsSqsTopologyManager>.Instance),
            client,
            new ObservabilityContext(
                new NoOpMessagingLog(),
                new NoOpMessagingMetrics(),
                new MessagingTraceSource(tracingEnabled: false),
                new PayloadRedactor()),
            new AmazonSqsMetrics(),
            tracker ?? new InFlightTracker(),
            shutdownSeconds,
            NullLogger<AmazonSqsBusAdapter>.Instance);
    }

    [Theory]
    [InlineData(MassTransitRole.Consumer)]
    [InlineData(MassTransitRole.Both)]
    public async Task StartBusAsync_ConsumingRole_AppliesTopologyBeforeStartingBus(MassTransitRole role)
    {
        var client = new FakeSnsSqsTopologyClient();
        var callsWhenBusStarted = -1;
        var bus = new FakeBusControl(onStart: () => callsWhenBusStarted = client.Calls.Count);
        var adapter = NewAdapter(bus, client, role);

        await adapter.StartBusAsync(CancellationToken.None);

        Assert.Equal(1, bus.StartCount);
        Assert.True(callsWhenBusStarted > 0, "Topology must be applied before the bus starts.");
        Assert.True(client.MutationCount > 0);
    }

    [Fact]
    public async Task StartBusAsync_PublisherRole_SkipsTopologyAndStartsBus()
    {
        var client = new FakeSnsSqsTopologyClient();
        var bus = new FakeBusControl();
        var adapter = NewAdapter(bus, client, MassTransitRole.Publisher);

        await adapter.StartBusAsync(CancellationToken.None);

        Assert.Equal(1, bus.StartCount);
        Assert.Empty(client.Calls);
    }

    [Fact]
    public async Task StartBusAsync_TopologyFails_DoesNotStartBus()
    {
        var client = new FakeSnsSqsTopologyClient();
        var failure = new InvalidOperationException("sqs down");
        client.FailOn(SnsSqsTopologyOperation.GetQueueUrl, failure);
        client.FailOn(SnsSqsTopologyOperation.CreateQueue, failure);
        var bus = new FakeBusControl();
        var adapter = NewAdapter(bus, client, MassTransitRole.Consumer);

        await Assert.ThrowsAnyAsync<Exception>(() => adapter.StartBusAsync(CancellationToken.None));

        Assert.Equal(0, bus.StartCount);
    }

    [Fact]
    public async Task StartBusAsync_BusStartThrows_SurfacesErrorAndLeavesAdapterNotStarted()
    {
        var failure = new InvalidOperationException("bus failed");
        var bus = new FakeBusControl(throwOnStart: failure);
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Publisher);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => adapter.StartBusAsync(CancellationToken.None));
        await adapter.DisposeAsync();

        Assert.Same(failure, thrown);
        // _started stayed false, so dispose does not stop the bus.
        Assert.Equal(0, bus.StopCount);
    }

    [Fact]
    public async Task DisposeAsync_AfterSuccessfulStart_StopsBusOnce()
    {
        var bus = new FakeBusControl();
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Publisher);
        await adapter.StartBusAsync(CancellationToken.None);

        await adapter.DisposeAsync();
        await adapter.DisposeAsync();

        Assert.Equal(1, bus.StopCount);
    }

    [Fact]
    public async Task StopBusAsync_Completes_StopsAcceptingAndRecordsDrainedResult()
    {
        var tracker = new InFlightTracker();
        var bus = new FakeBusControl();
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Publisher, tracker);
        await adapter.StartBusAsync(CancellationToken.None);

        await adapter.StopBusAsync(CancellationToken.None);

        Assert.False(tracker.IsAccepting);
        Assert.Equal(1, bus.StopCount);
        Assert.NotNull(adapter.LastShutdownResult);
        Assert.False(adapter.LastShutdownResult!.TimedOut);
        Assert.Equal(0, adapter.LastShutdownResult.IncompleteInFlightCount);
    }

    [Fact]
    public async Task StopBusAsync_BusExceedsShutdownTimeout_RecordsTimedOutWithInFlightCount()
    {
        var tracker = new InFlightTracker();
        Assert.True(tracker.TryBeginProcessing());
        var bus = new FakeBusControl(onStop: ct => Task.Delay(Timeout.Infinite, ct));
        var adapter = NewAdapter(
            bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Publisher, tracker, shutdownSeconds: 1);
        await adapter.StartBusAsync(CancellationToken.None);

        await adapter.StopBusAsync(CancellationToken.None);

        Assert.NotNull(adapter.LastShutdownResult);
        Assert.True(adapter.LastShutdownResult!.TimedOut);
        Assert.Equal(1, adapter.LastShutdownResult.IncompleteInFlightCount);
    }

    [Fact]
    public async Task StopBusAsync_CallerCancels_PropagatesCancellation()
    {
        var bus = new FakeBusControl(onStop: ct => Task.Delay(Timeout.Infinite, ct));
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Publisher);
        await adapter.StartBusAsync(CancellationToken.None);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => adapter.StopBusAsync(cts.Token));
    }

    // ----- Publish: mode to endpoint (Req 1.5, 1.6) --------------------------------------------

    [Fact]
    public async Task PublishAsync_PublishSubscribeMode_SendsOnceToTopicAddress()
    {
        var bus = new FakeBusControl();
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Publisher);
        var envelope = MassTransitTestSupport.ValidEnvelope();

        await adapter.PublishAsync(envelope, CancellationToken.None);

        var expected = new Uri($"topic:{SnsSqsNamingConvention.TopicName(envelope.MessageType, Settings)}");
        Assert.Equal(1, bus.SendCount);
        Assert.Equal(0, bus.PublishCount);
        Assert.Equal(new[] { expected }, bus.SendEndpointUris);
    }

    [Fact]
    public async Task PublishAsync_MessageQueueMode_SendsOnceToQueueAddress()
    {
        var bus = new FakeBusControl();
        var adapter = NewAdapter(
            bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Publisher, mode: MessagingMode.MessageQueue);

        await adapter.PublishAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None);

        var expected = new Uri($"queue:{SnsSqsNamingConvention.QueueName(Settings.EndpointName, Settings)}");
        Assert.Equal(1, bus.SendCount);
        Assert.Equal(new[] { expected }, bus.SendEndpointUris);
    }

    // ----- Role gating (Req 11.1 to 11.5) ------------------------------------------------------

    [Theory]
    [InlineData(MassTransitRole.Publisher)]
    [InlineData(MassTransitRole.Both)]
    public async Task PublishAsync_PublishingRole_Sends(MassTransitRole role)
    {
        var bus = new FakeBusControl();
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), role);

        await adapter.PublishAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None);

        Assert.Equal(1, bus.SendCount);
    }

    [Fact]
    public async Task PublishAsync_ConsumerRole_ThrowsPublishNotPermittedAndSendsNothing()
    {
        var bus = new FakeBusControl();
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Consumer);

        await Assert.ThrowsAsync<PublishNotPermittedException>(
            () => adapter.PublishAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None));

        Assert.Equal(0, bus.SendCount);
        Assert.Empty(bus.SendEndpointUris);
    }

    [Theory]
    [InlineData(MassTransitRole.Publisher)]
    [InlineData(MassTransitRole.Consumer)]
    [InlineData(MassTransitRole.Both)]
    public async Task SendToDeadLetterQueueAsync_AnyRole_SendsToQueueAddress(MassTransitRole role)
    {
        var bus = new FakeBusControl();
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), role);
        var message = EnvelopeMessage.FromEnvelope(MassTransitTestSupport.ValidEnvelope());

        await adapter.SendToDeadLetterQueueAsync(
            "orders-dlq",
            message,
            new Dictionary<string, string>(),
            messageGroupId: null,
            deduplicationId: null,
            CancellationToken.None);

        Assert.Equal(1, bus.SendCount);
        Assert.Equal(new[] { new Uri("queue:orders-dlq") }, bus.SendEndpointUris);
    }

    // ----- Start failure and polling settings --------------------------------------------------

    [Fact]
    public async Task StartBusAsync_BusStartThrows_DoesNotSurviveAsStartedOnDispose()
    {
        var bus = new FakeBusControl(throwOnStart: new InvalidOperationException("bus failed"));
        var adapter = NewAdapter(bus, new FakeSnsSqsTopologyClient(), MassTransitRole.Consumer);

        await Assert.ThrowsAsync<InvalidOperationException>(() => adapter.StartBusAsync(CancellationToken.None));
        await adapter.DisposeAsync();

        Assert.Equal(0, bus.StopCount);
    }

    [Fact]
    public void Create_ConsumerRole_PollingSettingsReachReceiveEndpoint()
    {
        var settings = Settings with { WaitTimeSeconds = 7,
            MaxMessagesPerReceive = 4,
            ConcurrencyLimit = 3,
            AccessKeyId = "test-key",
            SecretAccessKey = "test-secret",
        };
        var client = new FakeSnsSqsTopologyClient();

        var adapter = AmazonSqsBusAdapter.Create(
            settings,
            MassTransitRole.Consumer,
            MessagingMode.PublishSubscribe,
            Contracts,
            new FakeMassTransitConsumeProcessor(),
            new SnsSqsTopologyManager(client, settings, NullLogger<SnsSqsTopologyManager>.Instance),
            client,
            new ObservabilityContext(
                new NoOpMessagingLog(),
                new NoOpMessagingMetrics(),
                new MessagingTraceSource(tracingEnabled: false),
                new PayloadRedactor()),
            new AmazonSqsMetrics(),
            new InFlightTracker(),
            30,
            NullLogger<AmazonSqsBusAdapter>.Instance);

        var json = System.Text.Json.JsonSerializer.Serialize(adapter.BusControl.GetProbeResult().Results);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var endpoints = doc.RootElement.GetProperty("bus").GetProperty("host").GetProperty("receiveEndpoint");
        var transport = endpoints.EnumerateArray()
            .Single(e => e.GetProperty("name").GetString() == "orders")
            .GetProperty("receiveTransport");

        Assert.Equal(7, transport.GetProperty("waitTimeSeconds").GetInt32());
        Assert.Equal(4, transport.GetProperty("prefetchCount").GetInt32());
        Assert.Equal(3, transport.GetProperty("concurrentMessageLimit").GetInt32());
    }
}
