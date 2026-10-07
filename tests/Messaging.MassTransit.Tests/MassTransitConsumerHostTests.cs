using Messaging.Abstractions;
using Messaging.Consuming;
using Messaging.MassTransit.Consuming;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.Tests;

public class MassTransitConsumerHostTests
{
    private static MassTransitConsumerHost CreateHost(
        FakeMassTransitBusAdapter bus,
        FakeMassTransitConsumeProcessor processor) =>
        new(bus, processor, NullLogger<MassTransitConsumerHost>.Instance);

    [Fact]
    public void State_BeforeStart_IsStopped()
    {
        var host = CreateHost(new FakeMassTransitBusAdapter(), new FakeMassTransitConsumeProcessor());

        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task StartThenStop_DrivesTheFullLifecycleSequence()
    {
        var bus = new FakeMassTransitBusAdapter();
        var host = CreateHost(bus, new FakeMassTransitConsumeProcessor());
        var observed = new List<ConsumerHostState> { host.State };

        await host.StartAsync(CancellationToken.None);
        observed.Add(host.State);

        await host.StopAsync(CancellationToken.None);
        observed.Add(host.State);

        // The host surfaces the resting Running/Stopped states around the bus start/stop. The
        // transient Starting/Stopping states are internal to the synchronous start/stop calls.
        Assert.Equal(
            new[] { ConsumerHostState.Stopped, ConsumerHostState.Running, ConsumerHostState.Stopped },
            observed);
        Assert.Equal(1, bus.StartCount);
        Assert.Equal(1, bus.StopCount);
    }

    [Fact]
    public async Task StartAsync_WhenBusStartThrows_LeavesHostStopped()
    {
        var bus = new FakeMassTransitBusAdapter(throwOnStart: new InvalidOperationException("broker down"));
        var host = CreateHost(bus, new FakeMassTransitConsumeProcessor());

        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync(CancellationToken.None));

        Assert.Equal(ConsumerHostState.Stopped, host.State);
    }

    [Fact]
    public async Task DeliverAsync_WhenRunning_RunsEnvelopeThroughCoreSteps()
    {
        var bus = new FakeMassTransitBusAdapter();
        var processor = new FakeMassTransitConsumeProcessor();
        var host = CreateHost(bus, processor);
        var envelope = MassTransitTestSupport.ValidEnvelope();

        await host.StartAsync(CancellationToken.None);
        await host.DeliverAsync(envelope, CancellationToken.None);

        Assert.Single(processor.Processed);
        Assert.Same(envelope, processor.Processed[0]);
    }

    [Fact]
    public async Task DeliverAsync_BeforeStart_IsRejected()
    {
        var host = CreateHost(new FakeMassTransitBusAdapter(), new FakeMassTransitConsumeProcessor());

        await Assert.ThrowsAsync<InvalidConsumerHostStateException>(() =>
            host.DeliverAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None));
    }

    [Fact]
    public async Task DeliverAsync_AfterStop_IsRejected()
    {
        var host = CreateHost(new FakeMassTransitBusAdapter(), new FakeMassTransitConsumeProcessor());

        await host.StartAsync(CancellationToken.None);
        await host.StopAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidConsumerHostStateException>(() =>
            host.DeliverAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None));
    }

    [Fact]
    public async Task DeliverAsync_WithNullEnvelope_Throws()
    {
        var host = CreateHost(new FakeMassTransitBusAdapter(), new FakeMassTransitConsumeProcessor());
        await host.StartAsync(CancellationToken.None);

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            host.DeliverAsync(null!, CancellationToken.None));
    }
}
