using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MassTransit;
using Messaging.Abstractions;
using Messaging.Contracts;
using Messaging.MassTransit.Transport;
using Messaging.Observability;
using Microsoft.Extensions.Logging.Abstractions;

namespace Messaging.MassTransit.Tests;

/// <summary>
/// Example/unit tests for <see cref="RabbitMqBusAdapter"/>'s seam behaviors, exercised against a fake
/// <see cref="IBusControl"/> so no live broker is required:
/// <list type="bullet">
///   <item>mode → endpoint selection: Publish-Subscribe publishes, Message Queue sends (Req 1.5, 1.6);</item>
///   <item>bounded reachability mapping: started+healthy → true, not-started → false, a throwing check
///   → false (never propagates), an injected slow check → false on the ≤ 1s timeout (Req 10.1, 10.3,
///   10.4, 10.5);</item>
///   <item>start failure surfaces the error and leaves the adapter not-started (Req 5.5).</item>
/// </list>
/// The adapter is constructed via its internal bus-seam constructor (exposed to this test project),
/// which injects the fake bus directly; the public <c>Create</c> factory stays the production path.
/// </summary>
/// <remarks>
/// This class joins the <c>ReachabilityProbe</c> collection (parallelization disabled) so its
/// timing-bounded reachability probes are not raced against the rest of the concurrently-scheduled
/// suite. The adapter ABANDONS (does not cancel) the losing <c>CheckHealth</c> task on timeout, so a
/// slow fake would otherwise keep a thread-pool thread parked after the test returns; under full-suite
/// pool pressure that leaked thread could starve a sibling reachability probe past its own 1s budget
/// and flip its result. Collection isolation plus the self-releasing <see cref="FakeBusControl"/>
/// (which signals any parked probe thread to return the instant it is disposed) together remove that
/// starvation flake.
/// </remarks>
[CollectionDefinition("ReachabilityProbe", DisableParallelization = true)]
public sealed class ReachabilityProbeCollection
{
}

[Collection("ReachabilityProbe")]
public class RabbitMqBusAdapterTests
{
    private const string Host = "broker-display";
    private const string QueueName = "messaging-core-tests";

    // ----- Mode → endpoint selection (Req 1.5, 1.6) --------------------------------------------

    [Fact]
    public async Task PublishAsync_InPublishSubscribeMode_UsesPublishEndpoint()
    {
        var bus = new FakeBusControl();
        var adapter = CreateAdapter(bus, MessagingMode.PublishSubscribe);

        await adapter.PublishAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None);

        Assert.Equal(1, bus.PublishCount);
        Assert.Equal(0, bus.SendCount);
    }

    [Fact]
    public async Task PublishAsync_InMessageQueueMode_UsesSendEndpoint()
    {
        var bus = new FakeBusControl();
        var adapter = CreateAdapter(bus, MessagingMode.MessageQueue);

        await adapter.PublishAsync(MassTransitTestSupport.ValidEnvelope(), CancellationToken.None);

        Assert.Equal(1, bus.SendCount);
        Assert.Equal(0, bus.PublishCount);
        Assert.NotNull(bus.LastSendEndpointUri);
        Assert.Contains(QueueName, bus.LastSendEndpointUri!.ToString());
    }

    // ----- Reachability mapping (Req 10.1, 10.3, 10.4, 10.5) -----------------------------------

    [Fact]
    public async Task IsTransportReachableAsync_WhenStartedAndHealthy_ReturnsTrue()
    {
        var bus = new FakeBusControl(health: BusHealthStatus.Healthy);
        var adapter = CreateAdapter(bus, MessagingMode.PublishSubscribe);
        await adapter.StartBusAsync(CancellationToken.None);

        var reachable = await adapter.IsTransportReachableAsync(CancellationToken.None);

        Assert.True(reachable);
    }

    [Fact]
    public async Task IsTransportReachableAsync_WhenNotStarted_ReturnsFalseWithoutProbingBroker()
    {
        var bus = new FakeBusControl(health: BusHealthStatus.Healthy);
        var adapter = CreateAdapter(bus, MessagingMode.PublishSubscribe);

        var reachable = await adapter.IsTransportReachableAsync(CancellationToken.None);

        Assert.False(reachable);
        Assert.Equal(0, bus.CheckHealthCount);
    }

    [Fact]
    public async Task IsTransportReachableAsync_WhenHealthCheckThrows_ReturnsFalseAndDoesNotPropagate()
    {
        var bus = new FakeBusControl(throwOnCheckHealth: new InvalidOperationException("broker down"));
        var adapter = CreateAdapter(bus, MessagingMode.PublishSubscribe);
        await adapter.StartBusAsync(CancellationToken.None);

        var reachable = await adapter.IsTransportReachableAsync(CancellationToken.None);

        Assert.False(reachable);
    }

    [Fact]
    public async Task IsTransportReachableAsync_WhenHealthCheckExceedsBudget_ReturnsFalseOnTimeout()
    {
        // The injected check blocks past the adapter's ≤ 1s reachability budget, so the timeout must
        // win and the probe must answer false without waiting for the slow check. The adapter ABANDONS
        // rather than cancels the losing CheckHealth task, so the fake's block would otherwise outlive
        // the test and park a thread-pool thread across sibling tests. The fake is now self-releasing:
        // its wait is an upper-bounded wait on a ManualResetEventSlim that Dispose() signals, so the
        // `using` below caps the actual block to this test's lifetime — no pool thread leaks into other
        // tests. 2s comfortably exceeds the 1s budget (timeout still wins, bounded-probe behavior still
        // proven, elapsed < 5s still holds) while the thread is reclaimed the instant the fake disposes.
        using var bus = new FakeBusControl(checkHealthDelay: TimeSpan.FromSeconds(2));
        var adapter = CreateAdapter(bus, MessagingMode.PublishSubscribe);
        await adapter.StartBusAsync(CancellationToken.None);

        var start = DateTimeOffset.UtcNow;
        var reachable = await adapter.IsTransportReachableAsync(CancellationToken.None);
        var elapsed = DateTimeOffset.UtcNow - start;

        Assert.False(reachable);
        Assert.True(elapsed < TimeSpan.FromSeconds(5), $"probe was not bounded: took {elapsed}.");
    }

    // ----- Start failure (Req 5.5) -------------------------------------------------------------

    [Fact]
    public async Task StartBusAsync_WhenBusStartThrows_SurfacesErrorAndLeavesAdapterNotStarted()
    {
        var failure = new InvalidOperationException("broker unreachable");
        var bus = new FakeBusControl(throwOnStart: failure, health: BusHealthStatus.Healthy);
        var adapter = CreateAdapter(bus, MessagingMode.PublishSubscribe);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => adapter.StartBusAsync(CancellationToken.None));

        Assert.Same(failure, thrown);

        // Not-started: the next reachability probe must answer false without touching the broker,
        // which is only possible when the start flag was never set.
        var reachable = await adapter.IsTransportReachableAsync(CancellationToken.None);
        Assert.False(reachable);
        Assert.Equal(0, bus.CheckHealthCount);
    }

    // ----- Helpers -----------------------------------------------------------------------------

    private static RabbitMqBusAdapter CreateAdapter(FakeBusControl bus, MessagingMode mode) =>
        new(bus, mode, QueueName, Host, BuildObservability(), NullLogger<RabbitMqBusAdapter>.Instance);

    private static ObservabilityContext BuildObservability() =>
        new(
            new NoOpMessagingLog(),
            new NoOpMessagingMetrics(),
            new MessagingTraceSource(tracingEnabled: false),
            new PayloadRedactor());
}

/// <summary>
/// A broker-free <see cref="IBusControl"/> double recording exactly the few operations the adapter's
/// seam uses — start, stop, health check, send-endpoint resolution, publish, and send — so the mode →
/// endpoint, reachability, and start-failure behaviors are observable without a live RabbitMQ broker.
/// Every other interface member is unused by the adapter and throws <see cref="NotSupportedException"/>
/// to keep the double honest: if the adapter ever starts depending on one, the test fails loudly
/// rather than passing against an accidental no-op.
/// </summary>
internal sealed class FakeBusControl : IBusControl, IDisposable
{
    private readonly Exception? _throwOnStart;
    private readonly Exception? _throwOnCheckHealth;
    private readonly TimeSpan _checkHealthDelay;
    private readonly BusHealthStatus _health;

    // Signaled on Dispose() so any probe thread still parked in CheckHealth() returns immediately
    // instead of sleeping out the full delay. This is what makes the slow fake non-leaking: the
    // adapter abandons (never cancels) the losing CheckHealth task, so without this release the
    // blocked pool thread would outlive the test and could starve a sibling reachability probe.
    private readonly ManualResetEventSlim _release = new(initialState: false);

    public FakeBusControl(
        BusHealthStatus health = BusHealthStatus.Healthy,
        Exception? throwOnStart = null,
        Exception? throwOnCheckHealth = null,
        TimeSpan checkHealthDelay = default)
    {
        _health = health;
        _throwOnStart = throwOnStart;
        _throwOnCheckHealth = throwOnCheckHealth;
        _checkHealthDelay = checkHealthDelay;
    }

    public int PublishCount { get; private set; }
    public int SendCount { get; private set; }
    public int CheckHealthCount { get; private set; }
    public Uri? LastSendEndpointUri { get; private set; }

    public Uri Address => new("loopback://localhost/");

    // ----- IBusControl lifecycle ---------------------------------------------------------------

    public Task<BusHandle> StartAsync(CancellationToken cancellationToken = default)
    {
        if (_throwOnStart is not null)
        {
            throw _throwOnStart;
        }

        return Task.FromResult<BusHandle>(new FakeBusHandle());
    }

    public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    // ----- IBus health -------------------------------------------------------------------------

    public BusHealthResult CheckHealth()
    {
        CheckHealthCount++;

        if (_checkHealthDelay > TimeSpan.Zero)
        {
            // Simulate a broker that answers slower than the adapter's reachability budget. Blocking is
            // intentional: the adapter runs this on a background Task.Run and must time out around it,
            // so the block has to exceed the ≤ 1s budget for the timeout path to be exercised. The wait
            // is UPPER-BOUNDED by the delay AND released early when the fake is disposed, so it returns
            // at whichever comes first — capping the actual block to the test's lifetime instead of
            // leaking a parked pool thread into sibling tests.
            _release.Wait(_checkHealthDelay);
        }

        if (_throwOnCheckHealth is not null)
        {
            throw _throwOnCheckHealth;
        }

        var endpoints = new Dictionary<string, EndpointHealthResult>();
        return _health == BusHealthStatus.Healthy
            ? BusHealthResult.Healthy("ok", endpoints)
            : BusHealthResult.Unhealthy("down", new InvalidOperationException("unhealthy"), endpoints);
    }

    // ----- IDisposable -------------------------------------------------------------------------

    /// <summary>
    /// Releases any probe thread still parked in <see cref="CheckHealth"/> so it returns immediately
    /// instead of sleeping out the full configured delay, then disposes the gate. Disposing the fake
    /// at the end of a slow-check test caps the injected block to the test's lifetime, so the adapter's
    /// abandoned (never cancelled) <c>CheckHealth</c> task cannot leak a blocked pool thread into
    /// sibling tests.
    /// </summary>
    public void Dispose()
    {
        _release.Set();
        _release.Dispose();
    }

    // ----- ISendEndpointProvider ---------------------------------------------------------------

    public Task<ISendEndpoint> GetSendEndpoint(Uri address)
    {
        LastSendEndpointUri = address;
        return Task.FromResult<ISendEndpoint>(new FakeSendEndpoint(this));
    }

    // ----- IPublishEndpoint --------------------------------------------------------------------

    public Task Publish<T>(T message, CancellationToken cancellationToken = default) where T : class =>
        RecordPublish();

    public Task Publish<T>(T message, IPipe<PublishContext<T>> publishPipe, CancellationToken cancellationToken = default) where T : class =>
        RecordPublish();

    public Task Publish<T>(T message, IPipe<PublishContext> publishPipe, CancellationToken cancellationToken = default) where T : class =>
        RecordPublish();

    public Task Publish(object message, CancellationToken cancellationToken = default) => RecordPublish();

    public Task Publish(object message, Type messageType, CancellationToken cancellationToken = default) => RecordPublish();

    public Task Publish(object message, IPipe<PublishContext> publishPipe, CancellationToken cancellationToken = default) => RecordPublish();

    public Task Publish(object message, Type messageType, IPipe<PublishContext> publishPipe, CancellationToken cancellationToken = default) => RecordPublish();

    public Task Publish<T>(object values, CancellationToken cancellationToken = default) where T : class => RecordPublish();

    public Task Publish<T>(object values, IPipe<PublishContext<T>> publishPipe, CancellationToken cancellationToken = default) where T : class => RecordPublish();

    public Task Publish<T>(object values, IPipe<PublishContext> publishPipe, CancellationToken cancellationToken = default) where T : class => RecordPublish();

    private Task RecordPublish()
    {
        PublishCount++;
        return Task.CompletedTask;
    }

    private Task RecordSend(Uri? endpointUri)
    {
        SendCount++;
        return Task.CompletedTask;
    }

    // ----- IPublishEndpointProvider ------------------------------------------------------------

    public Task<ISendEndpoint> GetPublishSendEndpoint<T>() where T : class =>
        throw new NotSupportedException();

    // ----- IPublishObserverConnector -----------------------------------------------------------

    public ConnectHandle ConnectPublishObserver(IPublishObserver observer) =>
        throw new NotSupportedException();

    // ----- ISendObserverConnector --------------------------------------------------------------

    public ConnectHandle ConnectSendObserver(ISendObserver observer) =>
        throw new NotSupportedException();

    // ----- IReceiveConnector / IConsumePipeConnector / IConsumeMessageObserverConnector --------

    public HostReceiveEndpointHandle ConnectReceiveEndpoint(IEndpointDefinition definition, IEndpointNameFormatter? endpointNameFormatter, Action<IReceiveEndpointConfigurator>? configure = null) =>
        throw new NotSupportedException();

    public HostReceiveEndpointHandle ConnectReceiveEndpoint(string queueName, Action<IReceiveEndpointConfigurator>? configureEndpoint = null) =>
        throw new NotSupportedException();

    // ----- IBus / IReceiveEndpointConnector probe ----------------------------------------------

    public IBusTopology Topology => throw new NotSupportedException();

    void IProbeSite.Probe(ProbeContext context) => throw new NotSupportedException();

    public ConnectHandle ConnectConsumePipe<T>(IPipe<ConsumeContext<T>> pipe) where T : class => throw new NotSupportedException();

    public ConnectHandle ConnectConsumePipe<T>(IPipe<ConsumeContext<T>> pipe, ConnectPipeOptions options) where T : class => throw new NotSupportedException();

    public ConnectHandle ConnectRequestPipe<T>(Guid requestId, IPipe<ConsumeContext<T>> pipe) where T : class => throw new NotSupportedException();

    public ConnectHandle ConnectConsumeMessageObserver<T>(IConsumeMessageObserver<T> observer) where T : class => throw new NotSupportedException();

    public ConnectHandle ConnectConsumeObserver(IConsumeObserver observer) => throw new NotSupportedException();

    public ConnectHandle ConnectReceiveObserver(IReceiveObserver observer) => throw new NotSupportedException();

    public ConnectHandle ConnectReceiveEndpointObserver(IReceiveEndpointObserver observer) => throw new NotSupportedException();

    public ConnectHandle ConnectEndpointConfigurationObserver(IEndpointConfigurationObserver observer) => throw new NotSupportedException();

    public ConnectHandle ConnectBusObserver(IBusObserver observer) => throw new NotSupportedException();

    private sealed class FakeBusHandle : BusHandle
    {
        // The adapter discards the returned BusHandle; Ready is never awaited, so it stays unsupported.
        public Task<BusReady> Ready => throw new NotSupportedException();

        public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    private sealed class FakeSendEndpoint : ISendEndpoint
    {
        private readonly FakeBusControl _owner;

        public FakeSendEndpoint(FakeBusControl owner) => _owner = owner;

        public ConnectHandle ConnectSendObserver(ISendObserver observer) => throw new NotSupportedException();

        public Task Send<T>(T message, CancellationToken cancellationToken = default) where T : class =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send<T>(T message, IPipe<SendContext<T>> pipe, CancellationToken cancellationToken = default) where T : class =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send<T>(T message, IPipe<SendContext> pipe, CancellationToken cancellationToken = default) where T : class =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send(object message, CancellationToken cancellationToken = default) =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send(object message, Type messageType, CancellationToken cancellationToken = default) =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send(object message, IPipe<SendContext> pipe, CancellationToken cancellationToken = default) =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send(object message, Type messageType, IPipe<SendContext> pipe, CancellationToken cancellationToken = default) =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send<T>(object values, CancellationToken cancellationToken = default) where T : class =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send<T>(object values, IPipe<SendContext<T>> pipe, CancellationToken cancellationToken = default) where T : class =>
            _owner.RecordSend(_owner.LastSendEndpointUri);

        public Task Send<T>(object values, IPipe<SendContext> pipe, CancellationToken cancellationToken = default) where T : class =>
            _owner.RecordSend(_owner.LastSendEndpointUri);
    }
}
