using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Lifecycle;
using Messaging.Tests.Fakes;
using Xunit;

namespace Messaging.Tests.Lifecycle;

// Integration-style endpoint and timing tests (task 22.5).
// Validates: Requirements 1.7, 10.1, 10.2, 10.3, 10.4, 10.6.
//
// The library does not stand up its own HTTP endpoints — the embedding application wires the
// Application-layer health-check logic (MessagingHealthChecks) and the consumer-host drain
// (GracefulShutdownCoordinator over InFlightTracker) into its own host. These tests exercise that
// logic end-to-end against the in-process FakeMessagingProvider, under REAL timing, so the 1-second
// probe bounds (Req 10.2, 10.3) and the drain-within-shutdown-timeout behaviour (Req 10.6) are
// asserted the way an operator's probe/orchestrator would observe them — no Docker required.
[Trait("Feature", "messaging-core")]
public class EndpointAndTimingIntegrationTests
{
    private const MessagingMode ActiveMode = MessagingMode.Streaming;

    // The probe-response bound the requirements mandate (Req 10.2, 10.3). The in-process fake answers
    // far inside this; the assertion guards against a regression that would make a probe block.
    private static readonly TimeSpan ProbeBound = TimeSpan.FromSeconds(1);

    private static FakeMessagingProvider CreateProvider() =>
        FakeMessagingProvider.CreatePublishOnly(ActiveMode);

    // ----- Liveness (Req 10.1, 10.2) ----------------------------------------------------------

    [Fact]
    public async Task LivenessAsync_WhileProcessRunning_ReturnsHealthyWithinOneSecond()
    {
        // Arrange: the process is running, so liveness must report healthy without any transport call.
        var checks = new MessagingHealthChecks(CreateProvider());

        // Act: measure the real wall-clock cost of the probe.
        var stopwatch = Stopwatch.StartNew();
        var result = await checks.LivenessAsync(CancellationToken.None);
        stopwatch.Stop();

        // Assert: healthy (Req 10.1) and answered within the 1s bound (Req 10.2).
        Assert.True(result.IsHealthy);
        Assert.True(
            stopwatch.Elapsed < ProbeBound,
            $"Liveness took {stopwatch.Elapsed} which exceeds the {ProbeBound} bound (Req 10.2).");
    }

    // ----- Readiness: reachable (Req 10.3, 1.7) -----------------------------------------------

    [Fact]
    public async Task ReadinessAsync_WhenTransportReachable_ReturnsReadyWithinOneSecond()
    {
        // Arrange: the fake transport reports reachable (its default health).
        var provider = CreateProvider();
        provider.SetTransportHealth(new TransportHealth(Reachable: true, UnavailableDependency: null));
        var checks = new MessagingHealthChecks(provider);

        // Act.
        var stopwatch = Stopwatch.StartNew();
        var result = await checks.ReadinessAsync(CancellationToken.None);
        stopwatch.Stop();

        // Assert: ready (Req 10.3), within the 1s bound (Req 10.3), carrying no unavailable dependency.
        Assert.True(result.IsReady);
        Assert.Null(result.UnavailableDependency);
        Assert.True(
            stopwatch.Elapsed < ProbeBound,
            $"Readiness took {stopwatch.Elapsed} which exceeds the {ProbeBound} bound (Req 10.3).");
    }

    // ----- Readiness: unreachable names the dependency (Req 10.4) -----------------------------

    [Fact]
    public async Task ReadinessAsync_WhenTransportUnreachable_ReturnsNotReadyNamingDependency()
    {
        // Arrange: the fake transport reports unreachable, naming the dependency that is down.
        const string unavailable = "streaming-broker";
        var provider = CreateProvider();
        provider.SetTransportHealth(new TransportHealth(Reachable: false, UnavailableDependency: unavailable));
        var checks = new MessagingHealthChecks(provider);

        // Act.
        var result = await checks.ReadinessAsync(CancellationToken.None);

        // Assert: not-ready and the response names the unavailable dependency (Req 10.4).
        Assert.False(result.IsReady);
        Assert.Equal(unavailable, result.UnavailableDependency);
    }

    // ----- Readiness exposes the active mode (Req 1.7) ----------------------------------------

    [Fact]
    public async Task ReadinessAsync_Always_ExposesActiveMode()
    {
        // Arrange: a provider serving a known mode; drive both the ready and the not-ready path to
        // show the active mode is carried on every readiness response (Req 1.7).
        var provider = CreateProvider();
        var checks = new MessagingHealthChecks(provider);

        provider.SetTransportHealth(new TransportHealth(Reachable: true, UnavailableDependency: null));
        var whenReady = await checks.ReadinessAsync(CancellationToken.None);

        provider.SetTransportHealth(new TransportHealth(Reachable: false, UnavailableDependency: "broker"));
        var whenNotReady = await checks.ReadinessAsync(CancellationToken.None);

        // Assert: the active mode is reported regardless of reachability (Req 1.7).
        Assert.Equal(ActiveMode, whenReady.ActiveMode);
        Assert.Equal(ActiveMode, whenNotReady.ActiveMode);
    }

    // ----- Graceful drain completes in-flight within the shutdown timeout (Req 10.6) ----------

    [Fact]
    public async Task StopAsync_WithInFlightMessages_DrainsWithinConfiguredShutdownTimeout()
    {
        // Arrange: a consumer host wired to a real in-flight tracker and the real graceful-shutdown
        // coordinator under REAL timing, holding work that is still in flight when shutdown begins.
        var lifecycle = new LifecycleConfig { ShutdownTimeoutSeconds = 5 };
        var host = new DrainingConsumerHost(lifecycle);
        await host.StartAsync(CancellationToken.None);

        const int inFlight = 3;
        for (var i = 0; i < inFlight; i++)
        {
            Assert.True(host.AdmitInFlightMessage());
        }

        // A background completer finishes the in-flight work shortly after the drain begins, well
        // inside the 5s timeout, so the drain must complete cleanly rather than time out.
        host.CompleteInFlightAfter(TimeSpan.FromMilliseconds(100));

        // Act: shut down and measure how long the drain actually took.
        var stopwatch = Stopwatch.StartNew();
        await host.StopAsync(CancellationToken.None);
        stopwatch.Stop();

        // Assert: the drain completed within the configured timeout (Req 10.6) with nothing left in
        // flight, no new work was admitted during shutdown (Req 10.5), and the host wound down.
        var shutdownTimeout = TimeSpan.FromSeconds(lifecycle.ShutdownTimeoutSeconds);
        Assert.False(host.LastShutdown!.TimedOut);
        Assert.Equal(0, host.LastShutdown.IncompleteInFlightCount);
        Assert.Equal(0, host.InFlightCount);
        Assert.False(host.AdmitInFlightMessage());
        Assert.Equal(ConsumerHostState.Stopped, host.State);
        Assert.True(
            stopwatch.Elapsed < shutdownTimeout,
            $"Drain took {stopwatch.Elapsed} which exceeds the configured {shutdownTimeout} timeout (Req 10.6).");
    }

    /// <summary>
    /// A minimal in-process <see cref="IMessageConsumerHost"/> that wires the real
    /// <see cref="InFlightTracker"/> and <see cref="GracefulShutdownCoordinator"/> so
    /// <see cref="StopAsync"/> performs a genuine stop-accepting-then-drain under real timing, exactly
    /// as a transport adapter's host would. It exists only to drive the 22.5 drain assertion against
    /// the real coordinator; it keeps no bespoke lifecycle logic beyond the state transitions.
    /// </summary>
    private sealed class DrainingConsumerHost : IMessageConsumerHost
    {
        private readonly InFlightTracker _tracker = new();
        private readonly GracefulShutdownCoordinator _coordinator;
        private readonly Stopwatch _drainClock = new();

        public DrainingConsumerHost(LifecycleConfig lifecycle)
        {
            // Wire the real coordinator with real polling, but source the elapsed window from a
            // stopwatch that starts when the shutdown signal arrives (StopAsync), not at construction.
            // The requirement measures the drain window from the shutdown signal (Req 10.6); starting
            // the clock at StopAsync also keeps the test immune to any construction-to-shutdown lag
            // under heavy parallel test load.
            _coordinator = new GracefulShutdownCoordinator(
                _tracker,
                lifecycle,
                pollInterval: TimeSpan.FromMilliseconds(20),
                delay: Task.Delay,
                elapsed: () => _drainClock.Elapsed);
        }

        public ConsumerHostState State { get; private set; } = ConsumerHostState.Stopped;

        /// <summary>The outcome of the last <see cref="StopAsync"/> drain, for assertion.</summary>
        public ShutdownResult? LastShutdown { get; private set; }

        public int InFlightCount => _tracker.InFlightCount;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            State = ConsumerHostState.Running;
            return Task.CompletedTask;
        }

        /// <summary>Admits a message into processing, mirroring a transport pushing work into the pipeline.</summary>
        public bool AdmitInFlightMessage() => _tracker.TryBeginProcessing();

        /// <summary>
        /// Schedules the currently in-flight messages to complete after <paramref name="delay"/>,
        /// simulating handlers finishing their work while the drain waits. Runs on a dedicated
        /// foreground thread rather than the thread pool so the completer cannot be starved by other
        /// parallel tests — the point of the test is the drain policy, not thread-pool scheduling.
        /// </summary>
        public void CompleteInFlightAfter(TimeSpan delay)
        {
            var toComplete = _tracker.InFlightCount;
            var completer = new Thread(() =>
            {
                Thread.Sleep(delay);
                for (var i = 0; i < toComplete; i++)
                {
                    _tracker.CompleteProcessing();
                }
            })
            {
                IsBackground = true,
                Name = "test-inflight-completer",
            };
            completer.Start();
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            State = ConsumerHostState.Stopping;
            // Start the drain window at the shutdown signal, so the coordinator measures elapsed time
            // from here against the configured timeout (Req 10.6).
            _drainClock.Restart();
            LastShutdown = await _coordinator.ShutdownAsync(cancellationToken).ConfigureAwait(false);
            State = ConsumerHostState.Stopped;
        }
    }
}
