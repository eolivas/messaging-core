using System;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Configuration;
using Messaging.Lifecycle;
using Xunit;

namespace Messaging.Tests.Lifecycle;

// Unit tests for stop-accepting-new-messages on shutdown (task 18.4).
// Covers Req 10.5: once shutdown begins the host stops accepting new messages while already
// in-flight work drains. The InFlightTracker owns the accepting gate, and the
// GracefulShutdownCoordinator flips it via StopAccepting() before draining.
[Trait("Feature", "messaging-core")]
public class StopAcceptingOnShutdownTests
{
    [Fact]
    public void IsAccepting_BeforeShutdown_IsTrue()
    {
        var tracker = new InFlightTracker();

        Assert.True(tracker.IsAccepting);
    }

    [Fact]
    public void TryBeginProcessing_BeforeShutdown_AdmitsWork()
    {
        var tracker = new InFlightTracker();

        var admitted = tracker.TryBeginProcessing();

        Assert.True(admitted);
        Assert.Equal(1, tracker.InFlightCount);
    }

    [Fact]
    public void IsAccepting_AfterStopAccepting_IsFalse()
    {
        var tracker = new InFlightTracker();

        tracker.StopAccepting();

        Assert.False(tracker.IsAccepting);
    }

    [Fact]
    public void TryBeginProcessing_AfterStopAccepting_RejectsNewWork()
    {
        var tracker = new InFlightTracker();

        tracker.StopAccepting();

        var admitted = tracker.TryBeginProcessing();
        Assert.False(admitted);
        Assert.Equal(0, tracker.InFlightCount);
    }

    [Fact]
    public void TryBeginProcessing_AfterStopAccepting_LeavesExistingInFlightWorkToDrain()
    {
        var tracker = new InFlightTracker();
        Assert.True(tracker.TryBeginProcessing());

        tracker.StopAccepting();

        // The already-admitted message is still counted and may complete...
        Assert.Equal(1, tracker.InFlightCount);
        // ...but no new message is admitted.
        Assert.False(tracker.TryBeginProcessing());
        Assert.Equal(1, tracker.InFlightCount);

        tracker.CompleteProcessing();
        Assert.Equal(0, tracker.InFlightCount);
    }

    [Fact]
    public void StopAccepting_CalledTwice_IsIdempotent()
    {
        var tracker = new InFlightTracker();

        tracker.StopAccepting();
        tracker.StopAccepting();

        Assert.False(tracker.IsAccepting);
        Assert.False(tracker.TryBeginProcessing());
    }

    [Fact]
    public async Task ShutdownAsync_OnceBegun_StopsTrackerAcceptingNewMessages()
    {
        var tracker = new InFlightTracker();
        var coordinator = new GracefulShutdownCoordinator(
            tracker,
            new LifecycleConfig(),
            pollInterval: TimeSpan.FromMilliseconds(1),
            delay: static (_, _) => Task.CompletedTask,
            elapsed: static () => TimeSpan.Zero);

        // Pre-shutdown the host accepts new messages.
        Assert.True(tracker.IsAccepting);

        var result = await coordinator.ShutdownAsync(CancellationToken.None);

        // After shutdown the host no longer accepts new messages (Req 10.5).
        Assert.False(tracker.IsAccepting);
        Assert.False(tracker.TryBeginProcessing());
        // Nothing was in flight, so the drain completed cleanly.
        Assert.False(result.TimedOut);
        Assert.Equal(0, result.IncompleteInFlightCount);
    }
}
