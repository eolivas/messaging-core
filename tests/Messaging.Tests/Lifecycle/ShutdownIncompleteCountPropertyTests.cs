using System;
using System.Threading;
using System.Threading.Tasks;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Messaging.Configuration;
using Messaging.Lifecycle;

namespace Messaging.Tests.Lifecycle;

// Feature: messaging-core, Property 25: Shutdown records the exact incomplete in-flight count.
// Validates: Requirement 10.7.
//
// For ANY count N >= 0 of in-flight messages that NEVER complete within the shutdown timeout, the
// graceful shutdown drain terminates and the recorded incomplete-in-flight count equals EXACTLY N
// and is NEVER negative:
//   N > 0  — the drain cannot reach zero before the timeout elapses, so the result is TimedOut with
//            IncompleteInFlightCount == N (Req 10.7).
//   N == 0 — there is nothing to drain, so the drain completes cleanly (not timed out) with
//            IncompleteInFlightCount == 0. The non-negative-count invariant still holds.
//
// Timing is injected: an instant no-op delay and an elapsed-time source that immediately reports a
// value past the configured timeout, so the drain loop times out deterministically with no
// wall-clock waits.
[Trait("Feature", "messaging-core")]
[Trait("Property", "25")]
public class ShutdownIncompleteCountPropertyTests
{
    [Property(MaxTest = 200, QuietOnSuccess = true, Arbitrary = new[] { typeof(ShutdownIncompleteCountPropertyTests) })]
    public Property Shutdown_RecordsExactIncompleteInFlightCount(int inFlightCount)
    {
        // Arrange: a tracker holding N in-flight messages that never complete.
        var tracker = new InFlightTracker();
        for (var i = 0; i < inFlightCount; i++)
        {
            tracker.TryBeginProcessing();
        }

        var lifecycle = new LifecycleConfig(); // default 30s timeout; never actually waited for.

        // Elapsed source jumps immediately past any valid timeout (max 300s) so the drain loop sees
        // the window as already expired on its first non-empty check — a deterministic timeout.
        var coordinator = new GracefulShutdownCoordinator(
            tracker,
            lifecycle,
            pollInterval: TimeSpan.Zero,
            delay: static (_, _) => Task.CompletedTask,
            elapsed: static () => TimeSpan.FromDays(1));

        // Act.
        var result = coordinator
            .ShutdownAsync(CancellationToken.None)
            .GetAwaiter()
            .GetResult();

        // Assert: the recorded count is exact and non-negative; timed-out iff work remained.
        var expectedTimedOut = inFlightCount > 0;
        var holds =
            result.IncompleteInFlightCount == inFlightCount   // exact count (Req 10.7)
            && result.IncompleteInFlightCount >= 0            // never negative (Req 10.7)
            && result.TimedOut == expectedTimedOut;

        return holds
            .ToProperty()
            .Label(
                $"inFlight={inFlightCount} => timedOut={result.TimedOut} (expected {expectedTimedOut}), " +
                $"recordedCount={result.IncompleteInFlightCount}")
            .Classify(inFlightCount == 0, "empty drain (clean)")
            .Classify(inFlightCount > 0, "non-empty drain (timeout)");
    }

    // ----- Generators -------------------------------------------------------------------------

    public static Arbitrary<int> InFlightCountArbitrary() => Arb.From(InFlightCountGen());

    /// <summary>
    /// Generates a non-negative in-flight count across [0, 1000]. Zero exercises the clean-drain
    /// edge where the non-negative invariant must still hold; positive values exercise the
    /// exact-count-on-timeout path. The upper bound keeps each case fast.
    /// </summary>
    private static Gen<int> InFlightCountGen() => Gen.Choose(0, 1000);
}
