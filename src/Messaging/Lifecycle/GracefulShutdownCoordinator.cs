using Messaging.Configuration;

namespace Messaging.Lifecycle;

/// <summary>
/// Drives graceful shutdown of a consumer host (Req 10.5–10.7). On a shutdown signal it stops the
/// host accepting new messages (Req 10.5) and then drains the in-flight messages tracked by an
/// <see cref="InFlightTracker"/> within the configured shutdown timeout (default 30s, bounded
/// 1–300s — Req 10.6). If the timeout elapses before the drain completes, it reports a timed-out
/// <see cref="ShutdownResult"/> carrying the exact non-negative count of messages still in flight so
/// the host can be terminated and the count recorded (Req 10.7).
/// </summary>
/// <remarks>
/// <para>
/// SRP: this type owns only the stop-accepting-then-drain-within-timeout policy. The in-flight
/// counting and accepting gate live in <see cref="InFlightTracker"/>; the actual host teardown is
/// the caller's concern. The coordinator never terminates the host itself — it reports whether the
/// timeout elapsed so the caller performs termination, keeping this logic transport-free and
/// unit-testable (supports Property 25 and the 18.4 unit test).
/// </para>
/// <para>
/// Timing is injectable (mirroring <c>RetryExecutor</c>): the polling delay and the elapsed-time
/// source are supplied so a test can drive the drain loop deterministically without wall-clock
/// waits. The default constructor uses <see cref="Task.Delay(TimeSpan, CancellationToken)"/> and a
/// monotonic stopwatch.
/// </para>
/// </remarks>
public sealed class GracefulShutdownCoordinator
{
    private static readonly TimeSpan DefaultPollInterval = TimeSpan.FromMilliseconds(50);

    private readonly InFlightTracker _tracker;
    private readonly LifecycleConfig _lifecycle;
    private readonly TimeSpan _pollInterval;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<TimeSpan> _elapsed;

    /// <summary>
    /// Creates the coordinator with real timing: polling via <see cref="Task.Delay(TimeSpan, CancellationToken)"/>
    /// and a monotonic elapsed-time source.
    /// </summary>
    /// <param name="tracker">The in-flight tracker the drain observes (Req 10.5, 10.7).</param>
    /// <param name="lifecycle">Lifecycle settings supplying the shutdown timeout (Req 10.6).</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public GracefulShutdownCoordinator(InFlightTracker tracker, LifecycleConfig lifecycle)
        : this(tracker, lifecycle, DefaultPollInterval, Task.Delay, CreateStopwatchElapsed())
    {
    }

    /// <summary>
    /// Creates the coordinator with injected timing so a test can drain deterministically without
    /// wall-clock waits while still exercising the timeout decision.
    /// </summary>
    /// <param name="tracker">The in-flight tracker the drain observes (Req 10.5, 10.7).</param>
    /// <param name="lifecycle">Lifecycle settings supplying the shutdown timeout (Req 10.6).</param>
    /// <param name="pollInterval">How long to wait between in-flight count checks during the drain.</param>
    /// <param name="delay">The function invoked to wait <paramref name="pollInterval"/> between checks.</param>
    /// <param name="elapsed">Returns the time elapsed since the drain started; compared against the timeout.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public GracefulShutdownCoordinator(
        InFlightTracker tracker,
        LifecycleConfig lifecycle,
        TimeSpan pollInterval,
        Func<TimeSpan, CancellationToken, Task> delay,
        Func<TimeSpan> elapsed)
    {
        _tracker = tracker ?? throw new ArgumentNullException(nameof(tracker));
        _lifecycle = lifecycle ?? throw new ArgumentNullException(nameof(lifecycle));
        _pollInterval = pollInterval;
        _delay = delay ?? throw new ArgumentNullException(nameof(delay));
        _elapsed = elapsed ?? throw new ArgumentNullException(nameof(elapsed));
    }

    /// <summary>
    /// Performs the graceful shutdown drain. First stops the tracker accepting new messages
    /// (Req 10.5), then polls until either every in-flight message completes within the configured
    /// timeout (Req 10.6) or the timeout elapses, in which case the remaining non-negative in-flight
    /// count is reported for the caller to terminate the host and record (Req 10.7).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the drain; cancellation is treated as an immediate timeout.</param>
    /// <returns>
    /// <see cref="ShutdownResult.Drained"/> when the drain completes within the timeout; otherwise a
    /// timed-out result carrying the exact remaining in-flight count.
    /// </returns>
    public async Task<ShutdownResult> ShutdownAsync(CancellationToken cancellationToken)
    {
        // Stop accepting new work before draining so no message started during shutdown can keep
        // the drain from ever completing (Req 10.5).
        _tracker.StopAccepting();

        var timeout = ResolveTimeout();

        while (true)
        {
            var inFlight = _tracker.InFlightCount;
            if (inFlight == 0)
            {
                return ShutdownResult.Drained;
            }

            // Timeout elapsed (or the drain was cancelled): terminate and report the remaining count.
            if (cancellationToken.IsCancellationRequested || _elapsed() >= timeout)
            {
                return new ShutdownResult(timedOut: true, incompleteInFlightCount: _tracker.InFlightCount);
            }

            try
            {
                await _delay(_pollInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Cancellation mid-wait is an immediate timeout: report whatever remains in flight.
                return new ShutdownResult(timedOut: true, incompleteInFlightCount: _tracker.InFlightCount);
            }
        }
    }

    /// <summary>
    /// Resolves the shutdown timeout from configuration, clamping to the documented 1–300s bound so
    /// an unvalidated or out-of-range value never produces a zero/negative or absurd drain window
    /// (Req 10.6). Validated configuration already falls inside this range; the clamp is defensive.
    /// </summary>
    private TimeSpan ResolveTimeout()
    {
        var bounds = ConfigurationBounds.ShutdownTimeoutSeconds;
        var seconds = Math.Clamp(_lifecycle.ShutdownTimeoutSeconds, bounds.Min, bounds.Max);
        return TimeSpan.FromSeconds(seconds);
    }

    private static Func<TimeSpan> CreateStopwatchElapsed()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        return () => stopwatch.Elapsed;
    }
}
