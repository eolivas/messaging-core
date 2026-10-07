namespace Messaging.Lifecycle;

/// <summary>
/// The outcome of a graceful shutdown drain (Req 10.6, 10.7). Observable after
/// <see cref="GracefulShutdownCoordinator.ShutdownAsync"/> returns so the embedding application can
/// record how the drain ended and how many in-flight messages were still incomplete when the
/// timeout elapsed.
/// </summary>
public sealed record ShutdownResult
{
    /// <summary>
    /// Creates a shutdown result, guarding the non-negative-count invariant (Req 10.7). A negative
    /// count can only arise from a tracking bug; failing fast here surfaces it rather than reporting
    /// a nonsense value through a readiness/metric surface.
    /// </summary>
    /// <param name="timedOut">Whether the drain timed out and the host was terminated (Req 10.7).</param>
    /// <param name="incompleteInFlightCount">The remaining incomplete in-flight count; must be &gt;= 0.</param>
    /// <exception cref="ArgumentOutOfRangeException">If <paramref name="incompleteInFlightCount"/> is negative.</exception>
    public ShutdownResult(bool timedOut, int incompleteInFlightCount)
    {
        if (incompleteInFlightCount < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(incompleteInFlightCount),
                incompleteInFlightCount,
                "The incomplete in-flight count must be a non-negative integer (Req 10.7).");
        }

        TimedOut = timedOut;
        IncompleteInFlightCount = incompleteInFlightCount;
    }

    /// <summary>
    /// True when the configured shutdown timeout elapsed before all in-flight messages completed, so
    /// the host was terminated (Req 10.7); false when the drain completed within the timeout (Req 10.6).
    /// </summary>
    public bool TimedOut { get; }

    /// <summary>
    /// The count of in-flight messages that had not completed when the drain ended. Always a
    /// non-negative integer (Req 10.7): zero on a clean drain, and the exact remaining count on timeout.
    /// </summary>
    public int IncompleteInFlightCount { get; }

    /// <summary>A clean drain: every in-flight message completed within the timeout (Req 10.6).</summary>
    public static readonly ShutdownResult Drained = new(timedOut: false, incompleteInFlightCount: 0);
}
