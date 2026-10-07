namespace Messaging.Lifecycle;

/// <summary>
/// Tracks the number of in-flight messages a consumer host is currently processing and whether the
/// host is still accepting new ones (Req 10.5, 10.7). This is the single source of truth the
/// <see cref="GracefulShutdownCoordinator"/> drains against; isolating it here keeps the counting
/// and the "stop accepting" gate testable without a real transport (SRP).
/// </summary>
/// <remarks>
/// <para>
/// Lifecycle: a message enters processing via <see cref="TryBeginProcessing"/> and leaves via
/// <see cref="CompleteProcessing"/>. Once <see cref="StopAccepting"/> is called,
/// <see cref="TryBeginProcessing"/> refuses new work (Req 10.5) while already-started messages are
/// allowed to drain. <see cref="IsAccepting"/> and <see cref="InFlightCount"/> are observable
/// throughout so a drain loop and the 18.4 unit test can watch the counts without a broker.
/// </para>
/// <para>
/// All operations are serialized by a lock so concurrent begin/complete calls from multiple consume
/// threads cannot corrupt the count or race the accepting gate.
/// </para>
/// </remarks>
public sealed class InFlightTracker
{
    private readonly object _gate = new();
    private int _inFlight;
    private bool _accepting = true;

    /// <summary>The number of messages currently being processed. Never negative. Thread-safe read.</summary>
    public int InFlightCount
    {
        get
        {
            lock (_gate)
            {
                return _inFlight;
            }
        }
    }

    /// <summary>
    /// Whether the tracker is still accepting new messages. Starts true and flips to false once
    /// <see cref="StopAccepting"/> is called on shutdown (Req 10.5). Thread-safe read.
    /// </summary>
    public bool IsAccepting
    {
        get
        {
            lock (_gate)
            {
                return _accepting;
            }
        }
    }

    /// <summary>
    /// Attempts to admit a new message into processing. Succeeds and increments the in-flight count
    /// while accepting; refuses (returns false) once the host has stopped accepting new messages on
    /// shutdown (Req 10.5).
    /// </summary>
    /// <returns>True if the message was admitted and must later be released with <see cref="CompleteProcessing"/>; false if rejected.</returns>
    public bool TryBeginProcessing()
    {
        lock (_gate)
        {
            if (!_accepting)
            {
                return false;
            }

            _inFlight++;
            return true;
        }
    }

    /// <summary>
    /// Releases a message previously admitted by <see cref="TryBeginProcessing"/>, decrementing the
    /// in-flight count. Each admitted message must be completed exactly once.
    /// </summary>
    /// <exception cref="InvalidOperationException">If called more times than there are in-flight messages (a tracking bug).</exception>
    public void CompleteProcessing()
    {
        lock (_gate)
        {
            if (_inFlight == 0)
            {
                throw new InvalidOperationException(
                    "CompleteProcessing was called with no in-flight messages; begin/complete calls are unbalanced.");
            }

            _inFlight--;
        }
    }

    /// <summary>
    /// Stops accepting new messages (Req 10.5). Idempotent: calling it again has no effect. After
    /// this call <see cref="TryBeginProcessing"/> always returns false while in-flight messages
    /// drain.
    /// </summary>
    public void StopAccepting()
    {
        lock (_gate)
        {
            _accepting = false;
        }
    }
}
