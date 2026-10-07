using Messaging.Abstractions;

namespace Messaging.Startup;

/// <summary>
/// Holds the outcome of startup validation so the embedding application can expose the active
/// <see cref="MessagingMode"/> on its readiness/informational endpoint once the Messaging Core has
/// started successfully (Req 1.7). Before <see cref="MarkStarted"/> runs, the Messaging Core is not
/// yet processing-ready; after it runs, <see cref="ActiveMode"/> reports the single mode the instance
/// started in, fixed for the instance lifetime (Req 1.4).
/// </summary>
/// <remarks>
/// Registered as a singleton and written exactly once by <see cref="MessagingStartupHostedService"/>
/// after a successful validate-and-resolve. It is the readiness seam for the active mode specifically
/// (Req 1.7); transport reachability is reported separately by <c>MessagingHealthChecks</c>. Access is
/// serialized by a lock so a readiness probe racing startup observes a consistent started/not-started
/// view. The active mode cannot change after startup — a second <see cref="MarkStarted"/> for a
/// different mode is rejected (Req 1.4).
/// </remarks>
public sealed class MessagingReadinessState
{
    private readonly object _gate = new();
    private MessagingMode? _activeMode;

    /// <summary>
    /// Whether startup validation has completed and the active provider has been resolved, meaning the
    /// Messaging Core has entered a processing-ready state (Req 2.6). Thread-safe read.
    /// </summary>
    public bool IsStarted
    {
        get
        {
            lock (_gate)
            {
                return _activeMode is not null;
            }
        }
    }

    /// <summary>
    /// The active messaging mode the instance started in, exposed on readiness once startup succeeds
    /// (Req 1.7). Null until <see cref="MarkStarted"/> runs. Thread-safe read.
    /// </summary>
    public MessagingMode? ActiveMode
    {
        get
        {
            lock (_gate)
            {
                return _activeMode;
            }
        }
    }

    /// <summary>
    /// Records that startup validation passed and fixes the active mode for the instance lifetime
    /// (Req 1.4, 1.7). Idempotent for the same mode; rejects a different mode because the mode cannot
    /// change after startup.
    /// </summary>
    /// <param name="activeMode">The resolved active mode to expose on readiness.</param>
    /// <exception cref="InvalidOperationException">
    /// If called again with a different mode than the one already started (Req 1.4).
    /// </exception>
    public void MarkStarted(MessagingMode activeMode)
    {
        lock (_gate)
        {
            if (_activeMode is not null && _activeMode.Value != activeMode)
            {
                throw new InvalidOperationException(
                    $"The active messaging mode is already '{_activeMode.Value}' and cannot change to "
                    + $"'{activeMode}' after startup.");
            }

            _activeMode = activeMode;
        }
    }
}
