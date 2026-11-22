namespace Demo.Infrastructure.Messaging;

/// <summary>Attempt numbering shared by the handler chain decorators.</summary>
internal static class AttemptTrackerExtensions
{
    /// <summary>
    /// The attempt number about to run for a correlation id: one more than the last failed
    /// attempt recorded by the chain, or 1 when none is recorded. Retries run in-process, so
    /// handler entries equal attempts.
    /// </summary>
    public static int NextAttempt(this AttemptTracker tracker, string correlationId) =>
        tracker.TryGet(correlationId, out var info) && info is not null ? info.Attempt + 1 : 1;
}
