using Messaging.Contracts;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// The seam the batch adapter pulls from: a scheduled/triggered read of a <b>bounded window</b> of
/// envelopes starting at the last checkpoint. This is the shallow port the shell plugs into; the
/// concrete batch mechanics (how a window is read from the configured batch source — a table, a file
/// drop, an object-store prefix) are owned by a later spec.
/// </summary>
/// <remarks>
/// A window is bounded by <see cref="BatchProviderSettings.WindowSize"/>: an implementation returns at
/// most that many envelopes and an empty window when the source is drained. The adapter never assumes
/// the window is full — a short or empty window ends the current pull cycle.
/// </remarks>
public interface IBatchSource
{
    /// <summary>
    /// Pulls the next bounded window of envelopes to process, up to <paramref name="windowSize"/>
    /// items, resuming from the last checkpoint. Returns an empty window when nothing is pending.
    /// </summary>
    /// <param name="windowSize">The maximum number of envelopes to return in the window.</param>
    /// <param name="cancellationToken">Token to cancel the pull.</param>
    Task<IReadOnlyList<MessageEnvelope>> PullWindowAsync(int windowSize, CancellationToken cancellationToken);
}
