namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// Persists the batch checkpoint — the batch mode's equivalent of a Kafka offset commit or a broker
/// ack. The adapter checkpoints a window only after every item in it has been processed, so a crash
/// mid-window reprocesses from the last checkpointed window (at-least-once, Req 6.7). This is the
/// shallow port the shell plugs into; where the checkpoint is stored is a later-spec concern.
/// </summary>
public interface IBatchCheckpointStore
{
    /// <summary>
    /// Commits the checkpoint for a fully-processed window, advancing the resume point so its items
    /// are excluded from future pulls.
    /// </summary>
    /// <param name="processedCount">The number of items processed in the window just completed.</param>
    /// <param name="cancellationToken">Token to cancel the checkpoint write.</param>
    Task CheckpointAsync(int processedCount, CancellationToken cancellationToken);
}
