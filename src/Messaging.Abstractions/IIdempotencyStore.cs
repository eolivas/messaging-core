namespace Messaging.Abstractions;

/// <summary>
/// Records processed idempotency keys and detects duplicate deliveries so at-least-once delivery
/// does not cause duplicate side effects (Req 7).
/// </summary>
public interface IIdempotencyStore
{
    /// <summary>
    /// Returns whether the given idempotency key has already been recorded as processed (Req 7.2).
    /// </summary>
    /// <param name="idempotencyKey">The key to look up.</param>
    /// <param name="cancellationToken">Token to cancel the lookup.</param>
    Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken);

    /// <summary>
    /// Records the given key as processed for the retention window. A recording failure surfaces so
    /// the message is not acknowledged and the stored record is left unchanged (Req 7.3, 7.5).
    /// </summary>
    /// <param name="idempotencyKey">The key to record.</param>
    /// <param name="retention">How long the recorded key is retained.</param>
    /// <param name="cancellationToken">Token to cancel the recording.</param>
    Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken);
}
