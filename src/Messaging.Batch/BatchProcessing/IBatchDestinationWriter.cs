using Messaging.Contracts;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// The seam the batch publisher writes to: the configured batch destination
/// (<see cref="BatchProviderSettings.Destination"/>). The real write mechanics (append to a table,
/// write a file, put to an object store) are owned by a later spec; the publisher shell depends only
/// on this shallow port so publishing to the batch destination is expressed and testable here.
/// </summary>
public interface IBatchDestinationWriter
{
    /// <summary>
    /// Writes a validated envelope to the batch destination, preserving every field unchanged
    /// (Req 4.4).
    /// </summary>
    /// <param name="envelope">The envelope to write.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task WriteAsync(MessageEnvelope envelope, CancellationToken cancellationToken);
}
