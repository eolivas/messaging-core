using Messaging.Contracts;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// The seam the batch adapter runs each pulled envelope through — the batch mode's stand-in for the
/// core consume pipeline (idempotency → bind contract → handler → retry/dead-letter). The real
/// adapter wiring (task 21) supplies an implementation that drives the Application-layer pipeline; the
/// shell depends only on this shallow port so the pull → chunk → process → checkpoint shape is
/// expressed and testable without the deep pipeline.
/// </summary>
public interface IBatchItemProcessor
{
    /// <summary>
    /// Processes a single envelope from the current window through the consume pipeline.
    /// </summary>
    /// <param name="envelope">The envelope to process.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken);
}
