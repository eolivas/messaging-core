using Messaging.Contracts;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// The seam the consumer host runs each consumed envelope through — the streaming mode's stand-in for
/// the core consume pipeline (idempotency → bind contract → handler → retry/dead-letter). The real
/// adapter wiring (task 21) supplies an implementation that drives the Application-layer pipeline; the
/// shell depends only on this shallow port so the consume → process → commit shape is expressed and
/// testable without the deep pipeline.
/// </summary>
public interface IStreamMessageProcessor
{
    /// <summary>
    /// Processes a single consumed envelope through the consume pipeline. The consumer host commits
    /// the record's offset only after this completes successfully, preserving at-least-once (Req 6.7).
    /// </summary>
    /// <param name="envelope">The envelope to process.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    Task ProcessAsync(MessageEnvelope envelope, CancellationToken cancellationToken);
}
