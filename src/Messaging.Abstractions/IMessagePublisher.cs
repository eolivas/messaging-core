using Messaging.Contracts;

namespace Messaging.Abstractions;

/// <summary>
/// The transport-neutral publishing port. Application code depends only on this abstraction and
/// never on a concrete provider (Req 3.1, 3.8).
/// </summary>
public interface IMessagePublisher
{
    /// <summary>
    /// Validates the envelope's invariants and, when valid, hands it off to the active provider.
    /// Returns a failed <see cref="PublishResult"/> and leaves the envelope undelivered when the
    /// envelope's required fields are absent or malformed (Req 3.2, 4.3).
    /// </summary>
    /// <param name="envelope">The envelope to publish.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task<PublishResult> PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken);
}
