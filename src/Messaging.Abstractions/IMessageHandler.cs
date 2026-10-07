using Messaging.Contracts;

namespace Messaging.Abstractions;

/// <summary>
/// A consumer-supplied handler for messages of a known contract type. The core binds the
/// envelope payload to <typeparamref name="TContract"/> before invoking the handler (Req 4.5).
/// </summary>
/// <typeparam name="TContract">The message contract this handler processes.</typeparam>
public interface IMessageHandler<TContract>
{
    /// <summary>
    /// Handles a received message. Both the raw <paramref name="envelope"/> (for metadata such as
    /// correlation id and idempotency key) and the bound <paramref name="message"/> are provided.
    /// </summary>
    /// <param name="envelope">The received envelope carrying metadata.</param>
    /// <param name="message">The payload bound to the contract type.</param>
    /// <param name="cancellationToken">Token to cancel processing.</param>
    Task HandleAsync(MessageEnvelope envelope, TContract message, CancellationToken cancellationToken);
}
