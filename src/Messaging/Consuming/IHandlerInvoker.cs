using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Consuming;

/// <summary>
/// Invokes the consumer-supplied <see cref="IMessageHandler{TContract}"/> for an already
/// contract-bound message. This seam decouples the consume pipeline from the open generic handler
/// type: the pipeline binds the payload to a runtime contract object (see <see cref="IContractBinder"/>)
/// and hands it here, and the invoker is responsible for dispatching to the correctly-typed handler.
/// </summary>
/// <remarks>
/// The pipeline cannot name <c>TContract</c> at compile time because the bound type is chosen at
/// runtime from the envelope's message type. Hiding the generic dispatch behind this port keeps the
/// pipeline free of reflection details (SRP) and lets the host be unit-tested with a trivial fake
/// invoker, so tasks 9.2–9.5 can assert pipeline behavior without a real handler registry.
/// </remarks>
public interface IHandlerInvoker
{
    /// <summary>
    /// Invokes the registered handler for <paramref name="boundMessage"/>, passing the original
    /// <paramref name="envelope"/> through so the handler sees the preserved metadata — notably the
    /// unchanged correlation id (Req 9.4) — along with the per-message <paramref name="context"/> that
    /// exposes the derived idempotency key for duplicate detection (Req 6.8, 7.1).
    /// </summary>
    /// <param name="envelope">The received envelope carrying metadata (correlation id, headers).</param>
    /// <param name="boundMessage">The payload already bound to its known contract type.</param>
    /// <param name="context">
    /// The consume context exposing the idempotency key (null when idempotency is disabled) and the
    /// duplicate indication (Req 6.8, 7.1, 7.2).
    /// </param>
    /// <param name="cancellationToken">Token to cancel handler processing.</param>
    Task InvokeAsync(
        MessageEnvelope envelope,
        object boundMessage,
        ConsumeContext context,
        CancellationToken cancellationToken);
}
