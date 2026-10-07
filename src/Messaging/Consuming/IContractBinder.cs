using Messaging.Contracts;

namespace Messaging.Consuming;

/// <summary>
/// The consume-side binding port: resolves an envelope's untrusted payload bytes to a known message
/// contract identified by <see cref="MessageEnvelope.MessageType"/> (Req 4.5). Implementations bind
/// only to pre-registered contract types and reject any unknown contract with an
/// <see cref="UnknownContractException"/> before the consumer handler runs (Req 4.6).
/// </summary>
/// <remarks>
/// <para>
/// This abstraction exists because binding crosses a concern boundary: the consume pipeline decides
/// <em>when</em> to bind and how to sequence binding against the handler, while <em>how</em> payload
/// bytes become a typed contract (serialization format, tolerant/additive deserialization) is a
/// swappable detail. Depending on the port keeps the pipeline testable and lets a different wire
/// format be substituted without touching the host (DIP).
/// </para>
/// <para>
/// Security: implementations MUST treat the payload as untrusted and MUST deserialize only into a
/// registered contract type — never into a type chosen from the message itself (code-conventions).
/// </para>
/// </remarks>
public interface IContractBinder
{
    /// <summary>
    /// Returns <see langword="true"/> when a contract is registered for the given message type.
    /// </summary>
    /// <param name="messageType">The <see cref="MessageEnvelope.MessageType"/> to probe.</param>
    bool IsKnown(string messageType);

    /// <summary>
    /// Binds the envelope's payload to the registered contract type for its
    /// <see cref="MessageEnvelope.MessageType"/> and returns the bound object.
    /// </summary>
    /// <param name="envelope">The received envelope whose payload is bound.</param>
    /// <returns>The payload deserialized into the registered contract type.</returns>
    /// <exception cref="UnknownContractException">
    /// If no contract is registered for the envelope's message type, or the payload cannot be bound to
    /// the registered contract (Req 4.6).
    /// </exception>
    object Bind(MessageEnvelope envelope);
}
