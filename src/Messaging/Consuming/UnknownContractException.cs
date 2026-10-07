using Messaging.Contracts;

namespace Messaging.Consuming;

/// <summary>
/// Domain-specific exception raised when a received <see cref="MessageEnvelope"/> carries a
/// <see cref="MessageEnvelope.MessageType"/> that matches no known/registered message contract,
/// or whose payload cannot be bound to the registered contract (Req 4.6). Fail-fast: the consume
/// pipeline rejects the envelope with this exception <em>before</em> invoking the consumer handler,
/// so an unbindable payload never reaches application code.
/// </summary>
/// <remarks>
/// <para>
/// This is distinct from <see cref="InvalidEnvelopeException"/> (a malformed envelope) and from the
/// correlation-id gate's failure: it specifically signals that no contract is known for the
/// envelope's message type. Keeping it a dedicated type lets later dead-letter routing (task 13)
/// treat an unknown contract as a poison message routed without retry (Req 8.3) while the consume
/// pipeline here is only responsible for surfacing the rejection.
/// </para>
/// </remarks>
public sealed class UnknownContractException : Exception
{
    /// <summary>The unrecognized message type identifier carried by the envelope, for diagnostics.</summary>
    public string MessageType { get; }

    /// <summary>
    /// Creates the exception for an envelope whose message type is not registered.
    /// </summary>
    /// <param name="messageType">The unrecognized <see cref="MessageEnvelope.MessageType"/>.</param>
    /// <param name="message">A human-readable description of the rejection.</param>
    public UnknownContractException(string messageType, string message)
        : base(message)
    {
        MessageType = messageType;
    }

    /// <summary>
    /// Creates the exception for an envelope whose payload failed to bind to the registered contract,
    /// preserving the underlying deserialization error as the inner exception.
    /// </summary>
    /// <param name="messageType">The envelope's <see cref="MessageEnvelope.MessageType"/>.</param>
    /// <param name="message">A human-readable description of the rejection.</param>
    /// <param name="innerException">The underlying binding/deserialization failure.</param>
    public UnknownContractException(string messageType, string message, Exception innerException)
        : base(message, innerException)
    {
        MessageType = messageType;
    }
}
