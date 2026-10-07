using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Consuming;

/// <summary>
/// Domain-specific exception raised when idempotent consumption is enabled but the
/// <see cref="IIdempotencyKeyProvider"/> cannot derive an idempotency key from the received
/// <see cref="MessageEnvelope"/> (Req 7.6). Fail-fast: the consume gate throws this
/// <em>before</em> the store is consulted and before the consumer handler runs, so a message with no
/// derivable key never produces side effects.
/// </summary>
/// <remarks>
/// This is distinct from <see cref="UnknownContractException"/> (an unbindable payload): it signals
/// specifically that the message lacks the stable identity needed to deduplicate it. The exception
/// carries the offending <see cref="MessageType"/> for diagnostics without leaking payload content.
/// </remarks>
public sealed class MissingIdempotencyKeyException : Exception
{
    /// <summary>The message type of the envelope whose idempotency key could not be derived, for diagnostics.</summary>
    public string MessageType { get; }

    /// <summary>
    /// Creates the exception for an envelope whose idempotency key could not be derived.
    /// </summary>
    /// <param name="messageType">The envelope's <see cref="MessageEnvelope.MessageType"/>.</param>
    public MissingIdempotencyKeyException(string messageType)
        : base($"Idempotency is enabled but no idempotency key could be derived for message type "
            + $"'{messageType}'; the message was rejected before any side effects.")
    {
        MessageType = messageType;
    }
}
