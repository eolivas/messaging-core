using Messaging.Contracts;

namespace Messaging.Abstractions;

/// <summary>
/// Derives a deterministic idempotency key from a <see cref="MessageEnvelope"/> such that two
/// envelopes representing the same logical message produce an identical key (Req 7.1).
/// </summary>
public interface IIdempotencyKeyProvider
{
    /// <summary>
    /// Attempts to derive the idempotency key for the envelope. Returns false and leaves
    /// <paramref name="idempotencyKey"/> empty when a key cannot be derived (Req 7.6).
    /// </summary>
    /// <param name="envelope">The envelope to derive the key from.</param>
    /// <param name="idempotencyKey">The derived key when the method returns true.</param>
    bool TryDerive(MessageEnvelope envelope, out string idempotencyKey);
}
