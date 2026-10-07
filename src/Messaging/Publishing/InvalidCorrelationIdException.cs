using Messaging.Contracts;

namespace Messaging.Publishing;

/// <summary>
/// Domain-specific exception raised when a caller supplies a <see cref="MessageEnvelope.CorrelationId"/>
/// that is present but invalid — empty/whitespace or longer than
/// <see cref="MessageEnvelope.MaxCorrelationIdLength"/> characters (Req 4.3). Fail-fast: the
/// correlation id gate throws this rather than silently generating a replacement, so a caller that
/// intended to supply a correlation id learns the value was rejected instead of having it quietly
/// overwritten.
/// </summary>
/// <remarks>
/// A <see langword="null"/> or whitespace-only correlation id is treated as <em>absent</em> (the
/// gate generates one) rather than invalid; only a non-whitespace value that violates the length
/// bounds is rejected. This exception is distinct from <see cref="InvalidEnvelopeException"/> so the
/// publish pipeline can map it to a dedicated invalid-correlation-id publish failure (Req 4.3) while
/// still letting the broader envelope validation gate (task 8.2) report other invariant violations.
/// </remarks>
public sealed class InvalidCorrelationIdException : Exception
{
    /// <summary>The invalid correlation id value that was supplied, for diagnostics.</summary>
    public string SuppliedCorrelationId { get; }

    /// <summary>
    /// Creates the exception for a supplied-but-invalid correlation id.
    /// </summary>
    /// <param name="suppliedCorrelationId">The rejected value.</param>
    /// <param name="message">A human-readable description of why the value is invalid.</param>
    public InvalidCorrelationIdException(string suppliedCorrelationId, string message)
        : base(message)
    {
        SuppliedCorrelationId = suppliedCorrelationId;
    }
}
