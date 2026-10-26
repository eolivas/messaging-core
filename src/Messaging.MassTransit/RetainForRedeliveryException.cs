namespace Messaging.MassTransit;

/// <summary>
/// Domain-specific exception thrown from the MassTransit consume seam when the core dead-letter sink
/// reports <c>RetainedUnacknowledged</c> — that is, dead-letter routing failed and the message must
/// <em>not</em> be acknowledged. Throwing this (rather than returning normally) signals MassTransit
/// to leave the message unacknowledged so it is redelivered for a later attempt, instead of being
/// silently dropped (Req 6.7, 8.7).
/// </summary>
/// <remarks>
/// This is a dedicated type — never a bare <see cref="Exception"/> — so the consume pipeline can map
/// the "retain and redeliver" terminal outcome distinctly from genuine processing failures. Every
/// other terminal outcome (success, duplicate, bypass, routed-and-acknowledged) returns normally and
/// acknowledges the message; only a retained, unacknowledged dead-letter raises this exception.
/// </remarks>
public sealed class RetainForRedeliveryException : Exception
{
    /// <summary>Creates the exception with a default message.</summary>
    public RetainForRedeliveryException()
        : base("Dead-letter routing was retained unacknowledged; the message must be redelivered.")
    {
    }

    /// <summary>Creates the exception with a human-readable description.</summary>
    /// <param name="message">A description of why redelivery is being requested.</param>
    public RetainForRedeliveryException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a description and the underlying cause.</summary>
    /// <param name="message">A description of why redelivery is being requested.</param>
    /// <param name="innerException">The underlying routing failure that triggered retention.</param>
    public RetainForRedeliveryException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
