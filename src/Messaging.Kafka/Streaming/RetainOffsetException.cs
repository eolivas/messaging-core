namespace Messaging.Kafka.Streaming;

/// <summary>
/// Domain-specific exception thrown from <see cref="IStreamMessageProcessor"/> when the core
/// dead-letter sink reports <c>RetainedUnacknowledged</c>: dead-letter routing failed, so the
/// record's offset must <em>not</em> be committed. Because the exception propagates out of
/// <c>ProcessAsync</c> before the host's commit step, the record is re-polled instead of being
/// silently dropped (Req 6.3, 6.4).
/// </summary>
/// <remarks>
/// The Kafka analogue of <c>Messaging.MassTransit.RetainForRedeliveryException</c>. Every other
/// terminal outcome (success, duplicate, routed-and-acknowledged) returns normally so the offset is
/// committed; only a retained, unacknowledged dead-letter raises this exception.
/// </remarks>
public sealed class RetainOffsetException : Exception
{
    /// <summary>Creates the exception with a default message.</summary>
    public RetainOffsetException()
        : base("Dead-letter routing was retained unacknowledged; the offset must not be committed.")
    {
    }

    /// <summary>Creates the exception with a human-readable description.</summary>
    /// <param name="message">A description of why the offset is being retained.</param>
    public RetainOffsetException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a description and the underlying cause.</summary>
    /// <param name="message">A description of why the offset is being retained.</param>
    /// <param name="innerException">The underlying routing failure that triggered retention.</param>
    public RetainOffsetException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
