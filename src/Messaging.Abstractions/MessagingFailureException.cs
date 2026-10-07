namespace Messaging.Abstractions;

/// <summary>
/// The base type for processing failures the core classifies when deciding whether to retry a
/// message or route it straight to the dead-letter sink (Req 6.2, 6.3). It is deliberately
/// <see langword="abstract"/> so a failure always carries a concrete, specific category
/// (timeout, connection, downstream-unavailable, validation, malformed payload) rather than a
/// bare <see cref="Exception"/>, honouring the "throw domain-specific exceptions, not bare
/// Exception" convention.
/// </summary>
/// <remarks>
/// <para>
/// Each concrete subclass is tagged with the <see cref="FailureKind"/> it represents through
/// <see cref="Kind"/>, so a classifier can decide retry-vs-dead-letter from the exception type
/// alone without string matching. This keeps classification total and testable: the category is a
/// property of the exception, not an inference from its message.
/// </para>
/// <para>
/// Living in <c>Messaging.Abstractions</c> (Domain) keeps these failure categories dependency-free
/// and shareable by every layer — the Application classifier, the adapters that raise transport
/// failures, and consumers.
/// </para>
/// </remarks>
public abstract class MessagingFailureException : Exception
{
    /// <summary>
    /// Creates a messaging failure with the given message.
    /// </summary>
    /// <param name="message">A human-readable description of the failure.</param>
    protected MessagingFailureException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Creates a messaging failure with the given message and underlying cause.
    /// </summary>
    /// <param name="message">A human-readable description of the failure.</param>
    /// <param name="innerException">The underlying cause of the failure.</param>
    protected MessagingFailureException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// The classification this failure category represents (Req 6.2, 6.3). A classifier reads this
    /// directly rather than inferring the category from the exception message.
    /// </summary>
    public abstract FailureKind Kind { get; }
}
