namespace Messaging.Abstractions;

/// <summary>
/// A non-transient processing failure raised when a message payload is malformed and cannot be
/// interpreted (Req 6.3). Classified as <see cref="FailureKind.NonTransient"/>, so the message is
/// routed to the dead-letter sink as a poison message without any retry — the bytes will not become
/// well-formed on a later attempt.
/// </summary>
public sealed class MalformedPayloadException : MessagingFailureException
{
    /// <summary>Creates the exception with the given message.</summary>
    /// <param name="message">A human-readable description of the malformed payload.</param>
    public MalformedPayloadException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with the given message and underlying cause.</summary>
    /// <param name="message">A human-readable description of the malformed payload.</param>
    /// <param name="innerException">The underlying parse/bind failure.</param>
    public MalformedPayloadException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <inheritdoc />
    public override FailureKind Kind => FailureKind.NonTransient;
}
