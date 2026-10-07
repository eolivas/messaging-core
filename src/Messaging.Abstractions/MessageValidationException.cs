namespace Messaging.Abstractions;

/// <summary>
/// A non-transient processing failure raised when a message fails business/content validation
/// (Req 6.3). Classified as <see cref="FailureKind.NonTransient"/>, so the message is routed to the
/// dead-letter sink without any retry — retrying a value that is semantically invalid would only
/// fail again.
/// </summary>
public sealed class MessageValidationException : MessagingFailureException
{
    /// <summary>Creates the exception with the given message.</summary>
    /// <param name="message">A human-readable description of the validation failure.</param>
    public MessageValidationException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with the given message and underlying cause.</summary>
    /// <param name="message">A human-readable description of the validation failure.</param>
    /// <param name="innerException">The underlying cause of the validation failure.</param>
    public MessageValidationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <inheritdoc />
    public override FailureKind Kind => FailureKind.NonTransient;
}
