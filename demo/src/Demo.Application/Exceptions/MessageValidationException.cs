namespace Demo.Application.Exceptions;

/// <summary>Thrown when a message payload fails validation. Non-transient: it must not be retried.</summary>
public sealed class MessageValidationException : Exception
{
    public MessageValidationException(string message)
        : base(message)
    {
    }

    public MessageValidationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
