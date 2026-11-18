namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Thrown when a publish is attempted by a bus whose role does not permit publishing
/// (the Consumer role). The message carries no credentials, connection values, or payload.
/// </summary>
public sealed class PublishNotPermittedException : Exception
{
    /// <summary>Creates the exception with a default message.</summary>
    public PublishNotPermittedException()
        : base("Publishing is not permitted for the Consumer role of the Amazon SNS/SQS provider.")
    {
    }

    /// <summary>Creates the exception with a description.</summary>
    /// <param name="message">A description that must not contain secrets or payload bytes.</param>
    public PublishNotPermittedException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a description and the underlying cause.</summary>
    /// <param name="message">A description that must not contain secrets or payload bytes.</param>
    /// <param name="innerException">The underlying cause.</param>
    public PublishNotPermittedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
