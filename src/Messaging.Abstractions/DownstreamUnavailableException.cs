namespace Messaging.Abstractions;

/// <summary>
/// A transient processing failure raised when a downstream dependency the handler relies on is
/// unavailable (Req 6.2). Classified as <see cref="FailureKind.Transient"/>, so processing is
/// retried under the configured retry policy.
/// </summary>
public sealed class DownstreamUnavailableException : MessagingFailureException
{
    /// <summary>Creates the exception with the given message.</summary>
    /// <param name="message">A human-readable description of the unavailable downstream.</param>
    public DownstreamUnavailableException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with the given message and underlying cause.</summary>
    /// <param name="message">A human-readable description of the unavailable downstream.</param>
    /// <param name="innerException">The underlying cause of the unavailability.</param>
    public DownstreamUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <inheritdoc />
    public override FailureKind Kind => FailureKind.Transient;
}
