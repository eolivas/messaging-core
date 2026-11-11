using Messaging.Abstractions;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Raised when a consumed Kafka record lacks (or carries an unreadable value for) a required
/// reserved metadata header, so no complete <c>MessageEnvelope</c> can be reconstructed (Req 9.6).
/// </summary>
/// <remarks>
/// Derives from <see cref="MessagingFailureException"/> with <see cref="FailureKind.NonTransient"/>,
/// so the core <c>FailureClassifier</c> routes it to dead-letter with zero retries via its existing
/// <c>MessagingFailureException</c> rule; no core change is needed. The message names only the
/// offending header, never payload bytes or header values.
/// </remarks>
public sealed class MissingRequiredHeaderException : MessagingFailureException
{
    /// <summary>Creates the exception for the given header name.</summary>
    /// <param name="headerName">The name of the missing or unreadable required header.</param>
    public MissingRequiredHeaderException(string headerName)
        : base($"Required Kafka record header '{headerName}' is missing or unreadable.")
    {
        HeaderName = headerName;
    }

    /// <summary>Creates the exception for the given header name with an underlying cause.</summary>
    /// <param name="headerName">The name of the missing or unreadable required header.</param>
    /// <param name="innerException">The underlying parse failure.</param>
    public MissingRequiredHeaderException(string headerName, Exception innerException)
        : base($"Required Kafka record header '{headerName}' is missing or unreadable.", innerException)
    {
        HeaderName = headerName;
    }

    /// <summary>The name of the offending header (never its value or the payload).</summary>
    public string HeaderName { get; }

    /// <inheritdoc />
    public override FailureKind Kind => FailureKind.NonTransient;
}
