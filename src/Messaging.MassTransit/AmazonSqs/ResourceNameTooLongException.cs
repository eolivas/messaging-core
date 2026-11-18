using System.Globalization;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Thrown when a derived topic or queue name would exceed the service limit. Names are never truncated.
/// </summary>
public sealed class ResourceNameTooLongException : Exception
{
    /// <summary>Creates the exception for the subject the name was derived from and the limit.</summary>
    /// <param name="subject">The message type string or endpoint name the name was derived from.</param>
    /// <param name="limit">The maximum permitted name length in characters.</param>
    public ResourceNameTooLongException(string subject, int limit)
        : base(string.Create(
            CultureInfo.InvariantCulture,
            $"The resource name derived from '{subject}' exceeds the limit of {limit} characters."))
    {
        Subject = subject;
        Limit = limit;
    }

    /// <summary>The message type string or endpoint name the name was derived from.</summary>
    public string Subject { get; }

    /// <summary>The maximum permitted name length in characters.</summary>
    public int Limit { get; }
}
