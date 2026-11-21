using System.Globalization;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Thrown before any send when the measured message size exceeds the configured limit.
/// Reports sizes only, never payload bytes.
/// </summary>
public sealed class MessageTooLargeException : Exception
{
    /// <summary>Creates the exception for a measured size and the limit it exceeds.</summary>
    /// <param name="actual">The measured size in bytes.</param>
    /// <param name="limit">The maximum permitted size in bytes.</param>
    public MessageTooLargeException(long actual, long limit)
        : base(string.Create(
            CultureInfo.InvariantCulture,
            $"Message size {actual} bytes exceeds the limit of {limit} bytes."))
    {
        Actual = actual;
        Limit = limit;
    }

    /// <summary>The measured size in bytes.</summary>
    public long Actual { get; }

    /// <summary>The maximum permitted size in bytes.</summary>
    public long Limit { get; }
}
