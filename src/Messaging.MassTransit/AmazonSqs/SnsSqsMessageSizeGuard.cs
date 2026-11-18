using System.Text;
using System.Text.Json;
using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Estimates the on-wire size of a message and rejects it before any send when it exceeds the limit.
/// The estimate is conservative: MassTransit serializes the envelope internally.
/// </summary>
public static class SnsSqsMessageSizeGuard
{
    /// <summary>Fixed allowance, in bytes, for the MassTransit message envelope.</summary>
    public const int EnvelopeAllowanceBytes = 2048;

    private const string AttributeDataType = "String";

    /// <summary>Measures the message and throws when it exceeds <paramref name="limit"/>.</summary>
    /// <param name="envelope">The wire body.</param>
    /// <param name="attributes">The message attributes by name.</param>
    /// <param name="limit">The maximum permitted size in bytes.</param>
    /// <exception cref="ArgumentNullException">If an argument is null.</exception>
    /// <exception cref="MessageTooLargeException">If the measured size exceeds the limit.</exception>
    public static void Check(
        EnvelopeMessage envelope,
        IReadOnlyDictionary<string, string> attributes,
        long limit)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(attributes);

        long actual = JsonSerializer.SerializeToUtf8Bytes(envelope).LongLength + EnvelopeAllowanceBytes;
        foreach (var attribute in attributes)
        {
            actual += Encoding.UTF8.GetByteCount(attribute.Key)
                + Encoding.UTF8.GetByteCount(AttributeDataType)
                + Encoding.UTF8.GetByteCount(attribute.Value);
        }

        if (actual > limit)
        {
            throw new MessageTooLargeException(actual, limit);
        }
    }
}
