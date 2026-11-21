using System.Globalization;
using System.Text;
using Messaging.Contracts;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>Pure derivation of the SQS FIFO message group id and deduplication id.</summary>
public static class FifoMessageIds
{
    /// <summary>Maximum SQS message group id length.</summary>
    public const int GroupIdLimit = 128;

    /// <summary>Derives the message group id from the correlation id or message type.</summary>
    /// <param name="envelope">The envelope being published.</param>
    /// <param name="selection">Which envelope field supplies the group id.</param>
    /// <returns>A group id of allowed characters, at most 128 characters long.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    public static string GroupId(MessageEnvelope envelope, FifoGroupSelection selection)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var source = selection == FifoGroupSelection.MessageType
            ? envelope.MessageType
            : envelope.CorrelationId;

        var builder = new StringBuilder(Math.Min(source.Length, GroupIdLimit));
        foreach (var character in source)
        {
            if (builder.Length == GroupIdLimit)
            {
                break;
            }

            // SQS permits printable ASCII (0x21 to 0x7E); anything else, including whitespace, becomes '_'.
            builder.Append(character is >= '!' and <= '~' ? character : '_');
        }

        return builder.ToString();
    }

    /// <summary>Derives the deduplication id: the message id in <c>D</c> format.</summary>
    /// <param name="envelope">The envelope being published.</param>
    /// <returns>The lowercase hyphenated message id.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    public static string DeduplicationId(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        return envelope.MessageId.ToString("D", CultureInfo.InvariantCulture);
    }
}
