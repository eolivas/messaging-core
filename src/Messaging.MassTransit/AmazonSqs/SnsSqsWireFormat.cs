using System.Globalization;
using Messaging.Contracts;
using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Pure mapping of a <see cref="MessageEnvelope"/> to and from the SNS/SQS wire shape. Headers that
/// fit the attribute budget travel as <c>h.{key}</c> attributes; every other header stays in the body.
/// </summary>
public static class SnsSqsWireFormat
{
    /// <summary>Maps an envelope to its wire message.</summary>
    /// <param name="envelope">The envelope to publish.</param>
    /// <param name="traceHeaders">The trace and correlation headers produced on publish.</param>
    /// <param name="fifo">Whether to derive FIFO group and deduplication ids.</param>
    /// <param name="groupSelection">Which envelope field supplies the FIFO group id.</param>
    /// <returns>The body, attributes, FIFO ids, and correlation Guid.</returns>
    /// <exception cref="ArgumentNullException">If an argument is null.</exception>
    public static SnsSqsWireMessage ToWire(
        MessageEnvelope envelope,
        IReadOnlyDictionary<string, string> traceHeaders,
        bool fifo = false,
        FifoGroupSelection groupSelection = FifoGroupSelection.CorrelationId)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        ArgumentNullException.ThrowIfNull(traceHeaders);

        var split = SnsSqsHeaderMapper.Split(envelope.Headers, traceHeaders);

        var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var reserved in split.ReservedAttributes)
        {
            attributes[reserved.Key] = reserved.Value;
        }

        attributes[SnsSqsHeaderMapper.MessageTypeAttribute] = envelope.MessageType;
        foreach (var header in split.AttributeHeaders)
        {
            attributes[header.Key] = header.Value;
        }

        var body = EnvelopeMessage.FromEnvelope(envelope) with
        {
            Headers = new Dictionary<string, string>(split.BodyHeaders, StringComparer.Ordinal),
        };

        return new SnsSqsWireMessage(
            body,
            attributes,
            fifo ? FifoMessageIds.GroupId(envelope, groupSelection) : null,
            fifo ? FifoMessageIds.DeduplicationId(envelope) : null,
            CorrelationGuid.From(envelope.CorrelationId));
    }

    /// <summary>Rebuilds the envelope from the delivered body and the consume-context headers.</summary>
    /// <param name="message">The delivered body.</param>
    /// <param name="consumeHeaders">The headers of <c>ConsumeContext.Headers</c>.</param>
    /// <returns>An envelope equal to the one that was published.</returns>
    /// <exception cref="ArgumentNullException">If an argument is null.</exception>
    public static MessageEnvelope FromWire(
        EnvelopeMessage message,
        IEnumerable<KeyValuePair<string, object>> consumeHeaders)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(consumeHeaders);

        // Body headers are authoritative, including keys that collide with reserved transport keys.
        var headers = new Dictionary<string, string>(message.Headers, StringComparer.Ordinal);

        foreach (var header in consumeHeaders)
        {
            if (!header.Key.StartsWith(SnsSqsHeaderMapper.UserHeaderPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            var key = header.Key[SnsSqsHeaderMapper.UserHeaderPrefix.Length..];
            var value = Convert.ToString(header.Value, CultureInfo.InvariantCulture);
            if (key.Length == 0 || value is null)
            {
                continue;
            }

            headers.TryAdd(key, value);
        }

        return (message with { Headers = headers }).ToEnvelope();
    }
}
