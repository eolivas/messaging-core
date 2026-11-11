using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Messaging.Contracts;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Lossless mapping between a <see cref="MessageEnvelope"/> and a Kafka record. The payload bytes are
/// the record value; the five required metadata fields and every user header travel as Kafka record
/// headers, so the consumer reconstructs the same envelope (Req 9.1-9.5).
/// </summary>
/// <remarks>
/// <see cref="ToEnvelope"/> is all-or-nothing: if any required header is absent or unreadable it throws
/// <see cref="MissingRequiredHeaderException"/> naming the header, and no partial envelope is returned
/// (Req 9.6). Exception messages carry the header name only, never payload bytes or header values (Req 10.5).
/// </remarks>
public static class KafkaWireFormat
{
    public const string MessageIdHeader = "x-message-id";
    public const string CorrelationIdHeader = "x-correlation-id";
    public const string MessageTypeHeader = "x-message-type";
    public const string CreatedAtHeader = "x-created-at-utc";
    public const string ContractVersionHeader = "x-contract-version";

    /// <summary>Reserved header names that carry envelope metadata rather than user headers.</summary>
    private static readonly HashSet<string> ReservedHeaders = new(StringComparer.Ordinal)
    {
        MessageIdHeader,
        CorrelationIdHeader,
        MessageTypeHeader,
        CreatedAtHeader,
        ContractVersionHeader,
    };

    // Strict decoding so invalid UTF-8 is detected instead of silently replaced.
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Serializes an envelope into a Kafka message (payload as value, metadata as headers).</summary>
    public static Message<Null, byte[]> ToMessage(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var headers = new Headers
        {
            { MessageIdHeader, Encoding.UTF8.GetBytes(envelope.MessageId.ToString("D")) },
            { CorrelationIdHeader, Encoding.UTF8.GetBytes(envelope.CorrelationId) },
            { MessageTypeHeader, Encoding.UTF8.GetBytes(envelope.MessageType) },
            { CreatedAtHeader, Encoding.UTF8.GetBytes(envelope.CreatedAtUtc.ToString("O", CultureInfo.InvariantCulture)) },
            { ContractVersionHeader, Encoding.UTF8.GetBytes(envelope.ContractVersion.ToString(CultureInfo.InvariantCulture)) },
        };

        foreach (var (key, value) in envelope.Headers)
        {
            headers.Add(key, Encoding.UTF8.GetBytes(value));
        }

        return new Message<Null, byte[]>
        {
            Value = envelope.Payload.ToArray(),
            Headers = headers,
        };
    }

    /// <summary>Reconstructs an envelope from a consumed Kafka message (inverse of <see cref="ToMessage"/>).</summary>
    /// <exception cref="MissingRequiredHeaderException">A required metadata header is absent or unreadable.</exception>
    public static MessageEnvelope ToEnvelope(Message<Ignore, byte[]> message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var headers = message.Headers ?? new Headers();

        // Read and validate every required header first so no partial envelope can escape.
        var messageId = ReadRequired(headers, MessageIdHeader, text => Guid.Parse(text, CultureInfo.InvariantCulture));
        var correlationId = ReadRequired(headers, CorrelationIdHeader, text => text);
        var messageType = ReadRequired(headers, MessageTypeHeader, text => text);
        var createdAt = ReadRequired(
            headers,
            CreatedAtHeader,
            text => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind));
        var contractVersion = ReadRequired(
            headers,
            ContractVersionHeader,
            text => int.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture));

        var userHeaders = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var header in headers)
        {
            if (ReservedHeaders.Contains(header.Key))
            {
                continue;
            }

            userHeaders[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes() ?? Array.Empty<byte>());
        }

        return new MessageEnvelope
        {
            MessageId = messageId,
            CorrelationId = correlationId,
            MessageType = messageType,
            CreatedAtUtc = createdAt,
            ContractVersion = contractVersion,
            Headers = userHeaders,
            Payload = new ReadOnlyMemory<byte>(message.Value ?? Array.Empty<byte>()),
        };
    }

    private static T ReadRequired<T>(Headers headers, string key, Func<string, T> parse)
    {
        if (!headers.TryGetLastBytes(key, out var bytes) || bytes is null)
        {
            throw new MissingRequiredHeaderException(key);
        }

        try
        {
            return parse(StrictUtf8.GetString(bytes));
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or ArgumentException)
        {
            // DecoderFallbackException derives from ArgumentException. Inner exceptions from the
            // parsers can echo the offending value, so only the header name is surfaced in the message.
            throw new MissingRequiredHeaderException(key, ex);
        }
    }
}
