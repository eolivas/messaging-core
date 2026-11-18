using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// An envelope shaped for the SNS/SQS wire: the MassTransit body, the message attributes, the FIFO
/// identifiers, and the Guid for <c>SendContext.CorrelationId</c>.
/// </summary>
/// <param name="Body">The message body; its headers hold only the body-resident headers.</param>
/// <param name="AttributeHeaders">
/// The message attributes by full name: <c>x-correlation-id</c>, <c>traceparent</c> (when present),
/// <c>x-message-type</c>, and <c>h.{key}</c> user headers. At most 10 entries.
/// </param>
/// <param name="GroupId">The FIFO message group id, or null for a standard queue.</param>
/// <param name="DeduplicationId">The FIFO deduplication id, or null for a standard queue.</param>
/// <param name="CorrelationGuid">The Guid derived from the correlation id.</param>
public sealed record SnsSqsWireMessage(
    EnvelopeMessage Body,
    IReadOnlyDictionary<string, string> AttributeHeaders,
    string? GroupId,
    string? DeduplicationId,
    Guid CorrelationGuid);
