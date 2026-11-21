namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// The result of partitioning an envelope's headers between SNS/SQS message attributes and the
/// message body.
/// </summary>
/// <param name="ReservedAttributes">
/// The reserved transport attributes taken from the trace headers (<c>x-correlation-id</c> and, when
/// present, <c>traceparent</c>). <c>x-message-type</c> is added by the wire format.
/// </param>
/// <param name="AttributeHeaders">The user headers sent as <c>h.{key}</c> attributes, keyed by the full attribute name.</param>
/// <param name="BodyHeaders">The user headers that stay in the message body.</param>
/// <param name="AllowedTransportHeaders">
/// The keys given to <c>AllowTransportHeader</c>: the reserved keys plus the selected <c>h.*</c> keys.
/// </param>
public sealed record SnsSqsHeaderSplit(
    IReadOnlyDictionary<string, string> ReservedAttributes,
    IReadOnlyDictionary<string, string> AttributeHeaders,
    IReadOnlyDictionary<string, string> BodyHeaders,
    IReadOnlySet<string> AllowedTransportHeaders);
