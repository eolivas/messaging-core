using Messaging.Contracts;

namespace Messaging.MassTransit.Transport;

/// <summary>
/// The MassTransit message contract that carries a transport-neutral <see cref="MessageEnvelope"/>
/// across RabbitMQ as data. Every envelope field rides alongside the payload — message id,
/// correlation id, message type, created-at, contract version, and any custom headers — so the
/// consumer reconstructs the <em>same</em> envelope on the far side. The payload bytes travel
/// verbatim.
/// </summary>
/// <remarks>
/// <para>
/// <b>Wire-contract fidelity.</b> <see cref="FromEnvelope"/> and <see cref="ToEnvelope"/> form a
/// lossless round trip: headers are copied with <see cref="StringComparer.Ordinal"/> (preserving
/// key identity exactly), payload bytes are materialized with <c>ToArray()</c> on the way out and
/// re-wrapped in a <see cref="ReadOnlyMemory{T}"/> on the way back in. Carrying the correlation id
/// as a first-class field is what lets the publish/consume round trip preserve it end to end.
/// </para>
/// <para>
/// This type lives in the Infrastructure (<c>Messaging.MassTransit</c>) package because it is a
/// transport concern: it exists only to shape the core envelope for the MassTransit/RabbitMQ wire
/// and has no meaning in the transport-neutral core.
/// </para>
/// </remarks>
public sealed record EnvelopeMessage
{
    /// <summary>The envelope's unique message identifier, carried verbatim across the wire.</summary>
    public Guid MessageId { get; init; }

    /// <summary>The correlation id used to tie published and consumed messages together.</summary>
    public string CorrelationId { get; init; } = string.Empty;

    /// <summary>The logical message type used to bind the payload to a registered contract.</summary>
    public string MessageType { get; init; } = string.Empty;

    /// <summary>The UTC instant the envelope was created, preserved across the round trip.</summary>
    public DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>The contract version the payload was produced against.</summary>
    public int ContractVersion { get; init; } = 1;

    /// <summary>Custom headers travelling alongside the payload (e.g. tenant, trace context).</summary>
    public Dictionary<string, string> Headers { get; init; } = new();

    /// <summary>The raw payload bytes, carried verbatim.</summary>
    public byte[] Payload { get; init; } = Array.Empty<byte>();

    /// <summary>
    /// Projects a transport-neutral <see cref="MessageEnvelope"/> onto the wire contract, copying
    /// headers with <see cref="StringComparer.Ordinal"/> and materializing the payload with
    /// <see cref="ReadOnlyMemory{T}.ToArray"/> so the wire message owns an independent byte buffer.
    /// </summary>
    /// <param name="envelope">The core envelope to carry across the transport.</param>
    public static EnvelopeMessage FromEnvelope(MessageEnvelope envelope) => new()
    {
        MessageId = envelope.MessageId,
        CorrelationId = envelope.CorrelationId,
        MessageType = envelope.MessageType,
        CreatedAtUtc = envelope.CreatedAtUtc,
        ContractVersion = envelope.ContractVersion,
        Headers = new Dictionary<string, string>(envelope.Headers, StringComparer.Ordinal),
        Payload = envelope.Payload.ToArray(),
    };

    /// <summary>
    /// Rebuilds the transport-neutral <see cref="MessageEnvelope"/> from the wire contract, copying
    /// headers with <see cref="StringComparer.Ordinal"/> and re-wrapping the payload bytes in a
    /// <see cref="ReadOnlyMemory{T}"/> — the inverse of <see cref="FromEnvelope"/>.
    /// </summary>
    public MessageEnvelope ToEnvelope() => new()
    {
        MessageId = MessageId,
        CorrelationId = CorrelationId,
        MessageType = MessageType,
        CreatedAtUtc = CreatedAtUtc,
        ContractVersion = ContractVersion,
        Headers = new Dictionary<string, string>(Headers, StringComparer.Ordinal),
        Payload = new ReadOnlyMemory<byte>(Payload),
    };
}
