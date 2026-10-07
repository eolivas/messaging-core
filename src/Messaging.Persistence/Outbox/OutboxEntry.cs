using Messaging.Abstractions;

namespace Messaging.Persistence.Outbox;

/// <summary>
/// EF persistence model for a row of the <c>messaging_outbox</c> table (Req 5). This is an
/// Infrastructure-only entity: it is deliberately distinct from the transport-neutral
/// <see cref="Messaging.Contracts.MessageEnvelope"/> so that EF mapping types never leak into
/// the Domain. The dispatcher (task 17) materialises an envelope from this row and back again;
/// this type exists only to shape the persisted record.
/// </summary>
/// <remarks>
/// Mutable, parameterless-constructible by design so EF can materialise and track it, and so the
/// dispatcher can transition <see cref="Status"/>, <see cref="AttemptCount"/>,
/// <see cref="DispatchedAtUtc"/>, and <see cref="LastError"/> as entries are dispatched (Req
/// 5.3–5.5). Column mapping lives in <see cref="OutboxEntryConfiguration"/>.
/// </remarks>
public sealed class OutboxEntry
{
    /// <summary>Primary key; equals the originating envelope's MessageId (Req 4.1). Maps to <c>id uuid</c>.</summary>
    public Guid Id { get; set; }

    /// <summary>Correlation id carried from the envelope, 1..128 chars (Req 4.1). Maps to <c>correlation_id varchar(128)</c>.</summary>
    public string CorrelationId { get; set; } = string.Empty;

    /// <summary>Contract identifier used to bind on consume (Req 4.5). Maps to <c>message_type varchar(256)</c>.</summary>
    public string MessageType { get; set; } = string.Empty;

    /// <summary>Additive contract revision, default 1 (Req 4.7). Maps to <c>contract_version int</c>.</summary>
    public int ContractVersion { get; set; } = 1;

    /// <summary>Envelope headers (0..64 entries) serialized to <c>headers jsonb</c> (Req 4.1).</summary>
    public IReadOnlyDictionary<string, string> Headers { get; set; }
        = new Dictionary<string, string>();

    /// <summary>Opaque envelope payload bytes. Maps to <c>payload bytea</c>.</summary>
    public byte[] Payload { get; set; } = Array.Empty<byte>();

    /// <summary>Dispatch lifecycle state (Req 5.3–5.5). Maps to <c>status smallint</c> via <see cref="OutboxStatus"/>.</summary>
    public OutboxStatus Status { get; set; } = OutboxStatus.Pending;

    /// <summary>Count of dispatch attempts; capped at 10 (Req 5.5). Maps to <c>attempt_count int</c> default 0.</summary>
    public int AttemptCount { get; set; }

    /// <summary>UTC time the entry was recorded. Maps to <c>created_at_utc timestamptz</c>.</summary>
    public DateTimeOffset CreatedAtUtc { get; set; }

    /// <summary>UTC time the publisher confirmed dispatch; null until confirmed (Req 5.3). Maps to <c>dispatched_at_utc timestamptz</c> nullable.</summary>
    public DateTimeOffset? DispatchedAtUtc { get; set; }

    /// <summary>Last failure reason, redacted before persisting (Req 9.7); null until a failure. Maps to <c>last_error text</c> nullable.</summary>
    public string? LastError { get; set; }
}
