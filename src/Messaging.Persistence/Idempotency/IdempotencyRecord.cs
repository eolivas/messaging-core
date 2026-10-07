namespace Messaging.Persistence.Idempotency;

/// <summary>
/// EF persistence model for a row of the <c>messaging_idempotency</c> table (Req 7). This is an
/// Infrastructure-only entity; it never leaves the persistence layer, keeping EF mapping types
/// out of the Domain.
/// </summary>
/// <remarks>
/// The primary key on <see cref="IdempotencyKey"/> makes the duplicate check a single indexed
/// lookup and makes record-on-success atomic (Req 7.1, 7.2, 7.4). Inserts use PostgreSQL
/// <c>ON CONFLICT DO NOTHING</c> semantics (wired in task 16.2) so a concurrent duplicate cannot
/// double-insert. <see cref="ExpiresAtUtc"/> encodes the retention window
/// (<c>processed_at_utc + retention</c>) a background sweep uses to prune expired rows (Req 7.3).
/// Column mapping lives in <see cref="IdempotencyRecordConfiguration"/>.
/// </remarks>
public sealed class IdempotencyRecord
{
    /// <summary>Primary key: the deterministic idempotency key (Req 7.1). Maps to <c>idempotency_key varchar(256)</c>.</summary>
    public string IdempotencyKey { get; set; } = string.Empty;

    /// <summary>Correlation id of the processed message, 1..128 chars (Req 4.1). Maps to <c>correlation_id varchar(128)</c>.</summary>
    public string CorrelationId { get; set; } = string.Empty;

    /// <summary>UTC time the message was processed (Req 7.3). Maps to <c>processed_at_utc timestamptz</c>.</summary>
    public DateTimeOffset ProcessedAtUtc { get; set; }

    /// <summary>UTC expiry: <c>processed_at_utc + retention</c>; rows past this are swept (Req 7.3). Maps to <c>expires_at_utc timestamptz</c>.</summary>
    public DateTimeOffset ExpiresAtUtc { get; set; }
}
