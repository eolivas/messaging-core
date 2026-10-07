using Messaging.Abstractions;
using Messaging.Contracts;

namespace Messaging.Persistence.Outbox;

/// <summary>
/// EF-backed <see cref="IOutboxRecorder"/> that stages an outgoing <see cref="MessageEnvelope"/>
/// as a pending <see cref="OutboxEntry"/> on the shared <see cref="MessagingDbContext"/> (Req 5.1).
/// </summary>
/// <remarks>
/// <para>
/// <b>Transactional recording (Req 5.1).</b> <see cref="Record"/> only calls
/// <see cref="Microsoft.EntityFrameworkCore.DbSet{TEntity}.Add(TEntity)"/>; it never calls
/// <c>SaveChanges</c>. Atomicity therefore depends on the caller sharing this adapter's
/// <see cref="MessagingDbContext"/> with its own state change and committing exactly once (for
/// example inside a single <c>Database.BeginTransactionAsync</c> scope or one
/// <c>SaveChangesAsync</c> call). When the caller commits, the originating state change and the
/// outbox row commit together or neither does; if the caller never commits, nothing is written.
/// Recording outside the caller's unit-of-work would break the atomicity guarantee.
/// </para>
/// <para>
/// <b>Mapping.</b> The entry's <see cref="OutboxEntry.Id"/> is the envelope's
/// <see cref="MessageEnvelope.MessageId"/> (Req 4.1), carrying the correlation id, message type,
/// contract version, and headers verbatim; the payload is copied out of the
/// <see cref="System.ReadOnlyMemory{T}"/> into an owned array for persistence. The entry starts
/// <see cref="OutboxStatus.Pending"/> with a zero attempt count, awaiting the dispatcher
/// (Req 5.2). <see cref="OutboxEntry.CreatedAtUtc"/> is the envelope's creation time — the time
/// the entry represents being recorded for dispatch.
/// </para>
/// </remarks>
public sealed class OutboxRecorder : IOutboxRecorder
{
    private readonly MessagingDbContext _dbContext;

    /// <summary>
    /// Creates the recorder over the shared <see cref="MessagingDbContext"/> (constructor
    /// injection only; the context must be the same unit-of-work the caller commits).
    /// </summary>
    /// <param name="dbContext">The shared messaging persistence context.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="dbContext"/> is null.</exception>
    public OutboxRecorder(MessagingDbContext dbContext)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
    }

    /// <inheritdoc />
    /// <exception cref="ArgumentNullException">If <paramref name="envelope"/> is null.</exception>
    public OutboxEntry Record(MessageEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var entry = new OutboxEntry
        {
            Id = envelope.MessageId,
            CorrelationId = envelope.CorrelationId,
            MessageType = envelope.MessageType,
            ContractVersion = envelope.ContractVersion,
            Headers = new Dictionary<string, string>(
                envelope.Headers.ToDictionary(pair => pair.Key, pair => pair.Value)),
            Payload = envelope.Payload.ToArray(),
            Status = OutboxStatus.Pending,
            AttemptCount = 0,
            CreatedAtUtc = envelope.CreatedAtUtc,
            DispatchedAtUtc = null,
            LastError = null,
        };

        // Stage only — do NOT persist. The entry enlists in the caller's transaction/unit-of-work
        // and commits atomically with the originating state change when the caller saves (Req 5.1).
        _dbContext.OutboxEntries.Add(entry);

        return entry;
    }
}
