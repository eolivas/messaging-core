using Messaging.Persistence.Idempotency;
using Messaging.Persistence.Outbox;
using Microsoft.EntityFrameworkCore;

namespace Messaging.Persistence;

/// <summary>
/// EF <see cref="DbContext"/> for the messaging persistence stores: the transactional outbox and
/// the idempotency store (Req 5, 7). It lives in the Infrastructure (persistence) package so no
/// EF type leaks into the Domain.
/// </summary>
/// <remarks>
/// <para>
/// Transactional model (Req 5.1): the outbox entry is recorded in the <em>same</em> database
/// transaction as the originating state change, so both commit together or neither does. Callers
/// achieve this by sharing one context/transaction across the state change and
/// <see cref="SaveChangesAsync(System.Threading.CancellationToken)"/>, or by enlisting via
/// <c>Database.BeginTransactionAsync</c>. This context exposes the sets and mapping needed for
/// that model; the dispatcher and store logic are added in tasks 16.2 and 17.
/// </para>
/// <para>
/// All access is parameterized through EF (code-conventions: parameterized access only). The
/// idempotency insert uses PostgreSQL <c>ON CONFLICT DO NOTHING</c> semantics, wired in task
/// 16.2. Migrations are never auto-applied at startup (steering); this context defines no
/// startup migration behavior.
/// </para>
/// </remarks>
public class MessagingDbContext : DbContext
{
    /// <summary>
    /// Creates the context with externally supplied options (connection string injected via DI,
    /// never committed to source).
    /// </summary>
    /// <param name="options">EF options configured by the embedding application.</param>
    public MessagingDbContext(DbContextOptions<MessagingDbContext> options)
        : base(options)
    {
    }

    /// <summary>The reliable-publish outbox entries, table <c>messaging_outbox</c> (Req 5).</summary>
    public DbSet<OutboxEntry> OutboxEntries => Set<OutboxEntry>();

    /// <summary>The processed-idempotency records, table <c>messaging_idempotency</c> (Req 7).</summary>
    public DbSet<IdempotencyRecord> IdempotencyRecords => Set<IdempotencyRecord>();

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.ApplyConfiguration(new OutboxEntryConfiguration());
        modelBuilder.ApplyConfiguration(new IdempotencyRecordConfiguration());
    }
}
