using System;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Contracts;
using Messaging.Persistence;
using Messaging.Persistence.Idempotency;
using Messaging.Persistence.Outbox;
using Microsoft.EntityFrameworkCore;

namespace Messaging.Persistence.Tests.Outbox;

/// <summary>
/// Integration tests for transactional outbox atomicity (task 22.1, Req 5.1): the outgoing
/// <see cref="MessageEnvelope"/> is recorded in the outbox within the <em>same</em> database
/// transaction as the originating state change, so either BOTH the state row and the outbox row
/// commit, or NEITHER does.
/// </summary>
/// <remarks>
/// <para>
/// <b>How "the originating state change" is modelled.</b> The messaging persistence context owns
/// two real tables — <c>messaging_idempotency</c> and <c>messaging_outbox</c>. These tests use an
/// <see cref="IdempotencyRecord"/> as the stand-in for an application's originating state row: it
/// is a genuine, independently-addressable row in a different table than the outbox, so observing
/// "state row present/absent" and "outbox row present/absent" are two independent reads. The point
/// under test is not the semantics of the idempotency table but that a state mutation and the
/// outbox write share one unit-of-work and commit or roll back together (Req 5.1).
/// </para>
/// <para>
/// <b>How atomicity is exercised honestly.</b> Both the state change and the outbox recording are
/// staged on one shared <see cref="MessagingDbContext"/> inside a single explicit
/// <c>Database.BeginTransactionAsync</c> scope — exactly the model <see cref="OutboxRecorder"/>
/// documents (stage via <c>Add</c>, never self-save; the caller commits once). The success case
/// commits the transaction; the failure case forces a rollback (the transaction is disposed
/// without a commit, which Npgsql rolls back), simulating the originating operation failing after
/// both rows were staged. Each case then reads back from a <em>fresh</em> no-tracking context so
/// the assertions observe durable database state, not the writer's change-tracker.
/// </para>
/// <para>
/// Both tests run against the real PostgreSQL container (one-engine rule — no in-memory substitute,
/// because transaction/rollback semantics must be the actual engine's) shared via
/// <see cref="PostgresCollection"/>. The fixture is injected through the constructor (xUnit supplies
/// the collection fixture to <c>[Fact]</c> constructors), and <see cref="PostgresContainerFixture.ResetAsync"/>
/// runs before each test so neither depends on the other's state or on execution order. Each case
/// also uses fresh GUID keys, so a committed row from one test can never be mistaken for another's.
/// </para>
/// </remarks>
[Trait("Feature", "messaging-core")]
[Collection(PostgresCollection.Name)]
public sealed class OutboxAtomicityIntegrationTests
{
    private readonly PostgresContainerFixture _fixture;

    /// <summary>
    /// Receives the shared PostgreSQL fixture via constructor injection (xUnit collection fixture).
    /// </summary>
    /// <param name="fixture">The shared PostgreSQL Testcontainer fixture.</param>
    public OutboxAtomicityIntegrationTests(PostgresContainerFixture fixture)
    {
        _fixture = fixture ?? throw new ArgumentNullException(nameof(fixture));
    }

    [Fact]
    public async Task CommitTransaction_WithStateChangeAndOutboxRecording_PersistsBothRows()
    {
        // Arrange: a clean store and fresh keys so this case is isolated from any other.
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var cancellationToken = cancellation.Token;

        await _fixture.ResetAsync(cancellationToken);

        var stateKey = $"{Guid.NewGuid():N}:state";
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid().ToString("N");

        // Act: stage the originating state change AND the outbox recording on one shared context,
        // then COMMIT once (Req 5.1 — both in the same transaction).
        await using (var context = await _fixture.CreateContextAsync(cancellationToken))
        await using (var transaction = await context.Database.BeginTransactionAsync(cancellationToken))
        {
            context.IdempotencyRecords.Add(BuildStateRow(stateKey, correlationId));

            new OutboxRecorder(context).Record(BuildEnvelope(messageId, correlationId));

            await context.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }

        // Assert: BOTH the state row and the outbox row are durably persisted.
        var stateRowPersisted = await StateRowExistsAsync(stateKey, cancellationToken);
        var outboxRowPersisted = await OutboxRowExistsAsync(messageId, cancellationToken);

        Assert.True(stateRowPersisted, "committed transaction must persist the originating state row");
        Assert.True(outboxRowPersisted, "committed transaction must persist the outbox row");
    }

    [Fact]
    public async Task RollbackTransaction_WithStateChangeAndOutboxRecording_PersistsNeitherRow()
    {
        // Arrange: a clean store and fresh keys so this case is isolated from any other.
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var cancellationToken = cancellation.Token;

        await _fixture.ResetAsync(cancellationToken);

        var stateKey = $"{Guid.NewGuid():N}:state";
        var messageId = Guid.NewGuid();
        var correlationId = Guid.NewGuid().ToString("N");

        // Act: stage the SAME state change + outbox recording on one shared context and SaveChanges
        // (so both rows are flushed within the transaction), then force a failure by rolling the
        // transaction back instead of committing (Req 5.1 — a failed originating operation).
        await using (var context = await _fixture.CreateContextAsync(cancellationToken))
        await using (var transaction = await context.Database.BeginTransactionAsync(cancellationToken))
        {
            context.IdempotencyRecords.Add(BuildStateRow(stateKey, correlationId));

            new OutboxRecorder(context).Record(BuildEnvelope(messageId, correlationId));

            await context.SaveChangesAsync(cancellationToken);

            // Forced failure: roll back the transaction. Disposing without a commit would also roll
            // back; the explicit rollback makes the "neither commits" intent unmistakable.
            await transaction.RollbackAsync(cancellationToken);
        }

        // Assert: NEITHER the state row nor the outbox row survived the rollback.
        var stateRowPersisted = await StateRowExistsAsync(stateKey, cancellationToken);
        var outboxRowPersisted = await OutboxRowExistsAsync(messageId, cancellationToken);

        Assert.False(stateRowPersisted, "rolled-back transaction must NOT persist the originating state row");
        Assert.False(outboxRowPersisted, "rolled-back transaction must NOT persist the outbox row");
    }

    /// <summary>
    /// Builds the originating state row (an <see cref="IdempotencyRecord"/> standing in for an
    /// application state mutation) with a far-future expiry so the retention sweep is irrelevant.
    /// </summary>
    private static IdempotencyRecord BuildStateRow(string stateKey, string correlationId)
    {
        var processedAtUtc = DateTimeOffset.UtcNow;

        return new IdempotencyRecord
        {
            IdempotencyKey = stateKey,
            CorrelationId = correlationId,
            ProcessedAtUtc = processedAtUtc,
            ExpiresAtUtc = processedAtUtc + TimeSpan.FromHours(24),
        };
    }

    /// <summary>
    /// Builds the outgoing envelope the recorder stages into the outbox within the shared
    /// transaction. The correlation id matches the state row so the two represent one logical
    /// unit-of-work.
    /// </summary>
    private static MessageEnvelope BuildEnvelope(Guid messageId, string correlationId) => new()
    {
        MessageId = messageId,
        CorrelationId = correlationId,
        MessageType = "messaging.outbox.atomicity.contract",
        CreatedAtUtc = DateTimeOffset.UtcNow,
        Headers = new Dictionary<string, string>(),
        Payload = new byte[] { 0x01, 0x02, 0x03 },
        ContractVersion = 1,
    };

    /// <summary>
    /// Reads back whether the originating state row exists, from a fresh no-tracking context so the
    /// assertion observes durable state rather than the writer's change-tracker.
    /// </summary>
    private async Task<bool> StateRowExistsAsync(string stateKey, CancellationToken cancellationToken)
    {
        await using var context = await _fixture.CreateContextAsync(cancellationToken);

        return await context.IdempotencyRecords
            .AsNoTracking()
            .AnyAsync(record => record.IdempotencyKey == stateKey, cancellationToken);
    }

    /// <summary>
    /// Reads back whether the outbox row exists, from a fresh no-tracking context so the assertion
    /// observes durable state rather than the writer's change-tracker.
    /// </summary>
    private async Task<bool> OutboxRowExistsAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await using var context = await _fixture.CreateContextAsync(cancellationToken);

        return await context.OutboxEntries
            .AsNoTracking()
            .AnyAsync(entry => entry.Id == messageId, cancellationToken);
    }
}
