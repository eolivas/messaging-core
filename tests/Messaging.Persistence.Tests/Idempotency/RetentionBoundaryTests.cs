using System;
using System.Threading;
using System.Threading.Tasks;
using Messaging.Persistence;
using Messaging.Persistence.Idempotency;
using Microsoft.EntityFrameworkCore;

namespace Messaging.Persistence.Tests.Idempotency;

/// <summary>
/// Unit tests for the idempotency retention boundary (task 16.5, Req 7.3): a recorded key keeps
/// blocking while it is still inside its retention window, and stops blocking once it is past
/// <c>expires_at_utc</c> and the retention sweep has pruned it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the boundary is tested against the sweep, not <see cref="EfIdempotencyStore.IsProcessedAsync"/>
/// alone.</b> <see cref="EfIdempotencyStore.IsProcessedAsync"/> is a straight primary-key existence
/// check — it does NOT filter on <c>expires_at_utc</c>. The retention boundary is therefore enforced
/// by the background <see cref="IdempotencySweepService"/>, which deletes rows whose
/// <c>expires_at_utc</c> is already in the past; after a sweep removes an expired row, the PK lookup
/// naturally returns <see langword="false"/>. These tests exercise exactly that real behaviour:
/// record (or age) a row, run the sweep's prune, then assert the PK lookup.
/// </para>
/// <para>
/// <b>How the sweep is run deterministically.</b> Hosting the real <see cref="IdempotencySweepService"/>
/// would introduce its <see cref="System.Threading.PeriodicTimer"/> and a background scope, which is
/// non-deterministic in a unit test. Instead these tests invoke the exact same parameterized prune
/// predicate the service uses in <c>SweepOnceAsync</c> — capturing <c>now</c> as a local so EF binds
/// it as a parameter — directly against the real context:
/// <c>IdempotencyRecords.Where(r =&gt; r.ExpiresAtUtc &lt; now).ExecuteDeleteAsync(ct)</c>. The service's
/// delete path is <see langword="private"/>; replicating this single-line predicate (rather than
/// adding <c>InternalsVisibleTo</c> or a production refactor just to reach it) keeps the test
/// deterministic without changing production code.
/// </para>
/// <para>
/// Both tests run against the real PostgreSQL container (one-engine rule — no in-memory substitute)
/// shared via <see cref="PostgresCollection"/>. The fixture is injected through the constructor
/// (xUnit supplies the collection fixture to <c>[Fact]</c> constructors), and
/// <see cref="PostgresContainerFixture.ResetAsync"/> runs before each test so neither depends on the
/// other's state or on execution order.
/// </para>
/// </remarks>
[Trait("Feature", "messaging-core")]
[Collection(PostgresCollection.Name)]
public sealed class RetentionBoundaryTests
{
    private readonly PostgresContainerFixture _fixture;

    /// <summary>
    /// Receives the shared PostgreSQL fixture via constructor injection (xUnit collection fixture).
    /// </summary>
    /// <param name="fixture">The shared PostgreSQL Testcontainer fixture.</param>
    public RetentionBoundaryTests(PostgresContainerFixture fixture)
    {
        _fixture = fixture ?? throw new ArgumentNullException(nameof(fixture));
    }

    [Fact]
    public async Task IsProcessedAsync_KeyWithinRetentionWindow_StillBlocksAfterSweep()
    {
        // Arrange: a clean store and a key recorded with a positive retention, so expires_at_utc is
        // comfortably in the future and the sweep must NOT remove it.
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var cancellationToken = cancellation.Token;

        await _fixture.ResetAsync(cancellationToken);

        var idempotencyKey = $"{Guid.NewGuid():N}:within-window";
        var retention = TimeSpan.FromHours(24);

        await using var context = await _fixture.CreateContextAsync(cancellationToken);
        var store = new EfIdempotencyStore(context);

        await store.RecordProcessedAsync(idempotencyKey, retention, cancellationToken);

        // Act: run the sweep's prune (same parameterized predicate the service uses). The row's
        // expiry is in the future, so this pass should delete nothing.
        await PruneExpiredAsync(context, cancellationToken);

        // Assert: the key is still inside its window, so it keeps blocking (duplicate detected).
        var stillBlocks = await store.IsProcessedAsync(idempotencyKey, cancellationToken);
        Assert.True(stillBlocks);
    }

    [Fact]
    public async Task IsProcessedAsync_KeyPastExpiresAtUtc_NoLongerBlocksAfterSweep()
    {
        // Arrange: a clean store and a key whose expires_at_utc is already in the PAST. The row is
        // added directly through the context so its expiry can be set to a past instant (the store's
        // RecordProcessedAsync always sets expires_at_utc = now + retention, which cannot be past).
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var cancellationToken = cancellation.Token;

        await _fixture.ResetAsync(cancellationToken);

        var idempotencyKey = $"{Guid.NewGuid():N}:past-expiry";

        await using var context = await _fixture.CreateContextAsync(cancellationToken);
        var store = new EfIdempotencyStore(context);

        var processedAtUtc = DateTimeOffset.UtcNow - TimeSpan.FromMinutes(2);
        context.IdempotencyRecords.Add(new IdempotencyRecord
        {
            IdempotencyKey = idempotencyKey,
            CorrelationId = "retention-boundary-test",
            ProcessedAtUtc = processedAtUtc,
            ExpiresAtUtc = processedAtUtc + TimeSpan.FromMinutes(1), // expired 1 min ago
        });
        await context.SaveChangesAsync(cancellationToken);

        // Sanity: before the sweep the row exists, so the PK lookup alone still reports it as
        // processed (IsProcessedAsync does not filter on expiry — the sweep enforces the boundary).
        Assert.True(await store.IsProcessedAsync(idempotencyKey, cancellationToken));

        // Act: run the sweep's prune. The row is past expiry, so this pass removes it.
        await PruneExpiredAsync(context, cancellationToken);

        // Assert: the expired key was swept, so it no longer blocks.
        var stillBlocks = await store.IsProcessedAsync(idempotencyKey, cancellationToken);
        Assert.False(stillBlocks);
    }

    /// <summary>
    /// Runs the retention sweep's prune deterministically: the exact parameterized bulk delete
    /// <see cref="IdempotencySweepService"/> issues per pass — <c>expires_at_utc &lt; now</c>, with
    /// <c>now</c> captured as a local so EF binds it as a parameter (no concatenated SQL).
    /// </summary>
    /// <param name="context">The messaging context bound to the shared container.</param>
    /// <param name="cancellationToken">Token to cancel the delete.</param>
    private static Task PruneExpiredAsync(MessagingDbContext context, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;

        return context.IdempotencyRecords
            .Where(record => record.ExpiresAtUtc < now)
            .ExecuteDeleteAsync(cancellationToken);
    }
}
