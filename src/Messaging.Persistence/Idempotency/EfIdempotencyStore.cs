using Messaging.Abstractions;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;

namespace Messaging.Persistence.Idempotency;

/// <summary>
/// EF/PostgreSQL-backed <see cref="IIdempotencyStore"/> adapter (Req 7). It lives in the
/// Infrastructure (persistence) package and implements the Domain port, so the Application layer
/// detects duplicates and records processed keys without taking any EF dependency (Clean
/// Architecture, DIP).
/// </summary>
/// <remarks>
/// <para>
/// <b>Duplicate check (Req 7.2).</b> <see cref="IsProcessedAsync"/> is a single indexed lookup
/// against the <c>idempotency_key</c> primary key via <see cref="EntityFrameworkQueryableExtensions.AnyAsync{TSource}(IQueryable{TSource}, System.Linq.Expressions.Expression{System.Func{TSource, bool}}, System.Threading.CancellationToken)"/>,
/// which EF translates to <c>EXISTS (SELECT 1 ... WHERE idempotency_key = @p)</c> — a parameterized
/// index seek, never a table scan.
/// </para>
/// <para>
/// <b>Record on success (Req 7.3, 7.4).</b> <see cref="RecordProcessedAsync(string, TimeSpan, CancellationToken)"/>
/// persists the key with <c>processed_at_utc = now</c> and <c>expires_at_utc = now + retention</c>
/// using a parameterized <c>INSERT ... ON CONFLICT (idempotency_key) DO NOTHING</c>. The
/// <c>ON CONFLICT DO NOTHING</c> makes the record idempotent and atomic: a concurrent duplicate
/// cannot double-insert, and a redelivered-but-already-recorded key is a no-op rather than a
/// primary-key violation. SQL is passed through
/// <see cref="RelationalDatabaseFacadeExtensions.ExecuteSqlInterpolatedAsync(Microsoft.EntityFrameworkCore.Infrastructure.DatabaseFacade, FormattableString, CancellationToken)"/>,
/// so every value is a bound parameter — no string-concatenated SQL (code-conventions).
/// </para>
/// <para>
/// <b>Recording failure surfaces (Req 7.5).</b> If the insert fails (e.g. a connection error), the
/// error is wrapped in <see cref="IdempotencyRecordingException"/> and rethrown — never swallowed —
/// so the consume pipeline does not acknowledge the message and the stored record is left
/// unchanged. The record and the acknowledgement therefore cannot diverge.
/// </para>
/// <para>
/// <b>Correlation id and the Domain port.</b> The <see cref="IIdempotencyStore"/> port intentionally
/// does <em>not</em> carry a correlation id, yet <c>messaging_idempotency.correlation_id</c> is
/// NOT NULL and semantically meaningful. The codebase propagates the correlation id explicitly on
/// the <see cref="Messaging.Contracts.MessageEnvelope"/> and the <c>x-correlation-id</c> header
/// (there is no ambient correlation-id accessor to inject). To honor the port without changing it,
/// this adapter exposes an <see langword="internal"/>
/// <see cref="RecordProcessedAsync(string, string, TimeSpan, CancellationToken)"/> overload that
/// takes the correlation id; the record-on-success call site, which holds the envelope, calls that
/// overload with the real correlation id. The public port method delegates to the overload with the
/// documented <see cref="UnknownCorrelationId"/> sentinel (a valid, non-empty marker) for callers
/// that reach the store only through the port, so the NOT NULL column is always satisfied and an
/// empty string is never persisted.
/// </para>
/// </remarks>
public sealed class EfIdempotencyStore : IIdempotencyStore
{
    /// <summary>
    /// Non-empty sentinel stored in <c>correlation_id</c> when a caller records through the
    /// correlation-id-free Domain port. The envelope-aware internal overload supplies the real id;
    /// this marker only guards the NOT NULL column so an empty string is never written.
    /// </summary>
    public const string UnknownCorrelationId = "unknown";

    private readonly MessagingDbContext _dbContext;

    /// <summary>
    /// Creates the store over the shared <see cref="MessagingDbContext"/> (constructor injection
    /// only; connection string injected via DI, never committed to source).
    /// </summary>
    /// <param name="dbContext">The messaging persistence context.</param>
    /// <exception cref="ArgumentNullException">If <paramref name="dbContext"/> is null.</exception>
    public EfIdempotencyStore(MessagingDbContext dbContext)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
    }

    /// <inheritdoc />
    /// <remarks>Single indexed lookup against the <c>idempotency_key</c> primary key (Req 7.2).</remarks>
    /// <exception cref="ArgumentException">If <paramref name="idempotencyKey"/> is null or empty.</exception>
    public Task<bool> IsProcessedAsync(string idempotencyKey, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(idempotencyKey);

        // AnyAsync on the PK => EXISTS over an index seek with a bound parameter (no table scan).
        return _dbContext.IdempotencyRecords
            .AsNoTracking()
            .AnyAsync(record => record.IdempotencyKey == idempotencyKey, cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Delegates to <see cref="RecordProcessedAsync(string, string, TimeSpan, CancellationToken)"/>
    /// with the <see cref="UnknownCorrelationId"/> sentinel because the Domain port does not carry a
    /// correlation id; envelope-aware callers use the internal overload with the real id.
    /// </remarks>
    /// <exception cref="ArgumentException">If <paramref name="idempotencyKey"/> is null or empty.</exception>
    /// <exception cref="IdempotencyRecordingException">If persisting the record fails (Req 7.5).</exception>
    public Task RecordProcessedAsync(string idempotencyKey, TimeSpan retention, CancellationToken cancellationToken)
        => RecordProcessedAsync(idempotencyKey, UnknownCorrelationId, retention, cancellationToken);

    /// <summary>
    /// Records the processed key together with its originating correlation id for the retention
    /// window (Req 7.3, 7.4). Preferred over the port method by the record-on-success call site,
    /// which holds the <see cref="Messaging.Contracts.MessageEnvelope"/> and so has the real
    /// correlation id to persist into the NOT NULL <c>correlation_id</c> column.
    /// </summary>
    /// <param name="idempotencyKey">The deterministic idempotency key to record.</param>
    /// <param name="correlationId">The correlation id of the processed message (1..128 chars).</param>
    /// <param name="retention">How long the recorded key is retained; <c>expires_at_utc = now + retention</c>.</param>
    /// <param name="cancellationToken">Token to cancel the recording.</param>
    /// <exception cref="ArgumentException">If <paramref name="idempotencyKey"/> or <paramref name="correlationId"/> is null or empty.</exception>
    /// <exception cref="IdempotencyRecordingException">If persisting the record fails (Req 7.5).</exception>
    internal async Task RecordProcessedAsync(
        string idempotencyKey,
        string correlationId,
        TimeSpan retention,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(idempotencyKey);
        ArgumentException.ThrowIfNullOrEmpty(correlationId);

        var processedAtUtc = DateTimeOffset.UtcNow;
        var expiresAtUtc = processedAtUtc + retention;

        try
        {
            // Parameterized INSERT ... ON CONFLICT DO NOTHING: a concurrent duplicate cannot
            // double-insert and a redelivered-but-recorded key is a no-op (Req 7.4).
            //
            // The table name is a SQL *identifier*, not a value, so it cannot be a bound parameter:
            // PostgreSQL rejects "INSERT INTO $1 ..." with 42601. It is a compile-time constant we
            // own (never user input), so it is composed into the SQL text. Only the four values
            // become bound parameters ($1..$4) — no value data is ever concatenated into SQL
            // (code-conventions: parameterize untrusted/value data; identifiers can't be parameters).
            var sql = FormattableStringFactory.Create(
                @"INSERT INTO " + IdempotencyRecordConfiguration.TableName + @" (idempotency_key, correlation_id, processed_at_utc, expires_at_utc)
                   VALUES ({0}, {1}, {2}, {3})
                   ON CONFLICT (idempotency_key) DO NOTHING",
                idempotencyKey,
                correlationId,
                processedAtUtc,
                expiresAtUtc);

            await _dbContext.Database.ExecuteSqlInterpolatedAsync(sql, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancellation is a control-flow signal, not a recording failure: let it propagate as-is.
            throw;
        }
        catch (Exception exception)
        {
            // Surface the failure (Req 7.5): the caller must NOT acknowledge the message, and the
            // stored record is left unchanged. Wrap in a domain-specific exception, never swallow.
            throw new IdempotencyRecordingException(idempotencyKey, exception);
        }
    }
}
