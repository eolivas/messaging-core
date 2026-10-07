using Messaging.Abstractions;
using Messaging.Configuration;
using Messaging.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Messaging.Persistence.Outbox;

/// <summary>
/// EF/PostgreSQL-backed <see cref="IOutboxDispatcher"/> that reads pending outbox entries and
/// publishes them through the <see cref="IMessagePublisher"/>, providing reliable at-least-once
/// delivery of committed entries (Req 5.2–5.6).
/// </summary>
/// <remarks>
/// <para>
/// <b>Pending scan (Req 5.2).</b> A dispatch pass queries entries with
/// <see cref="OutboxStatus.Pending"/> ordered by <see cref="OutboxEntry.CreatedAtUtc"/>, served by
/// the <c>(status, created_at_utc)</c> index. The query is parameterized EF LINQ — never
/// string-concatenated SQL (code-conventions). The captured <c>Pending</c> value binds as a
/// parameter.
/// </para>
/// <para>
/// <b>Dispatch and confirm (Req 5.3, 5.6).</b> For each pending entry the dispatcher materialises
/// a <see cref="MessageEnvelope"/> and calls <see cref="IMessagePublisher.PublishAsync"/>. The
/// entry is marked <see cref="OutboxStatus.Dispatched"/> with a non-null
/// <see cref="OutboxEntry.DispatchedAtUtc"/> <em>only after</em> the publisher confirms
/// (<see cref="PublishResult.Succeeded"/>), which excludes it from every future pending scan — so
/// a committed entry is never lost and, once dispatched, never dispatched again.
/// </para>
/// <para>
/// <b>Failure handling (Req 5.4, 5.5).</b> When publication is not confirmed — a failed
/// <see cref="PublishResult"/> or any thrown exception — the entry stays
/// <see cref="OutboxStatus.Pending"/>, its <see cref="OutboxEntry.AttemptCount"/> is incremented,
/// and a short, redacted <see cref="OutboxEntry.LastError"/> is recorded (never a payload or
/// secret, Req 9.7). The dispatcher only ever mutates the outbox row, so the originating state
/// change is left untouched. Once an entry has accumulated
/// <see cref="OutboxConfig.MaxDispatchAttempts"/> failed attempts (default 10) it is marked
/// <see cref="OutboxStatus.Failed"/> and excluded from further automatic dispatch (Req 5.5).
/// </para>
/// <para>
/// <b>Per-entry persistence.</b> Each entry's transition is saved with its own
/// <c>SaveChangesAsync</c> inside its own try/catch, so one entry's failure neither rolls back
/// another entry's progress nor aborts the pass; the loop continues to the next entry. All I/O is
/// async, takes a <see cref="CancellationToken"/>, and uses <c>ConfigureAwait(false)</c>.
/// </para>
/// </remarks>
public sealed partial class EfOutboxDispatcher : IOutboxDispatcher
{
    /// <summary>Upper bound on the length of a persisted <see cref="OutboxEntry.LastError"/> (redaction/size guard).</summary>
    private const int MaxLastErrorLength = 512;

    private readonly MessagingDbContext _dbContext;
    private readonly IMessagePublisher _publisher;
    private readonly int _maxDispatchAttempts;
    private readonly ILogger<EfOutboxDispatcher> _logger;

    /// <summary>
    /// Creates the dispatcher (constructor injection only; no service locator/statics).
    /// </summary>
    /// <param name="dbContext">The shared messaging persistence context.</param>
    /// <param name="publisher">The transport-neutral publishing port entries are dispatched through.</param>
    /// <param name="outboxConfig">Outbox settings supplying the dispatch-attempt cap (Req 5.5).</param>
    /// <param name="logger">Structured logger for dispatch outcomes.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">If <see cref="OutboxConfig.MaxDispatchAttempts"/> is not positive.</exception>
    public EfOutboxDispatcher(
        MessagingDbContext dbContext,
        IMessagePublisher publisher,
        OutboxConfig outboxConfig,
        ILogger<EfOutboxDispatcher> logger)
    {
        _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        ArgumentNullException.ThrowIfNull(outboxConfig);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        ArgumentOutOfRangeException.ThrowIfLessThan(outboxConfig.MaxDispatchAttempts, 1);
        _maxDispatchAttempts = outboxConfig.MaxDispatchAttempts;
    }

    /// <inheritdoc />
    public async Task DispatchPendingAsync(CancellationToken cancellationToken)
    {
        // Parameterized pending scan over the (status, created_at_utc) index (Req 5.2). The
        // captured status is bound as a parameter; no string SQL.
        const OutboxStatus pending = OutboxStatus.Pending;

        var pendingEntries = await _dbContext.OutboxEntries
            .Where(entry => entry.Status == pending)
            .OrderBy(entry => entry.CreatedAtUtc)
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);

        var dispatchedCount = 0;
        var failedCount = 0;

        foreach (var entry in pendingEntries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (await TryDispatchEntryAsync(entry, cancellationToken).ConfigureAwait(false))
            {
                dispatchedCount++;
            }
            else
            {
                failedCount++;
            }
        }

        LogPassCompleted(pendingEntries.Count, dispatchedCount, failedCount);
    }

    /// <summary>
    /// Dispatches a single entry and persists its transition, isolated so one entry's failure does
    /// not affect another's progress.
    /// </summary>
    /// <returns><c>true</c> when the publisher confirmed and the entry was marked dispatched; otherwise <c>false</c>.</returns>
    private async Task<bool> TryDispatchEntryAsync(OutboxEntry entry, CancellationToken cancellationToken)
    {
        try
        {
            var envelope = ToEnvelope(entry);

            var result = await _publisher.PublishAsync(envelope, cancellationToken).ConfigureAwait(false);

            if (result.Succeeded)
            {
                MarkDispatched(entry);
            }
            else
            {
                MarkFailure(entry, result.Error ?? "Publish was not confirmed.");
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Cooperative shutdown mid-pass: do not persist a spurious failure for this entry.
            throw;
        }
        catch (Exception exception)
        {
            // Any publish exception is a transient failure for this entry (Req 5.4): keep it
            // pending, count the attempt, record a redacted reason. The originating state change is
            // untouched — only this outbox row is mutated.
            MarkFailure(entry, DescribeException(exception));
        }

        var dispatched = entry.Status == OutboxStatus.Dispatched;

        try
        {
            // Per-entry save so one row's failure neither rolls back nor blocks another (Req 5.4).
            await _dbContext.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            // Persisting the transition failed: log and continue to the next entry. The entry stays
            // whatever it was before this save (durably Pending in the database), so it remains
            // eligible for a later pass — at-least-once is preserved.
            LogPersistFailed(entry.Id, exception);
            return false;
        }

        if (dispatched)
        {
            LogEntryDispatched(entry.Id, entry.AttemptCount);
        }
        else if (entry.Status == OutboxStatus.Failed)
        {
            LogEntryExhausted(entry.Id, entry.AttemptCount);
        }
        else
        {
            LogEntryRetained(entry.Id, entry.AttemptCount);
        }

        return dispatched;
    }

    /// <summary>Marks a confirmed entry dispatched so it is excluded from future scans (Req 5.3, 5.6).</summary>
    private static void MarkDispatched(OutboxEntry entry)
    {
        entry.Status = OutboxStatus.Dispatched;
        entry.DispatchedAtUtc = DateTimeOffset.UtcNow;
        entry.LastError = null;
    }

    /// <summary>
    /// Records a failed attempt: increments the attempt count, stores a redacted reason, and marks
    /// the entry <see cref="OutboxStatus.Failed"/> once the attempt cap is reached (Req 5.4, 5.5).
    /// </summary>
    private void MarkFailure(OutboxEntry entry, string reason)
    {
        entry.AttemptCount++;
        entry.LastError = Redact(reason);

        // Boundary (Req 5.5): once an entry has had MaxDispatchAttempts failed attempts it is
        // excluded from further automatic dispatch; before that it stays Pending for a later pass.
        entry.Status = entry.AttemptCount >= _maxDispatchAttempts
            ? OutboxStatus.Failed
            : OutboxStatus.Pending;
    }

    /// <summary>Materialises the transport-neutral envelope from a persisted outbox row.</summary>
    private static MessageEnvelope ToEnvelope(OutboxEntry entry) => new()
    {
        MessageId = entry.Id,
        CorrelationId = entry.CorrelationId,
        MessageType = entry.MessageType,
        CreatedAtUtc = entry.CreatedAtUtc,
        Headers = new Dictionary<string, string>(
            entry.Headers.ToDictionary(pair => pair.Key, pair => pair.Value)),
        Payload = entry.Payload,
        ContractVersion = entry.ContractVersion,
    };

    /// <summary>
    /// Builds a short, safe failure description from an exception — type and message only, never a
    /// payload or secret (Req 9.7).
    /// </summary>
    private static string DescribeException(Exception exception)
        => $"{exception.GetType().Name}: {exception.Message}";

    /// <summary>Clamps a failure reason to a bounded length so <c>last_error</c> stays small (Req 9.7).</summary>
    private static string Redact(string reason)
    {
        if (string.IsNullOrEmpty(reason))
        {
            return "Unknown error.";
        }

        return reason.Length <= MaxLastErrorLength
            ? reason
            : reason[..MaxLastErrorLength];
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Debug,
        Message = "Outbox dispatch pass scanned {ScannedCount} pending entry(ies): {DispatchedCount} dispatched, {FailedCount} retained or failed.")]
    private partial void LogPassCompleted(int scannedCount, int dispatchedCount, int failedCount);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Outbox entry {OutboxEntryId} dispatched after {AttemptCount} attempt(s).")]
    private partial void LogEntryDispatched(Guid outboxEntryId, int attemptCount);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Debug,
        Message = "Outbox entry {OutboxEntryId} publish not confirmed; retained Pending after {AttemptCount} attempt(s).")]
    private partial void LogEntryRetained(Guid outboxEntryId, int attemptCount);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Warning,
        Message = "Outbox entry {OutboxEntryId} reached the dispatch-attempt cap after {AttemptCount} attempt(s) and was marked Failed.")]
    private partial void LogEntryExhausted(Guid outboxEntryId, int attemptCount);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Error,
        Message = "Outbox entry {OutboxEntryId} transition failed to persist; the entry remains durably Pending and will be retried on a later pass.")]
    private partial void LogPersistFailed(Guid outboxEntryId, Exception exception);
}
