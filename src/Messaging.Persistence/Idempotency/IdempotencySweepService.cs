using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Messaging.Persistence.Idempotency;

/// <summary>
/// Background service that periodically prunes expired idempotency rows — those whose
/// <c>expires_at_utc</c> is before the current UTC time — so the <c>messaging_idempotency</c> table
/// does not grow unbounded once each key's retention window elapses (Req 7.3).
/// </summary>
/// <remarks>
/// <para>
/// <b>Own scope per pass.</b> <see cref="MessagingDbContext"/> is a scoped dependency, so this
/// singleton <see cref="BackgroundService"/> resolves it from a fresh
/// <see cref="IServiceScope"/> on every pass via <see cref="IServiceScopeFactory"/> rather than
/// capturing a long-lived context (which would be an EF lifetime violation).
/// </para>
/// <para>
/// <b>Parameterized bulk delete.</b> Each pass issues a single set-based
/// <see cref="EntityFrameworkQueryableExtensions.ExecuteDeleteAsync{TSource}(IQueryable{TSource}, CancellationToken)"/>
/// over <c>expires_at_utc &lt; now</c>. The <c>now</c> value is a captured local, so EF binds it as a
/// parameter — no string-concatenated SQL (code-conventions) — and no entities are loaded into
/// memory.
/// </para>
/// <para>
/// <b>Resilient and cooperative.</b> A failed pass is logged and swallowed so one transient
/// database error does not crash the host; the loop simply waits for the next interval. The
/// configured <see cref="CancellationToken"/> stops both the delay and the delete promptly on
/// shutdown; cancellation is treated as a normal stop, not an error.
/// </para>
/// <para>
/// Structured logging only, and no secrets or PII: the sweep logs the interval and the number of
/// rows removed, never key values or payloads (code-conventions security).
/// </para>
/// </remarks>
public sealed partial class IdempotencySweepService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<IdempotencySweepService> _logger;
    private readonly TimeSpan _sweepInterval;

    /// <summary>
    /// Creates the sweep service.
    /// </summary>
    /// <param name="scopeFactory">Factory used to create a scope (and scoped <see cref="MessagingDbContext"/>) per pass.</param>
    /// <param name="options">Sweep settings; a sub-minimum interval is clamped to <see cref="IdempotencySweepOptions.MinimumInterval"/>.</param>
    /// <param name="logger">Structured logger for sweep outcomes.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public IdempotencySweepService(
        IServiceScopeFactory scopeFactory,
        IdempotencySweepOptions options,
        ILogger<IdempotencySweepService> logger)
    {
        _scopeFactory = scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory));
        ArgumentNullException.ThrowIfNull(options);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Guard against a hot-loop misconfiguration (e.g. zero/negative interval).
        _sweepInterval = options.SweepInterval < IdempotencySweepOptions.MinimumInterval
            ? IdempotencySweepOptions.MinimumInterval
            : options.SweepInterval;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogStarted(_sweepInterval);

        using var timer = new PeriodicTimer(_sweepInterval);

        // Run an immediate first pass, then one pass per interval tick until shutdown.
        do
        {
            await SweepOnceAsync(stoppingToken).ConfigureAwait(false);
        }
        while (await WaitForNextTickAsync(timer, stoppingToken).ConfigureAwait(false));

        LogStopped();
    }

    private static async Task<bool> WaitForNextTickAsync(PeriodicTimer timer, CancellationToken stoppingToken)
    {
        try
        {
            return await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Shutdown requested: stop looping without surfacing cancellation as an error.
            return false;
        }
    }

    private async Task SweepOnceAsync(CancellationToken stoppingToken)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<MessagingDbContext>();

            // Capture 'now' as a local so EF binds it as a parameter in the delete predicate.
            var now = DateTimeOffset.UtcNow;

            var removed = await dbContext.IdempotencyRecords
                .Where(record => record.ExpiresAtUtc < now)
                .ExecuteDeleteAsync(stoppingToken)
                .ConfigureAwait(false);

            LogSweepCompleted(removed, _sweepInterval);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown during a pass: not an error.
        }
        catch (Exception exception)
        {
            // Resilience: one failed pass must not crash the host (Req 7.3). Log and continue; the
            // next tick retries. No key values or payloads are logged (security).
            LogSweepFailed(exception);
        }
    }

    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "Idempotency retention sweep started; interval {SweepInterval}.")]
    private partial void LogStarted(TimeSpan sweepInterval);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Idempotency retention sweep removed {RemovedCount} expired row(s); next pass in {SweepInterval}.")]
    private partial void LogSweepCompleted(int removedCount, TimeSpan sweepInterval);

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Error,
        Message = "Idempotency retention sweep pass failed; the host continues and the next pass will retry.")]
    private partial void LogSweepFailed(Exception exception);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Information,
        Message = "Idempotency retention sweep stopped.")]
    private partial void LogStopped();
}
