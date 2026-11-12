using Messaging.Abstractions;
using Messaging.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Background consume loop that pumps <see cref="KafkaConsumerHost.ConsumeOnceAsync"/> while the host
/// is <see cref="ConsumerHostState.Running"/> (Req 8.1, 8.2), survives a thrown iteration by logging
/// the correlation id only (never the payload), backing off a bounded interval and re-polling the same
/// uncommitted record (Req 8.5, 8.6, 8.7), and drains gracefully on shutdown within
/// <see cref="LifecycleConfig.ShutdownTimeoutSeconds"/> (Req 8.3, 8.4).
/// </summary>
/// <remarks>
/// The loop owns no offset or processing logic: that lives in the shell and the processor. In-flight
/// iterations receive a token that is cancelled only when the drain timeout elapses, so a normal stop
/// lets process-then-commit finish while a timed-out drain abandons the record before the commit.
/// <para>
/// Correlation id of a failed iteration: <c>ConsumeOnceAsync</c> does not surface it, so processors
/// that throw attach it under <see cref="CorrelationIdDataKey"/> in <see cref="Exception.Data"/>. When
/// absent the log records <c>unavailable</c>; the payload is never read or logged.
/// </para>
/// </remarks>
public sealed class KafkaConsumerHostedService : IHostedService
{
    /// <summary>Key under which a thrown exception may carry the failed record's correlation id.</summary>
    public const string CorrelationIdDataKey = "Messaging.CorrelationId";

    /// <summary>Minimum / maximum / default hosted-service backoff in seconds (Req 8.5).</summary>
    public const int MinBackoffSeconds = 1;

    /// <summary>Maximum backoff in seconds.</summary>
    public const int MaxBackoffSeconds = 60;

    /// <summary>Default backoff in seconds.</summary>
    public const int DefaultBackoffSeconds = 5;

    private const string UnavailableCorrelationId = "unavailable";
    private static readonly TimeSpan IdleDelay = TimeSpan.FromMilliseconds(500);

    private readonly KafkaConsumerHost _host;
    private readonly TimeSpan _shutdownTimeout;
    private readonly TimeSpan _backoff;
    private readonly ILogger<KafkaConsumerHostedService> _logger;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly TimeProvider _clock;

    private CancellationTokenSource? _stopLoop;
    private CancellationTokenSource? _abandonIteration;
    private Task _loop = Task.CompletedTask;

    /// <summary>Creates the hosted service. Optional parameters are test seams.</summary>
    /// <param name="host">The Kafka consumer host shell.</param>
    /// <param name="lifecycle">Lifecycle settings (graceful shutdown timeout).</param>
    /// <param name="logger">Structured logger.</param>
    /// <param name="backoff">Backoff after a failed iteration, 1..60s; default 5s.</param>
    /// <param name="delay">Delay seam; defaults to a <see cref="TimeProvider"/>-based delay.</param>
    /// <param name="clock">Clock seam; defaults to <see cref="TimeProvider.System"/>.</param>
    /// <exception cref="ArgumentNullException">If a required dependency is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">If a timeout is outside its bounds.</exception>
    public KafkaConsumerHostedService(
        KafkaConsumerHost host,
        LifecycleConfig lifecycle,
        ILogger<KafkaConsumerHostedService> logger,
        TimeSpan? backoff = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        TimeProvider? clock = null)
    {
        _host = host ?? throw new ArgumentNullException(nameof(host));
        ArgumentNullException.ThrowIfNull(lifecycle);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (lifecycle.ShutdownTimeoutSeconds is < 1 or > 300)
        {
            throw new ArgumentOutOfRangeException(
                nameof(lifecycle), lifecycle.ShutdownTimeoutSeconds, "ShutdownTimeoutSeconds must be 1..300.");
        }

        _shutdownTimeout = TimeSpan.FromSeconds(lifecycle.ShutdownTimeoutSeconds);
        _backoff = backoff ?? TimeSpan.FromSeconds(DefaultBackoffSeconds);
        if (_backoff < TimeSpan.FromSeconds(MinBackoffSeconds) || _backoff > TimeSpan.FromSeconds(MaxBackoffSeconds))
        {
            throw new ArgumentOutOfRangeException(nameof(backoff), _backoff, "Backoff must be 1..60 seconds.");
        }

        _clock = clock ?? TimeProvider.System;
        _delay = delay ?? ((span, ct) => Task.Delay(span, _clock, ct));
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _host.StartAsync(cancellationToken).ConfigureAwait(false);

        _stopLoop = new CancellationTokenSource();
        _abandonIteration = new CancellationTokenSource();
        CancellationToken stop = _stopLoop.Token;
        CancellationToken abandon = _abandonIteration.Token;

        // Run on the thread pool so a synchronous prefix never blocks host startup.
        _loop = Task.Run(() => RunLoopAsync(stop, abandon), CancellationToken.None);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stopLoop is null || _abandonIteration is null)
        {
            return;
        }

        // Stop starting new iterations; an in-flight one keeps its un-cancelled token and may finish.
        await _stopLoop.CancelAsync().ConfigureAwait(false);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task timeout = _delay(_shutdownTimeout, timeoutCts.Token);
        Task finished = await Task.WhenAny(_loop, timeout).ConfigureAwait(false);

        if (finished == _loop)
        {
            await timeoutCts.CancelAsync().ConfigureAwait(false);
            await SwallowAsync(timeout).ConfigureAwait(false);
        }
        else
        {
            // Drain exceeded the timeout: abandon the uncompleted record so it is not committed.
            _logger.LogWarning(
                "Kafka consumer drain exceeded the {TimeoutSeconds}s shutdown timeout; stopping without committing the in-flight record. Correlation id {CorrelationId}.",
                _shutdownTimeout.TotalSeconds,
                UnavailableCorrelationId);
            await _abandonIteration.CancelAsync().ConfigureAwait(false);
        }

        await SwallowAsync(_loop).ConfigureAwait(false);

        await _host.StopAsync(cancellationToken).ConfigureAwait(false);

        _stopLoop.Dispose();
        _abandonIteration.Dispose();
        _stopLoop = null;
        _abandonIteration = null;
    }

    private async Task RunLoopAsync(CancellationToken stop, CancellationToken abandon)
    {
        while (!stop.IsCancellationRequested)
        {
            if (_host.State != ConsumerHostState.Running)
            {
                // Not Running: never iterate; idle until the host is Running or we are stopped.
                if (!await TryDelayAsync(IdleDelay, stop).ConfigureAwait(false))
                {
                    return;
                }

                continue;
            }

            try
            {
                await _host.ConsumeOnceAsync(abandon).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (abandon.IsCancellationRequested || stop.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                // Covers RetainOffsetException, client-reported rebalances and any pipeline fault.
                // Log type + correlation id only: never the payload or exception message text.
                _logger.LogWarning(
                    "Kafka consume iteration failed ({ExceptionType}); correlation id {CorrelationId}. Backing off {BackoffSeconds}s then re-polling.",
                    ex.GetType().Name,
                    ReadCorrelationId(ex),
                    _backoff.TotalSeconds);

                if (!await TryDelayAsync(_backoff, stop).ConfigureAwait(false))
                {
                    return;
                }
            }
        }
    }

    private async Task<bool> TryDelayAsync(TimeSpan span, CancellationToken stop)
    {
        try
        {
            await _delay(span, stop).ConfigureAwait(false);
            return !stop.IsCancellationRequested;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private static string ReadCorrelationId(Exception ex) =>
        ex.Data[CorrelationIdDataKey] is string id && !string.IsNullOrWhiteSpace(id)
            ? id
            : UnavailableCorrelationId;

    private static async Task SwallowAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected when a delay or iteration is cancelled during shutdown.
        }
    }
}
