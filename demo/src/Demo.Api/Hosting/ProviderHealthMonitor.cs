using Demo.Application.Flow;
using Demo.Application.Sessions;

namespace Demo.Api.Hosting;

/// <summary>
/// Polls provider health every 5 s while a Session is Active and pushes a <c>session</c> event
/// when the reported health changes (Req 4.9).
/// </summary>
public sealed class ProviderHealthMonitor : BackgroundService
{
    public static readonly TimeSpan Interval = TimeSpan.FromSeconds(5);

    private readonly SessionStatusProvider _status;
    private readonly IFlowEventBroadcaster _broadcaster;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ProviderHealthMonitor> _logger;
    private string? _lastHealth;

    public ProviderHealthMonitor(
        SessionStatusProvider status,
        IFlowEventBroadcaster broadcaster,
        TimeProvider timeProvider,
        ILogger<ProviderHealthMonitor> logger)
    {
        _status = status ?? throw new ArgumentNullException(nameof(status));
        _broadcaster = broadcaster ?? throw new ArgumentNullException(nameof(broadcaster));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval, _timeProvider);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await CheckOnceAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected on shutdown.
        }
    }

    /// <summary>Runs one poll; exposed for tests.</summary>
    public async Task CheckOnceAsync(CancellationToken cancellationToken)
    {
        try
        {
            var snapshot = await _status.GetAsync(cancellationToken).ConfigureAwait(false);

            // Health is only meaningful while Active; transitions into and out of Active are
            // published by the session handlers, so only changes within Active are ours.
            var wasActive = _lastHealth is not null;
            var health = snapshot.State == "active" ? snapshot.Health : null;
            var changed = !string.Equals(health, _lastHealth, StringComparison.Ordinal);
            _lastHealth = health;

            if (changed && wasActive && health is not null)
            {
                _broadcaster.Publish(new SessionChangedEvent(snapshot));
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Provider health poll failed.");
        }
    }
}
