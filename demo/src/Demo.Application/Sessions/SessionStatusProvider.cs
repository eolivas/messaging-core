using Demo.Application.Providers;
using Microsoft.Extensions.Logging;

namespace Demo.Application.Sessions;

/// <summary>Builds the <see cref="SessionStatusDto"/> from the coordinator, the cluster and the catalog.</summary>
public sealed class SessionStatusProvider
{
    public const string HealthReachable = "reachable";
    public const string HealthUnreachable = "unreachable";
    public const string HealthUnknown = "unknown";
    public static readonly TimeSpan HealthTimeout = TimeSpan.FromSeconds(2);

    private readonly SessionCoordinator _coordinator;
    private readonly IMicroserviceCluster _cluster;
    private readonly IProviderCatalog _catalog;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<SessionStatusProvider> _logger;

    public SessionStatusProvider(
        SessionCoordinator coordinator,
        IMicroserviceCluster cluster,
        IProviderCatalog catalog,
        TimeProvider timeProvider,
        ILogger<SessionStatusProvider> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
    }

    public async Task<SessionStatusDto> GetAsync(CancellationToken cancellationToken)
    {
        var state = _coordinator.State;
        var providerId = _coordinator.ActiveProvider;
        var lastError = _coordinator.LastError;

        var health = HealthUnknown;
        var ready = false;
        if (state == SessionState.Active)
        {
            (health, ready) = await ReadHealthAsync(cancellationToken).ConfigureAwait(false);
        }

        var providerName = providerId is { } id
            ? _catalog.List().FirstOrDefault(d => d.Id == id)?.DisplayName ?? id.Value
            : null;

        return new SessionStatusDto(
            state.ToString().ToLowerInvariant(),
            providerId?.Value,
            providerName,
            health,
            new[]
            {
                new ServiceStatusDto("Sales Leads MS", "Publisher", ready),
                new ServiceStatusDto("Customer MS", "Consumer", ready),
                new ServiceStatusDto("Communications MS", "Consumer", ready)
            },
            lastError);
    }

    private async Task<(string Health, bool Ready)> ReadHealthAsync(CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(HealthTimeout, _timeProvider);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            var health = await _cluster.CheckHealthAsync(linked.Token).ConfigureAwait(false);
            return health.Ready ? (HealthReachable, true) : (HealthUnreachable, false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Provider health check failed; reporting the provider as unreachable.");
            return (HealthUnreachable, false);
        }
    }
}
