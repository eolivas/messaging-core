using Demo.Application.Providers;
using Demo.Application.Sessions;
using Demo.Contracts;
using Demo.Infrastructure.Composition;
using Microsoft.Extensions.Logging;

namespace Demo.Infrastructure.Hosting;

/// <summary>
/// The three microservice hosts of one Session. Start order: TCP pre-flight, both consumers in
/// parallel, then the publisher. A host is ready when its provider reports the transport
/// reachable. Any failure stops every host started so far within the cleanup budget and names
/// the failing microservice, or the provider, host and container command for a missing broker.
/// </summary>
public sealed class MicroserviceCluster : IMicroserviceCluster
{
    public static readonly TimeSpan StartBudget = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan CleanupBudget = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan ReadinessPollInterval = TimeSpan.FromMilliseconds(250);

    private readonly IReadOnlyDictionary<ProviderId, IProviderComposition> _compositions;
    private readonly IMicroserviceHostFactory _factory;
    private readonly DependencyPreflight _preflight;
    private readonly ILogger<MicroserviceCluster> _logger;
    private readonly TimeProvider _time;
    private readonly SemaphoreSlim _startGate = new(1, 1);
    private readonly object _gate = new();
    private readonly SalesLeadsGateway _gateway;

    private List<IMicroserviceHost> _hosts = [];
    private IMicroserviceHost? _publisher;
    private string? _providerName;
    private int _customerMode;
    private int _communicationsMode;

    public MicroserviceCluster(
        IEnumerable<IProviderComposition> compositions,
        IMicroserviceHostFactory factory,
        DependencyPreflight preflight,
        ILogger<MicroserviceCluster> logger,
        TimeProvider? time = null)
    {
        ArgumentNullException.ThrowIfNull(compositions);
        _compositions = compositions.ToDictionary(c => c.Id);
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
        _preflight = preflight ?? throw new ArgumentNullException(nameof(preflight));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _time = time ?? TimeProvider.System;
        _gateway = new SalesLeadsGateway(this);
    }

    public ISalesLeadsGateway SalesLeads => _gateway;

    public async Task StartAsync(ClusterStartRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!_compositions.TryGetValue(request.ProviderId, out var composition))
        {
            throw new InvalidOperationException($"Provider '{request.ProviderId}' is not supported.");
        }

        await _startGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_gate)
            {
                if (_hosts.Count > 0)
                {
                    throw new InvalidOperationException("The microservice hosts are already started.");
                }
            }

            Volatile.Write(ref _customerMode, (int)FailureMode.Off);
            Volatile.Write(ref _communicationsMode, (int)FailureMode.Off);

            using var budget = new CancellationTokenSource(StartBudget, _time);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, budget.Token);
            try
            {
                await StartHostsAsync(request, composition, linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (budget.IsCancellationRequested
                                                     && !cancellationToken.IsCancellationRequested)
            {
                await CleanUpAfterFailureAsync(request.ProviderId).ConfigureAwait(false);
                throw new TimeoutException(
                    $"Provider '{request.ProviderId}' did not become ready within {StartBudget.TotalSeconds:0} seconds.");
            }
            catch (Exception ex)
            {
                // Handled here: stop everything started, then surface the cause to the caller.
                _logger.LogError(ex, "Starting the microservice hosts for provider {Provider} failed.", request.ProviderId);
                await CleanUpAfterFailureAsync(request.ProviderId).ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            _startGate.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        List<IMicroserviceHost> hosts;
        lock (_gate)
        {
            hosts = _hosts;
            _hosts = [];
            _publisher = null;
            _providerName = null;
        }

        if (hosts.Count == 0)
        {
            return;
        }

        using var budget = new CancellationTokenSource(CleanupBudget, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, budget.Token);
        // Publisher first, so nothing new is published while the consumers drain.
        var ordered = hosts.OrderByDescending(h => h.Id == MicroserviceId.SalesLeads).ToList();
        foreach (var host in ordered)
        {
            await StopHostAsync(host, linked.Token).ConfigureAwait(false);
        }
    }

    public async Task<ProviderHealth> CheckHealthAsync(CancellationToken cancellationToken)
    {
        List<IMicroserviceHost> hosts;
        lock (_gate)
        {
            hosts = [.. _hosts];
        }

        if (hosts.Count == 0)
        {
            return new ProviderHealth(false, "not started");
        }

        var results = await Task.WhenAll(hosts.Select(h => ReadinessOfAsync(h, cancellationToken)))
            .ConfigureAwait(false);
        var unavailable = results.Where(r => r is not null).Select(r => r!).ToList();
        return unavailable.Count == 0
            ? new ProviderHealth(true, "ready")
            : new ProviderHealth(false, "not ready: " + string.Join("; ", unavailable));
    }

    public void SetFailureMode(ConsumerId consumer, FailureMode mode)
    {
        switch (consumer)
        {
            case ConsumerId.Customer:
                Volatile.Write(ref _customerMode, (int)mode);
                break;
            case ConsumerId.Communications:
                Volatile.Write(ref _communicationsMode, (int)mode);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(consumer), consumer, "Unknown consumer.");
        }
    }

    public IntegrationInfo Describe()
    {
        IMicroserviceHost? publisher;
        string? providerName;
        lock (_gate)
        {
            publisher = _publisher;
            providerName = _providerName;
        }

        if (publisher is null || providerName is null)
        {
            throw new InvalidOperationException("The microservice hosts are not started.");
        }

        var config = publisher.Configuration;
        return new IntegrationInfo(
            config.Mode?.ToString() ?? string.Empty,
            providerName,
            config.Idempotency.Enabled,
            config.Retry.MaxAttempts,
            config.Retry.Backoff.ToString());
    }

    private async Task StartHostsAsync(
        ClusterStartRequest request, IProviderComposition composition, CancellationToken token)
    {
        await _preflight.EnsureReachableAsync(request.ProviderId, request.EffectiveSettings, token)
            .ConfigureAwait(false);

        var customer = Register(MicroserviceId.Customer, composition, request,
            () => (FailureMode)Volatile.Read(ref _customerMode));
        var communications = Register(MicroserviceId.Communications, composition, request,
            () => (FailureMode)Volatile.Read(ref _communicationsMode));
        var publisher = Register(MicroserviceId.SalesLeads, composition, request, () => FailureMode.Off);

        await Task.WhenAll(StartAndAwaitReadyAsync(customer, token), StartAndAwaitReadyAsync(communications, token))
            .ConfigureAwait(false);
        await StartAndAwaitReadyAsync(publisher, token).ConfigureAwait(false);

        lock (_gate)
        {
            _publisher = publisher;
            _providerName = DisplayName(request.ProviderId);
        }
    }

    private IMicroserviceHost Register(
        MicroserviceId service,
        IProviderComposition composition,
        ClusterStartRequest request,
        Func<FailureMode> mode)
    {
        IMicroserviceHost host;
        try
        {
            host = _factory.Create(service, composition, request, mode);
        }
        catch (Exception ex) when (ex is InvalidOperationException or ArgumentException)
        {
            throw new InvalidOperationException($"{NameOf(service)} could not be built: {ex.Message}", ex);
        }

        lock (_gate)
        {
            _hosts.Add(host);
        }

        return host;
    }

    private async Task StartAndAwaitReadyAsync(IMicroserviceHost host, CancellationToken token)
    {
        try
        {
            await host.StartAsync(token).ConfigureAwait(false);
            while (true)
            {
                var health = await host.CheckTransportAsync(token).ConfigureAwait(false);
                if (health.Reachable)
                {
                    return;
                }

                await Task.Delay(ReadinessPollInterval, _time, token).ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException)
        {
            throw new InvalidOperationException(
                $"{NameOf(host.Id)} failed to become ready: {ex.GetType().Name}.", ex);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            throw new InvalidOperationException($"{NameOf(host.Id)} failed to become ready: operation cancelled.");
        }
    }

    private async Task CleanUpAfterFailureAsync(ProviderId providerId)
    {
        _logger.LogInformation("Stopping started hosts after a failed start of provider {Provider}.", providerId);
        using var cleanup = new CancellationTokenSource(CleanupBudget, _time);
        await StopAsync(cleanup.Token).ConfigureAwait(false);
    }

    private async Task StopHostAsync(IMicroserviceHost host, CancellationToken token)
    {
        try
        {
            await host.StopAsync(token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Best effort: one host failing to stop must not leave the others running.
            _logger.LogError(ex, "Stopping {Service} failed.", host.Id);
        }

        try
        {
            await host.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            _logger.LogError(ex, "Disposing {Service} failed.", host.Id);
        }
    }

    private async Task<string?> ReadinessOfAsync(IMicroserviceHost host, CancellationToken token)
    {
        try
        {
            var readiness = await host.CheckReadinessAsync(token).ConfigureAwait(false);
            return readiness.IsReady
                ? null
                : $"{NameOf(host.Id)}: {readiness.UnavailableDependency ?? "transport unavailable"}";
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException)
        {
            // Not rethrown: an unreadable host is reported as not ready. The type only; messages can hold secrets.
            _logger.LogWarning(ex, "Readiness check of {Service} failed.", host.Id);
            return $"{NameOf(host.Id)}: {ex.GetType().Name}";
        }
    }

    private static string NameOf(MicroserviceId id) => id switch
    {
        MicroserviceId.SalesLeads => "Sales Leads MS",
        MicroserviceId.Customer => "Customer MS",
        MicroserviceId.Communications => "Communications MS",
        _ => id.ToString(),
    };

    private static string DisplayName(ProviderId id) =>
        id == ProviderId.RabbitMq ? "RabbitMQ"
        : id == ProviderId.AmazonSnsSqs ? "Amazon SNS/SQS"
        : id == ProviderId.Kafka ? "Kafka"
        : id.Value;

    private IMicroserviceHost? CurrentPublisher()
    {
        lock (_gate)
        {
            return _publisher;
        }
    }

    private sealed class SalesLeadsGateway : ISalesLeadsGateway
    {
        private readonly MicroserviceCluster _cluster;

        public SalesLeadsGateway(MicroserviceCluster cluster)
        {
            _cluster = cluster;
        }

        public Task<AcceptQuoteOutcome> AcceptQuoteAsync(CancellationToken cancellationToken)
        {
            var publisher = _cluster.CurrentPublisher()
                ?? throw new InvalidOperationException("The Sales Leads microservice is not started.");
            return publisher.AcceptQuoteAsync(cancellationToken);
        }
    }
}
