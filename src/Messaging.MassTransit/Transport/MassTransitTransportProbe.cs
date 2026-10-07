using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.Transport;

/// <summary>
/// The default <see cref="IMassTransitTransportProbe"/>: asks the bus seam whether the transport is
/// reachable and maps the answer to a <see cref="TransportHealth"/>, naming the configured broker as
/// the unavailable dependency when it is not (Req 10.3, 10.4). A probe that throws is treated as
/// unreachable rather than propagating, so a transient broker outage reports not-ready instead of
/// faulting the readiness endpoint.
/// </summary>
public sealed class MassTransitTransportProbe : IMassTransitTransportProbe
{
    private readonly IMassTransitBusAdapter _bus;
    private readonly MassTransitProviderSettings _settings;
    private readonly ILogger<MassTransitTransportProbe> _logger;

    /// <summary>
    /// Creates the transport probe (constructor injection only).
    /// </summary>
    /// <param name="bus">The seam over the configured MassTransit bus the probe queries.</param>
    /// <param name="settings">The provider settings naming the broker/dependency.</param>
    /// <param name="logger">Structured logger for probe outcomes.</param>
    /// <exception cref="ArgumentNullException">If any dependency is null.</exception>
    public MassTransitTransportProbe(
        IMassTransitBusAdapter bus,
        MassTransitProviderSettings settings,
        ILogger<MassTransitTransportProbe> logger)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<TransportHealth> ProbeAsync(CancellationToken cancellationToken)
    {
        try
        {
            var reachable = await _bus.IsTransportReachableAsync(cancellationToken).ConfigureAwait(false);

            if (reachable)
            {
                return new TransportHealth(Reachable: true, UnavailableDependency: null);
            }

            _logger.LogWarning(
                "MassTransit transport {Transport} is not reachable at dependency {Dependency}.",
                _settings.Transport,
                _settings.Host);

            return new TransportHealth(Reachable: false, UnavailableDependency: DependencyName());
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            // A probe failure is treated as "not reachable", naming the dependency (Req 10.4). The
            // readiness endpoint reports not-ready rather than erroring.
            _logger.LogWarning(
                ex,
                "MassTransit transport probe for {Transport} failed; reporting dependency {Dependency} unavailable.",
                _settings.Transport,
                _settings.Host);

            return new TransportHealth(Reachable: false, UnavailableDependency: DependencyName());
        }
    }

    private string DependencyName() => $"{_settings.Transport} ({_settings.Host})";
}
