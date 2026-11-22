using Demo.Application.Providers;
using Demo.Application.Sessions;
using Demo.Contracts;
using Demo.Infrastructure.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// Startup check (design D4, Req 11.1, 11.2). For each supported provider it validates the default
/// settings and builds, without starting, the publisher and both consumer hosts. Scope follows the
/// spike (demo/docs/spike-no-network-io.md): RabbitMQ and Amazon SQS also resolve the bus adapter,
/// which performs no network I/O. Kafka is limited to validators and registration, because
/// constructing the Confluent clients opens connections.
/// </summary>
public sealed class ProviderCompositionSelfCheck
{
    private static readonly ProviderId[] ClientBuildSkipped = [ProviderId.Kafka];

    private static readonly MicroserviceId[] Roles =
        [MicroserviceId.SalesLeads, MicroserviceId.Customer, MicroserviceId.Communications];

    private readonly IReadOnlyList<IProviderComposition> _compositions;
    private readonly IMicroserviceHostFactory _factory;
    private readonly ILogger<ProviderCompositionSelfCheck> _logger;

    public ProviderCompositionSelfCheck(
        IEnumerable<IProviderComposition> compositions,
        IMicroserviceHostFactory factory,
        ILogger<ProviderCompositionSelfCheck> logger)
    {
        ArgumentNullException.ThrowIfNull(compositions);
        _compositions = compositions.ToList();
        _factory = factory ?? throw new ArgumentNullException(nameof(factory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Runs the check. On failure logs the failed registration once and throws
    /// <see cref="ProviderCompositionSelfCheckException"/>.
    /// </summary>
    public async Task RunAsync()
    {
        foreach (var composition in _compositions)
        {
            foreach (var role in Roles)
            {
                await CheckAsync(composition, role).ConfigureAwait(false);
            }
        }
    }

    private async Task CheckAsync(IProviderComposition composition, MicroserviceId role)
    {
        var registration = $"{composition.Id}/{role}";
        try
        {
            var effective = composition.Defaults().Settings
                .ToDictionary(s => $"{s.Group}:{s.Name}", s => s.Value, StringComparer.Ordinal);

            var errors = composition.Validate(effective);
            if (errors.Count > 0)
            {
                var fields = string.Join(", ", errors.Select(e => e.Field));
                throw new InvalidOperationException($"Default settings are invalid: {fields}.");
            }

            var request = new ClusterStartRequest(composition.Id, effective);
            var host = _factory.Create(role, composition, request, () => FailureMode.Off);
            await using (host.ConfigureAwait(false))
            {
                if (host is MicroserviceHost concrete && !ClientBuildSkipped.Contains(composition.Id))
                {
                    concrete.VerifyRegistrations();
                }
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Never log setting values: the exception messages name settings only.
            _logger.LogCritical(
                ex,
                "Messaging registration {Registration} failed the startup self-check. Startup is terminated.",
                registration);
            throw new ProviderCompositionSelfCheckException(
                registration, $"Registration '{registration}' failed the startup self-check.", ex);
        }
    }
}
