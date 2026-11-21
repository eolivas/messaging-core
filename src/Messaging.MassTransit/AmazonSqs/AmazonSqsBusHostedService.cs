using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Transport;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Messaging.MassTransit.AmazonSqs;

/// <summary>
/// Starts the Amazon SQS bus when the application host starts and stops it when the host stops
/// (Req 12.1, 12.2, 12.3, 12.5).
/// </summary>
/// <remarks>
/// <para>
/// Consumer or both role: it drives <see cref="IMessageConsumerHost.StartAsync"/> and
/// <see cref="IMessageConsumerHost.StopAsync"/>, so the shell state machine tracks Running and Stopped
/// and the shell starts and stops the bus through the adapter.
/// </para>
/// <para>
/// Publisher role: no processor is registered, so the consumer host is not available. The service
/// calls <see cref="IMassTransitBusAdapter.StartBusAsync"/> and
/// <see cref="IMassTransitBusAdapter.StopBusAsync"/> on the adapter directly.
/// </para>
/// <para>
/// A start failure propagates so the host fails fast (Req 12.5). Stop is idempotent: it does nothing
/// if start never completed.
/// </para>
/// </remarks>
public sealed class AmazonSqsBusHostedService : IHostedService
{
    private readonly MassTransitRole _role;
    private readonly IMassTransitBusAdapter _bus;
    private readonly IMessageConsumerHost? _consumerHost;
    private readonly ILogger<AmazonSqsBusHostedService> _logger;

    private bool _started;

    /// <summary>Creates the hosted service (constructor injection only).</summary>
    /// <param name="role">The bus role.</param>
    /// <param name="bus">The bus adapter, used directly for the publisher role.</param>
    /// <param name="consumerHost">The consumer host. Required for a consuming role; ignored for a publisher.</param>
    /// <param name="logger">Structured logger.</param>
    /// <exception cref="ArgumentNullException">A required dependency is null.</exception>
    public AmazonSqsBusHostedService(
        MassTransitRole role,
        IMassTransitBusAdapter bus,
        IMessageConsumerHost? consumerHost,
        ILogger<AmazonSqsBusHostedService> logger)
    {
        _role = role;
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (UsesConsumerHost && consumerHost is null)
        {
            throw new ArgumentNullException(
                nameof(consumerHost),
                $"An '{nameof(IMessageConsumerHost)}' is required for role '{role}'.");
        }

        _consumerHost = consumerHost;
    }

    private bool UsesConsumerHost => _role is MassTransitRole.Consumer or MassTransitRole.Both;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (UsesConsumerHost)
        {
            await _consumerHost!.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await _bus.StartBusAsync(cancellationToken).ConfigureAwait(false);
        }

        _started = true;
        _logger.LogInformation("Amazon SQS bus hosted service started for role {Role}.", _role);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!_started)
        {
            return;
        }

        _started = false;

        if (UsesConsumerHost)
        {
            await _consumerHost!.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await _bus.StopBusAsync(cancellationToken).ConfigureAwait(false);
        }

        _logger.LogInformation("Amazon SQS bus hosted service stopped for role {Role}.", _role);
    }
}
