using Messaging.Abstractions;
using Messaging.MassTransit.Transport;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Infrastructure.Composition;

/// <summary>
/// Starts and stops the RabbitMQ bus, which the library does not do for the DI path (finding F3).
/// A consumer host starts through IMessageConsumerHost, which also starts the bus. A
/// publisher-only host has no consumer host, so the bus is started and stopped directly.
/// </summary>
public sealed class TransportLifecycleHostedService : IHostedService
{
    private readonly IMassTransitBusAdapter _bus;
    private readonly IMessageConsumerHost? _consumerHost;
    private readonly ILogger<TransportLifecycleHostedService> _logger;

    public TransportLifecycleHostedService(
        IMassTransitBusAdapter bus,
        IMessageConsumerHost? consumerHost,
        ILogger<TransportLifecycleHostedService> logger)
    {
        _bus = bus ?? throw new ArgumentNullException(nameof(bus));
        _consumerHost = consumerHost;
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_consumerHost is not null)
        {
            await _consumerHost.StartAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Consumer host started.");
            return;
        }

        await _bus.StartBusAsync(cancellationToken).ConfigureAwait(false);
        _logger.LogInformation("Publisher bus started.");
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumerHost is not null)
        {
            await _consumerHost.StopAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        await _bus.StopBusAsync(cancellationToken).ConfigureAwait(false);
    }
}
