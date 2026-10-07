using Messaging.Abstractions;

namespace Messaging.MassTransit.Transport;

/// <summary>
/// Reports whether the configured MassTransit transport (RabbitMQ / SQS / Azure Service Bus /
/// in-memory) is reachable, backing the provider's
/// <see cref="IMessagingProvider.CheckTransportAsync"/> and therefore the readiness probe
/// (Req 10.3, 10.4). This is the shallow port the shell plugs into; the concrete reachability check
/// (opening a broker connection / querying the bus health) is a later-spec concern.
/// </summary>
public interface IMassTransitTransportProbe
{
    /// <summary>
    /// Checks reachability of the MassTransit transport, naming the unavailable dependency when it
    /// cannot be reached (Req 10.4).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the check.</param>
    Task<TransportHealth> ProbeAsync(CancellationToken cancellationToken);
}
