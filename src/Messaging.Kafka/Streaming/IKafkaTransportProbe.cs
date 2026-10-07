using Messaging.Abstractions;

namespace Messaging.Kafka.Streaming;

/// <summary>
/// Reports whether the Kafka cluster is reachable, backing the provider's
/// <see cref="IMessagingProvider.CheckTransportAsync"/> and therefore the readiness probe
/// (Req 10.3, 10.4). This is the shallow port the shell plugs into; the concrete reachability check
/// (request cluster metadata from the bootstrap servers via the admin client) is a later-spec concern.
/// </summary>
public interface IKafkaTransportProbe
{
    /// <summary>
    /// Checks reachability of the Kafka cluster, naming the unavailable dependency when it cannot be
    /// reached (Req 10.4).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the check.</param>
    Task<TransportHealth> ProbeAsync(CancellationToken cancellationToken);
}
