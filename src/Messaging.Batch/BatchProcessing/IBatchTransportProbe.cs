using Messaging.Abstractions;

namespace Messaging.Batch.BatchProcessing;

/// <summary>
/// Reports whether the batch source and destination are reachable, backing the provider's
/// <see cref="IMessagingProvider.CheckTransportAsync"/> and therefore the readiness probe
/// (Req 10.3, 10.4). This is the shallow port the shell plugs into; the concrete reachability check
/// (query the batch store, stat the drop location) is a later-spec concern.
/// </summary>
public interface IBatchTransportProbe
{
    /// <summary>
    /// Checks reachability of the batch transport, naming the unavailable dependency when it cannot
    /// be reached (Req 10.4).
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the check.</param>
    Task<TransportHealth> ProbeAsync(CancellationToken cancellationToken);
}
