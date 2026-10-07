using Messaging.Contracts;

namespace Messaging.MassTransit.Transport;

/// <summary>
/// The thin seam between this adapter shell and a configured MassTransit bus. It exposes exactly the
/// MassTransit surface the shell delegates to — start/stop of the bus, the publish/send endpoints,
/// and a reachability probe — without leaking MassTransit types into the shell's own logic.
/// </summary>
/// <remarks>
/// <para>
/// In production this is implemented over MassTransit's <c>IBusControl</c> (start/stop),
/// <c>IPublishEndpoint</c> (Publish-Subscribe fan-out) and <c>ISendEndpoint</c> (Message Queue
/// point-to-point) — wired by the DI task and the provider's own later spec. Keeping it behind a port
/// lets the shell compile and be exercised without standing up a live broker, which is the whole point
/// of an adapter shell (design: "shallow, by design").
/// </para>
/// <para>
/// The envelope is handed through verbatim; the shell does not reshape it. Broker topology, routing
/// keys, and serialization are MassTransit/transport concerns owned by the deeper spec.
/// </para>
/// </remarks>
public interface IMassTransitBusAdapter
{
    /// <summary>Starts the underlying MassTransit bus (begins hosting consumers and endpoints).</summary>
    /// <param name="cancellationToken">Token to cancel startup.</param>
    Task StartBusAsync(CancellationToken cancellationToken);

    /// <summary>Stops the underlying MassTransit bus, draining in-flight consumers.</summary>
    /// <param name="cancellationToken">Token to cancel the drain.</param>
    Task StopBusAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Publishes an envelope through MassTransit's publish endpoint for the Publish-Subscribe fan-out
    /// shape, or its send endpoint for the Message Queue point-to-point shape, per the active mode.
    /// </summary>
    /// <param name="envelope">The transport-neutral envelope to hand to MassTransit.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    Task PublishAsync(MessageEnvelope envelope, CancellationToken cancellationToken);

    /// <summary>
    /// Probes whether the configured MassTransit transport is reachable, backing the readiness probe
    /// (Req 10.3). Returns false (rather than throwing) when the broker is unavailable.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the probe.</param>
    Task<bool> IsTransportReachableAsync(CancellationToken cancellationToken);
}
