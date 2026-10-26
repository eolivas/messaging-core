namespace Messaging.MassTransit.Configuration;

/// <summary>
/// Selects which transport endpoints the MassTransit bus stands up, driving role-scoped endpoint
/// registration. A publisher-only build registers no receive endpoint, a consumer build registers
/// exactly one, and a combined build does both — so a process takes on only the broker surface area
/// its role requires.
/// </summary>
public enum MassTransitRole
{
    /// <summary>
    /// Publish/send only: the bus registers <em>no</em> receive endpoint. Suited to a process that
    /// emits messages but never consumes them.
    /// </summary>
    Publisher,

    /// <summary>
    /// Consume only: the bus registers exactly one receive endpoint and does not expose application
    /// publishing. Suited to a worker that processes messages but does not emit new ones.
    /// </summary>
    Consumer,

    /// <summary>
    /// Both publish and consume: the bus registers exactly one receive endpoint <em>and</em> enables
    /// application publishing. Suited to a process that participates on both sides of the exchange.
    /// </summary>
    Both,
}
