using Messaging.Abstractions;
using Messaging.MassTransit.Configuration;
using Messaging.MassTransit.Consuming;
using Messaging.MassTransit.Transport;

namespace Messaging.MassTransit;

/// <summary>
/// The MassTransit <see cref="IMessagingProvider"/> for the broker-shaped modes: Publish-Subscribe
/// (RabbitMQ fan-out) and Message Queue (SQS / Azure Service Bus / in-memory point-to-point). It is
/// the single plug point of the ports-and-adapters design for these modes (Req 3.4): it wraps a
/// configured MassTransit bus and exposes the mode-independent ports the core depends on — a
/// publisher that delegates to MassTransit's publish/send endpoints, a consumer host that wraps the
/// bus start/stop, the required-settings declaration for the validator (Req 2.5), and a transport
/// reachability check backing readiness (Req 10.3).
/// </summary>
/// <remarks>
/// <para>
/// One provider instance serves exactly one <see cref="MessagingMode"/>; the same adapter class backs
/// both broker-shaped modes because only the configured transport and the publish-vs-send shape
/// differ, both of which live behind <see cref="IMassTransitBusAdapter"/>. The registry resolves this
/// provider by <see cref="Mode"/> (Req 3.4).
/// </para>
/// <para>
/// This is an adapter shell: broker topology, retry/redelivery tuning, and the concrete MassTransit
/// filter stack that invokes the core consume steps are owned by the provider's own later spec. The
/// shell composes the ports so the core can resolve, publish through, start/stop, validate, and probe
/// a MassTransit-backed mode without a live broker.
/// </para>
/// </remarks>
public sealed class MassTransitMessagingProvider : IMessagingProvider
{
    private readonly MassTransitProviderSettings _settings;
    private readonly IMassTransitTransportProbe _transportProbe;

    /// <summary>
    /// Creates the MassTransit provider for a single broker-shaped mode (constructor injection only).
    /// </summary>
    /// <param name="mode">
    /// The mode this provider serves. Must be <see cref="MessagingMode.PublishSubscribe"/> or
    /// <see cref="MessagingMode.MessageQueue"/> — the modes MassTransit backs.
    /// </param>
    /// <param name="settings">The provider-specific settings (transport, connection, host).</param>
    /// <param name="publisher">The publisher delegating to MassTransit publish/send endpoints.</param>
    /// <param name="consumerHost">The consumer host wrapping the MassTransit bus start/stop.</param>
    /// <param name="transportProbe">The transport reachability probe backing readiness.</param>
    /// <exception cref="ArgumentNullException">If any reference dependency is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// If <paramref name="mode"/> is a mode MassTransit does not back (Streaming / Batch).
    /// </exception>
    public MassTransitMessagingProvider(
        MessagingMode mode,
        MassTransitProviderSettings settings,
        IMessagePublisher publisher,
        IMessageConsumerHost consumerHost,
        IMassTransitTransportProbe transportProbe)
    {
        if (mode is not (MessagingMode.PublishSubscribe or MessagingMode.MessageQueue))
        {
            throw new ArgumentOutOfRangeException(
                nameof(mode),
                mode,
                "The MassTransit provider backs only the Publish-Subscribe and Message Queue modes.");
        }

        Mode = mode;
        _settings = settings ?? throw new ArgumentNullException(nameof(settings));
        Publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        ConsumerHost = consumerHost ?? throw new ArgumentNullException(nameof(consumerHost));
        _transportProbe = transportProbe ?? throw new ArgumentNullException(nameof(transportProbe));
    }

    /// <inheritdoc />
    public MessagingMode Mode { get; }

    /// <inheritdoc />
    public IMessagePublisher Publisher { get; }

    /// <inheritdoc />
    public IMessageConsumerHost ConsumerHost { get; }

    /// <inheritdoc />
    public IReadOnlyCollection<ProviderSettingDescriptor> RequiredSettings => _settings.ToRequiredSettings();

    /// <inheritdoc />
    public Task<TransportHealth> CheckTransportAsync(CancellationToken cancellationToken) =>
        _transportProbe.ProbeAsync(cancellationToken);
}
